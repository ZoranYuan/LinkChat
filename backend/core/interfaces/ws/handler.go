package ws

import (
	eventport "IM_backend/core/application/ports/events"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"IM_backend/configs"
	messageapp "IM_backend/core/application/message"
	ratelimitport "IM_backend/core/application/ports/ratelimit"
	domainmessage "IM_backend/core/domain/message"
	"IM_backend/core/interfaces/http/response"
	wspb "IM_backend/core/interfaces/ws/pb"
)

type Handler struct {
	upgrader   websocket.Upgrader
	app        *messageapp.MessageApplication
	config     configs.Config
	dispatcher *Dispatcher
	gateway    *Gateway
	limiter    ratelimitport.Limiter
}

func NewHandler(
	app *messageapp.MessageApplication,
	config configs.Config,
	dispatcher *Dispatcher,
	gateway *Gateway,
) *Handler {
	wh := &Handler{
		app:        app,
		config:     config,
		gateway:    gateway,
		dispatcher: dispatcher,
	}
	wh.upgrader.CheckOrigin = wh.checkOrigin

	dispatcher.RegisterHandler(eventport.TypeEventBatchAck, wh.handleEventBatchAck)
	dispatcher.RegisterHandler(eventport.TypeMessageSend, wh.handleMessageSend)
	dispatcher.RegisterHandler(eventport.TypeMessageRead, wh.handleMessageRead)
	dispatcher.RegisterHandler(eventport.TypeGroupEnter, wh.handleGroupEnter)
	dispatcher.RegisterHandler(eventport.TypeConnectionVisibility, wh.handleConnectionVisibility)

	return wh
}

func (wh *Handler) SetLimiter(limiter ratelimitport.Limiter) {
	wh.limiter = limiter
}

func (wh *Handler) allowEvent(ctx context.Context, op string, userID string, policy ratelimitport.Policy) (bool, error) {
	if wh.limiter == nil {
		return true, nil
	}

	key := fmt.Sprintf(
		"ws:user:%s:op:%s",
		userID,
		op,
	)

	decision, err := wh.limiter.Allow(
		ctx,
		key,
		policy,
	)

	if err != nil {
		// 限流服务异常时拒绝发送，向客户端返回失败结果。
		log.Printf(
			"WebSocket 限流器执行失败：%v",
			err,
		)
		return false, err
	}

	return decision.Allowed, nil
}

func (wh *Handler) handleEventBatchAck(
	_ context.Context,
	connection *Connection,
	data []byte,
) error {
	var ack wspb.EventBatchAck
	if err := proto.Unmarshal(data, &ack); err != nil {
		log.Printf("解析事件批次 ACK 失败：connection=%s err=%v", connection.ConnectionID(), err)
		return nil
	}

	connection.pendingBatch.handleAck(ack.GetBatchId())
	return nil
}

func (wh *Handler) handleMessageRead(ctx context.Context, connection *Connection, data []byte) error {
	var pb wspb.MessageReadRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	messageID := pb.GetMessageId()
	if messageID == "" {
		return errors.New("messageId 不能为空")
	}

	return wh.app.HandleReadMessage(ctx, connection.UserID(), pb.GetMessageId())
}

func (wh *Handler) handleGroupEnter(ctx context.Context, connection *Connection, data []byte) error {
	var pb wspb.GroupEnterRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	return wh.app.HandleGroupEntry(ctx, connection.UserID(), pb.GetGroupId())
}

func (wh *Handler) handleConnectionVisibility(_ context.Context, connection *Connection, data []byte) error {
	var pb wspb.ConnectionVisibilityRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Hidden == nil {
		return errors.New("页面可见状态不能为空")
	}
	connection.SetPageHidden(pb.GetHidden(), time.Now())
	return nil
}

func (wh *Handler) handleMessageSend(ctx context.Context, connection *Connection, data []byte) error {
	var request wspb.MessageSendRequest
	if err := proto.Unmarshal(data, &request); err != nil {
		return err
	}

	// 用户发消息频率限制
	allowed, limitErr := wh.allowEvent(
		ctx,
		eventport.TypeMessageSend,
		connection.UserID(),
		ratelimitport.Policy{
			Rate:  float64(wh.config.WebSocket.SendMessageRate),
			Burst: int64(wh.config.WebSocket.SendMessageBurst),
		},
	)
	if limitErr != nil {
		log.Printf("消息限流检查失败：user=%s error=%v", connection.UserID(), limitErr)
		return wh.replyMessageSendAck(connection, request.GetClientMsgId(), "failed", "限流服务暂不可用", "")
	}

	if !allowed {
		return wh.replyMessageSendAck(connection, request.GetClientMsgId(), "failed", "消息发送过于频繁", "")
	}

	conversationID := request.GetConversationId()
	if conversationID == "" || len(conversationID) > wh.config.Message.MaxIdentifierLength {
		return wh.replyMessageSendAck(connection, request.GetClientMsgId(), "failed", "会话标识无效", conversationID)
	}

	dto := messageapp.SendMessageDTO{
		SenderID:        connection.UserID(),
		ClientMessageID: request.GetClientMsgId(),
		Type:            int(request.GetCType()),
		Content:         request.GetContent(),
		FileID:          request.GetFileId(),
		StickerID:       request.GetStickerId(),
		PackID:          request.GetPackId(),
	}
	entry, err := wh.app.HandleSendMessage(ctx, conversationID, dto)
	if err != nil {
		entry.Status = "failed"
		entry.Error = "消息服务暂不可用"
		switch {
		case errors.Is(err, messageapp.ErrMessagePublishUncertain):
			// Broker 可能已接收；保留 processing，让客户端开始查状态并在必要时用原 ID 重试。
			entry.Status = "processing"
		case errors.Is(err, messageapp.ErrNotGroupMember), errors.Is(err, messageapp.ErrNotFriends), errors.Is(err, messageapp.ErrForbidden):
			entry.Error = "无权在该会话发送消息"
		case errors.Is(err, messageapp.ErrConversationNotFound):
			entry.Error = "会话不存在"
		case errors.Is(err, domainmessage.ErrClientMessageConflict):
			entry.Error = err.Error()
		default:
			log.Printf("消息投递失败：user=%s conversation=%s client_msg_id=%s error=%v",
				connection.UserID(), conversationID, request.GetClientMsgId(), err)
		}
	}
	return wh.replyMessageSendAck(connection, request.GetClientMsgId(), entry.Status, entry.Error, conversationID)
}

func (wh *Handler) replyMessageSendAck(connection *Connection, clientMsgID, sendStatus, extra, conversationID string) error {
	return connection.PushProtobufEvent(eventport.TypeMessageSendAck, &wspb.MessageSendAck{
		ClientMsgId:    clientMsgID,
		Status:         sendStatus,
		Extra:          extra,
		SendTime:       time.Now().UnixMilli(),
		ConversationId: conversationID,
	})
}

func (wh *Handler) checkOrigin(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))

	// 开发环境允许本地页面连接，生产环境校验允许的来源。
	if wh.config.App.Env == "development" {
		return true
	}

	if origin == "" {
		return false
	}

	for _, allowed := range strings.Split(wh.config.Security.AllowedOrigins, ",") {
		if strings.TrimSpace(allowed) == origin {
			return true
		}
	}

	return false
}

func (wh *Handler) Upgrade(c *gin.Context) {
	userID := strings.TrimSpace(c.GetString("userId"))

	if userID == "" {
		c.JSON(http.StatusUnauthorized, response.Error(http.StatusUnauthorized, "登录过期"))
		return
	}

	deviceID := c.Query("device_id")
	if deviceID == "" {
		deviceID = c.GetHeader("X-Device-ID")
	}

	platform := c.Query("platform")
	if platform == "" {
		platform = c.GetHeader("X-Platform")
	}

	authSessionID := strings.TrimSpace(c.GetString("authSessionID"))
	identity, err := NewConnectionIdentity(
		userID,
		deviceID,
		platform,
		authSessionID,
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "网络异常，无法连接服务器"))
		return
	}

	// 服务升级
	conn, err := wh.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}

	connection := NewConnection(
		wh.gateway.NetworkContext(),
		conn,
		identity,
		wh.config.WebSocket,
	)

	if err := wh.gateway.RegisterAndStart(
		connection,
		wh.dispatcher.Dispatch,
		wh.gateway.Unregister,
	); err != nil {
		return
	}
	groupIDs, err := wh.app.ListActiveGroupIDsByUserID(userID)
	if err != nil {
		log.Printf("加载用户在线群聊索引失败：用户=%s 错误=%v", userID, err)
	} else {
		// 将当前连接绑定到用户当前活跃的群聊。
		wh.gateway.BindOnlineGroups(connection, groupIDs)
	}
}
