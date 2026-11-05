package ws

import (
	"IM_backend/internal/application/ports/messaging"
	"context"
	"encoding/json"
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
	messageapp "IM_backend/internal/application/message"
	ratelimitport "IM_backend/internal/application/ports/ratelimit"
	"IM_backend/internal/interfaces/http/response"
	wspb "IM_backend/internal/interfaces/ws/pb"
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

	dispatcher.RegisterHandler(messaging.TypeMessageSend, wh.handleMessageSend)
	dispatcher.RegisterHandler(messaging.TypeMessageRead, wh.handleReadMessageAck)
	dispatcher.RegisterHandler(messaging.TypeRoomEnter, wh.handleRoomEnter)
	dispatcher.RegisterHandler(messaging.TypeConnectionVisibility, wh.handleConnectionVisibility)

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
		// 限流服务异常时拒绝发送，向客户端返回失败 ACK。
		log.Printf(
			"WebSocket 限流器执行失败：%v",
			err,
		)
		return false, err
	}

	return decision.Allowed, nil
}

func (wh *Handler) handleReadMessageAck(ctx context.Context, connection *Connection, data []byte) error {
	var pb wspb.MessageReadRequest

	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	return wh.app.HandleReadMessage(ctx, connection.UserID(), pb.GetMessageId())
}

func (wh *Handler) handleRoomEnter(ctx context.Context, connection *Connection, data []byte) error {
	var pb wspb.RoomEnterRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	return wh.app.HandleRoomEntry(ctx, connection.UserID(), pb.GetRoomId())
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
		messaging.TypeMessageSend,
		connection.UserID(),
		ratelimitport.Policy{
			Rate:  float64(wh.config.WebSocket.SendMessageRate),
			Burst: int64(wh.config.WebSocket.SendMessageBurst),
		},
	)
	if limitErr != nil {
		return wh.replyFailedSendResult(connection, request.GetClientMsgId(), "限流服务暂不可用")
	}

	if !allowed {
		return wh.replyFailedSendResult(connection, request.GetClientMsgId(), "消息发送过于频繁")
	}

	conversationID := request.GetConversationId()
	if conversationID == "" || len(conversationID) > wh.config.Message.MaxIdentifierLength {
		return wh.replyFailedSendResult(connection, request.GetClientMsgId(), "会话标识无效")
	}

	dto := messaging.SendMessageDTO{
		SenderID:        connection.UserID(),
		ClientMessageID: request.GetClientMsgId(),
		ConversationID:  conversationID,
		Type:            int(request.GetCType()),
		Content:         request.GetContent(),
		FileID:          request.GetFileId(),
		StickerID:       request.GetStickerId(),
		PackID:          request.GetPackId(),
	}
	messageID, err := wh.app.HandleSendMessage(ctx, conversationID, dto)
	if err != nil {
		reason := "消息服务暂不可用"
		switch {
		case errors.Is(err, messageapp.ErrNotRoomMember), errors.Is(err, messageapp.ErrNotFriends), errors.Is(err, messageapp.ErrForbidden):
			reason = "无权在该会话发送消息"
		case errors.Is(err, messageapp.ErrConversationNotFound):
			reason = "会话不存在"
		}
		return wh.replyFailedSendResult(connection, request.GetClientMsgId(), reason)
	}
	payload, err := json.Marshal(messaging.MessageSendResultEvent{
		ClientMsgID:    request.GetClientMsgId(),
		MessageID:      messageID,
		ConversationID: conversationID,
		Status:         messaging.AckStatusSent,
		SendTime:       time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return connection.PushEvent(messaging.TypeMessageSendResult, payload)
}

func (wh *Handler) replyFailedSendResult(connection *Connection, clientMsgID, reason string) error {
	payload, err := json.Marshal(messaging.MessageSendResultEvent{
		ClientMsgID: clientMsgID,
		Status:      messaging.AckStatusFailed,
		Extra:       reason,
		SendTime:    time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return connection.PushEvent(messaging.TypeMessageSendResult, payload)
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
	roomIDs, err := wh.app.ListActiveRoomIDsByUserID(userID)
	if err != nil {
		log.Printf("加载用户在线房间索引失败：用户=%s 错误=%v", userID, err)
	} else {
		// 将当前连接绑定到用户当前活跃的房间。
		wh.gateway.BindOnlineRooms(connection, roomIDs)
	}
}
