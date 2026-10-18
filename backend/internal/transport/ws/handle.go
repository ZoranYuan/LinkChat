package ws

import (
	"IM_backend/configs"
	messageapp "IM_backend/internal/application/message"
	"IM_backend/internal/infrastructure/id/snow"
	realtimews "IM_backend/internal/infrastructure/realtime/websocket"
	"IM_backend/internal/shared/diagnostics"
	"IM_backend/internal/shared/protocol"
	shared_ratelimit "IM_backend/internal/shared/ratelimit"
	"IM_backend/internal/transport/http/response"
	wspb "IM_backend/internal/transport/ws/pb"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type WSHandler struct {
	ctx                  context.Context
	upgrader             websocket.Upgrader
	app                  *messageapp.MessageApplication
	config               configs.Config
	dispatcher           *Dispatcher
	gateway              *realtimews.Gateway
	limiter              shared_ratelimit.Limit
	idGenerator          *snow.Generator
	sendMessageSequencer *sendMessageSequencer
}

type sendMessageSendTask struct {
	connection       *realtimews.Connection
	conversationID   string
	request          *wspb.MessageReq
	requestStartedAt time.Time
	decodedAt        time.Time
	enqueuedAt       time.Time
}

type sendMessageSequencerShard struct {
	messageTasks chan sendMessageSendTask
	ticker       *time.Ticker
	flushMu      sync.Mutex
}

type sendMessageSequencer struct {
	shards []*sendMessageSequencerShard
}

func (s *sendMessageSequencer) shardFor(conversationID string) *sendMessageSequencerShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(conversationID))
	index := int(h.Sum32() % uint32(len(s.shards)))
	return s.shards[index]
}

func newSendMessageSequencer(config configs.Config) *sendMessageSequencer {
	shards := make([]*sendMessageSequencerShard, config.WebSocket.MessageSequencerShardCount)
	for i := range shards {
		shards[i] = &sendMessageSequencerShard{
			messageTasks: make(chan sendMessageSendTask, config.WebSocket.MessageBatchSize),
			ticker:       time.NewTicker(time.Duration(config.WebSocket.MessageFlushIntervalMs) * time.Millisecond),
		}
	}
	return &sendMessageSequencer{shards: shards}
}

func (wh *WSHandler) runSendMessageShard(ctx context.Context, shard *sendMessageSequencerShard) {
	defer shard.ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-shard.ticker.C:
			wh.flushSendMessageShard(ctx, shard)
		}
	}
}

func (wh *WSHandler) enqueueSendMessage(ctx context.Context, task sendMessageSendTask) error {
	shard := wh.sendMessageSequencer.shardFor(task.conversationID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case shard.messageTasks <- task:
			return nil
		default:
			wh.flushSendMessageShard(wh.ctx, shard)
		}
	}
}

func (wh *WSHandler) flushSendMessageShard(ctx context.Context, shard *sendMessageSequencerShard) {
	shard.flushMu.Lock()
	defer shard.flushMu.Unlock()

	count := len(shard.messageTasks)
	if count == 0 {
		return
	}
	tasks := make([]sendMessageSendTask, 0, count)
	for i := 0; i < count; i++ {
		tasks = append(tasks, <-shard.messageTasks)
	}
	wh.processSendMessageBatch(ctx, tasks)
}

func NewWSHandler(
	ctx context.Context,
	app *messageapp.MessageApplication,
	config configs.Config,
	dispatcher *Dispatcher,
	gateway *realtimews.Gateway,
	idGenerator *snow.Generator,
) *WSHandler {
	wh := &WSHandler{
		ctx:                  ctx,
		app:                  app,
		config:               config,
		gateway:              gateway,
		dispatcher:           dispatcher,
		idGenerator:          idGenerator,
		sendMessageSequencer: newSendMessageSequencer(config),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")
				if origin == "" || config.App.Env == "development" {
					return true
				}
				for _, allowed := range strings.Split(config.Security.AllowedOrigins, ",") {
					if strings.TrimSpace(allowed) == origin {
						return true
					}
				}
				return false
			},
		},
	}
	for _, shard := range wh.sendMessageSequencer.shards {
		go wh.runSendMessageShard(ctx, shard)
	}

	dispatcher.RegisterHandler(protocol.EventTypeSendMessage, wh.handleSendMessage)
	dispatcher.RegisterHandler(protocol.EventReadMessageAck, wh.handleReadMessageAck)
	dispatcher.RegisterHandler(protocol.EventConnectionVisibility, wh.handleConnectionVisibility)

	return wh
}

func (wh *WSHandler) FlushPendingMessages(ctx context.Context) {
	for _, shard := range wh.sendMessageSequencer.shards {
		shard.ticker.Stop()
		wh.flushSendMessageShard(ctx, shard)
	}
}

func (wh *WSHandler) SetLimiter(limiter shared_ratelimit.Limit) {
	wh.limiter = limiter
}

func (wh *WSHandler) allowEvent(ctx context.Context, op string, userId string, policy shared_ratelimit.Policy) (bool, error) {
	if wh.limiter == nil {
		return true, nil
	}

	key := fmt.Sprintf(
		"ws:user:%s:op:%s",
		userId,
		op,
	)

	decision, err := wh.limiter.Allow(
		ctx,
		key,
		policy,
	)

	if err != nil {
		// WebSocket 普通消息采用 Fail Open。
		log.Printf(
			"WebSocket 限流器执行失败：%v",
			err,
		)
		return false, err
	}

	return decision.Allowed, nil
}

func (wh *WSHandler) handleReadMessageAck(ctx context.Context, connection *realtimews.Connection, data []byte) error {
	var pb wspb.MessageReadAckReq

	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	if err := wh.app.HandleReadMessage(ctx, connection.UserID(), pb.GetMessageId()); err != nil {
		return err
	}
	return nil
}

func (wh *WSHandler) handleConnectionVisibility(_ context.Context, connection *realtimews.Connection, data []byte) error {
	var pb wspb.ConnectionVisibilityReq
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Hidden == nil {
		return errors.New("页面可见状态不能为空")
	}
	connection.SetPageHidden(pb.GetHidden(), time.Now())
	return nil
}

func (wh *WSHandler) handleSendMessage(ctx context.Context, connection *realtimews.Connection, data []byte) error {
	requestStartedAt := time.Now()
	var request wspb.MessageReq
	if err := proto.Unmarshal(data, &request); err != nil {
		return err
	}
	decodedAt := time.Now()

	sendRate := wh.config.WebSocket.SendMessageRate
	if sendRate <= 0 {
		sendRate = 10
	}
	sendBurst := int64(wh.config.WebSocket.SendMessageBurst)
	if sendBurst <= 0 {
		sendBurst = 20
	}

	// 用户发消息频率限制
	rateLimitStartedAt := time.Now()
	allowed, limitErr := wh.allowEvent(
		ctx,
		protocol.EventTypeSendMessage,
		connection.UserID(),
		shared_ratelimit.Policy{
			Rate:  float64(sendRate),
			Burst: sendBurst,
		},
	)
	rateLimitDuration := time.Since(rateLimitStartedAt)
	if limitErr != nil {
		diagnostics.Logf("stage=ws_send client_msg_id=%s user_id=%s outcome=rate_limit_error decode_us=%d rate_limit_us=%d total_us=%d",
			request.GetClientMsgId(), connection.UserID(), decodedAt.Sub(requestStartedAt).Microseconds(), rateLimitDuration.Microseconds(), time.Since(requestStartedAt).Microseconds())
		return wh.replyFailedAck(connection, request.GetClientMsgId(), "限流服务暂不可用")
	}

	if !allowed {
		diagnostics.Logf("stage=ws_send client_msg_id=%s user_id=%s outcome=rate_limited decode_us=%d rate_limit_us=%d total_us=%d",
			request.GetClientMsgId(), connection.UserID(), decodedAt.Sub(requestStartedAt).Microseconds(), rateLimitDuration.Microseconds(), time.Since(requestStartedAt).Microseconds())
		return wh.replyFailedAck(connection, request.GetClientMsgId(), "消息发送过于频繁")
	}

	conversationID := request.GetConversationId()
	if conversationID == "" || len(conversationID) > wh.config.Message.MaxIdentifierLength {
		return wh.replyFailedAck(connection, request.GetClientMsgId(), "会话标识无效")
	}
	task := sendMessageSendTask{
		connection:       connection,
		conversationID:   conversationID,
		request:          &request,
		requestStartedAt: requestStartedAt,
		decodedAt:        decodedAt,
		enqueuedAt:       time.Now(),
	}
	if err := wh.enqueueSendMessage(ctx, task); err != nil {
		return wh.replyFailedAck(connection, request.GetClientMsgId(), "消息服务暂不可用")
	}
	diagnostics.Logf("stage=ws_send client_msg_id=%s user_id=%s outcome=queued decode_us=%d rate_limit_us=%d queue_depth=%d",
		request.GetClientMsgId(), connection.UserID(), decodedAt.Sub(requestStartedAt).Microseconds(), rateLimitDuration.Microseconds(), len(wh.sendMessageSequencer.shardFor(conversationID).messageTasks))
	return nil
}

func (wh *WSHandler) processSendMessageBatch(ctx context.Context, tasks []sendMessageSendTask) {
	groups := make(map[string][]sendMessageSendTask)
	conversationIDs := make([]string, 0)
	for _, task := range tasks {
		if _, exists := groups[task.conversationID]; !exists {
			conversationIDs = append(conversationIDs, task.conversationID)
		}
		groups[task.conversationID] = append(groups[task.conversationID], task)
	}

	for _, conversationID := range conversationIDs {
		group := groups[conversationID]
		dtos := make([]messageapp.SendMessageDTO, len(group))
		for i, task := range group {
			req := task.request
			dtos[i] = messageapp.SendMessageDTO{
				SenderID:        task.connection.UserID(),
				ClientMessageID: req.GetClientMsgId(),
				ConversationID:  conversationID,
				Type:            int(req.GetCType()),
				Content:         req.GetContent(),
				FileID:          req.GetFileId(),
				StickerID:       req.GetStickerId(),
				PackID:          req.GetPackId(),
			}
		}

		appStartedAt := time.Now()
		acks, batchErr := wh.app.HandleSendMessages(ctx, conversationID, dtos)
		appDuration := time.Since(appStartedAt)
		if batchErr != nil {
			log.Printf("批量处理消息失败：conversation=%s count=%d err=%v", conversationID, len(group), batchErr)
		}
		if len(acks) != len(group) {
			batchErr = fmt.Errorf("批量 ACK 数量不匹配：messages=%d acks=%d", len(group), len(acks))
			for _, task := range group {
				_ = wh.replyFailedAck(task.connection, task.request.GetClientMsgId(), batchErr.Error())
			}
			continue
		}

		for i, task := range group {
			ack := acks[i]
			if ack == nil {
				if err := wh.replyFailedAck(task.connection, task.request.GetClientMsgId(), "消息处理失败"); err != nil {
					log.Printf("发送消息失败 ACK 失败：clientMsgId=%s err=%v", task.request.GetClientMsgId(), err)
				}
				continue
			}
			ackEvent := protocol.MessageAckEvent{
				ClientMsgId:    ack.ClientMessageID,
				MessageId:      ack.MessageID,
				ConversationId: ack.ConversationID,
				Seq:            ack.Seq,
				AttachmentId:   ack.AttachmentID,
				SendTime:       ack.SendTime,
				Status:         protocol.AckStatus(ack.Status),
				Extra:          ack.Error,
			}
			payload, err := json.Marshal(ackEvent)
			ackStartedAt := time.Now()
			if err == nil {
				err = wh.replyToClient(task.connection, protocol.EventTypeMsgAck, payload)
			}
			diagnostics.Logf("stage=ws_send client_msg_id=%s message_id=%s seq=%d user_id=%s status=%s queue_wait_us=%d app_us=%d ack_enqueue_us=%d outcome=%s",
				ack.ClientMessageID, ack.MessageID, ack.Seq, task.connection.UserID(), ack.Status,
				time.Since(task.enqueuedAt).Microseconds(), appDuration.Microseconds(), time.Since(ackStartedAt).Microseconds(), diagnosticErrorLabel(err))
			if err != nil {
				log.Printf("发送消息 ACK 失败：clientMsgId=%s err=%v", ack.ClientMessageID, err)
			}
		}
	}
}

func (wh *WSHandler) replyFailedAck(connection *realtimews.Connection, clientMsgID, reason string) error {
	payload, err := json.Marshal(protocol.MessageAckEvent{
		ClientMsgId: clientMsgID,
		Status:      protocol.AckStatusFailed,
		Extra:       reason,
		SendTime:    time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return wh.replyToClient(connection, protocol.EventTypeMsgAck, payload)
}

func diagnosticErrorLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func (wh *WSHandler) replyToClient(connection *realtimews.Connection, op string, payload []byte) error {
	return connection.PushEvent(op, payload)
}

func (wh *WSHandler) handleClientClosed(connection *realtimews.Connection) {
	wh.gateway.Unregister(connection)
}

func (wh *WSHandler) checkOrigin(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))

	// 生产模式下直接返回成功
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

func (wh *WSHandler) Handler(c *gin.Context) {
	userId := strings.TrimSpace(c.GetString("userId"))

	if userId == "" {
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
	identity, err := realtimews.NewConnectionIdentity(
		userId,
		deviceID,
		platform,
		authSessionID,
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "网络异常，无法连接服务器"))
		return
	}

	if isValid := wh.checkOrigin(c.Request); !isValid {
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "非法请求"))
		return
	}

	// 服务升级
	conn, err := wh.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.Error(http.StatusInternalServerError, "网络异常，无法连接服务器"))
		return
	}

	batchConfig := realtimews.MessageBatchConfig{
		MaxMessages:    wh.config.WebSocket.BatchMaxMessages,
		MaxBytes:       wh.config.WebSocket.BatchMaxBytes,
		Linger:         time.Duration(wh.config.WebSocket.BatchLingerMilliseconds) * time.Millisecond,
		ReadyQueueSize: wh.config.WebSocket.BatchReadyQueueSize,
	}

	connection := realtimews.NewConnection(
		wh.gateway.NetworkContext(),
		conn,
		identity,
		wh.config.WebSocket.MaxMessageSendBufferSize,
		wh.idGenerator,
		batchConfig,
	)
	connection.SetReadLimit(int64(wh.config.WebSocket.MaxMessageSize))

	if err := wh.gateway.RegisterAndStart(
		connection,
		wh.config.WebSocket.PongWaitSeconds,
		wh.config.WebSocket.PingPeriodSeconds,
		wh.config.WebSocket.WriteWaitSeconds,
		wh.dispatcher.Dispatch,
		wh.handleClientClosed,
	); err != nil {
		return
	}
	roomIDs, err := wh.app.ListActiveRoomIDs(userId)
	if err != nil {
		log.Printf("加载用户在线房间索引失败：用户=%s 错误=%v", userId, err)
	} else {
		// 将当前连接绑定到用户当前活跃的房间。
		wh.gateway.BindOnlineRooms(connection, roomIDs)
	}
}
