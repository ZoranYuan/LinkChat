package send

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"IM_backend/configs"
	conversationapp "IM_backend/core/application/conversation"
	groupcache "IM_backend/core/application/ports/cache/group"
	eventport "IM_backend/core/application/ports/events"
	domainconversation "IM_backend/core/domain/conversation"
	domainmessage "IM_backend/core/domain/message"
	"IM_backend/core/infra/mq/handlers"
	"IM_backend/core/infra/mq/kafka"
)

type transferPayload struct {
	MessageID    string `json:"messageId"`
	ConvID       string `json:"convId"`
	SenderID     string `json:"senderId"`
	Seq          int64  `json:"seq"`
	Type         int    `json:"type"`
	Content      string `json:"content"`
	SendTime     int64  `json:"sendTime"`
	AttachmentID string `json:"attachmentId,omitempty"`
	ClientMsgID  string `json:"clientMsgId,omitempty"`
	StickerID    string `json:"stickerId,omitempty"`
	PackID       string `json:"packId,omitempty"`
}

type MessageTransferHandler struct {
	conversationApplication *conversationapp.ConversationApplication
	delivery                *handlers.MessageDelivery
	groupCache               groupcache.GroupCache
	config                  configs.Config
}

func NewMessageTransferHandler(
	app *conversationapp.ConversationApplication,
	delivery *handlers.MessageDelivery,
	groupCache groupcache.GroupCache,
	config configs.Config,
) *MessageTransferHandler {
	return &MessageTransferHandler{
		conversationApplication: app,
		delivery:                delivery,
		groupCache:               groupCache,
		config:                  config,
	}
}

func (h *MessageTransferHandler) Topic() string { return h.config.Kafka.Topics.MessageSendPush }

func (h *MessageTransferHandler) Handle(ctx context.Context, item *kafka.MessageItem) error {
	var p transferPayload
	if err := json.Unmarshal(item.Record.Value, &p); err != nil {
		item.Complete()
		return nil
	}

	info, err := h.conversationApplication.GetConversationInfo(ctx, p.ConvID)
	if err != nil {
		return err
	}
	recvID := info.GroupID
	if domainconversation.ConvType(info.ConversationType) == domainconversation.PrivateChat {
		recvID = info.UserID1
		if p.SenderID == info.UserID1 {
			recvID = info.UserID2
		}
	}

	ev := eventport.MessagePushEvent{
		MessageID:      p.MessageID,
		ConversationID: p.ConvID,
		SenderID:       p.SenderID,
		RecvID:         recvID,
		Seq:            p.Seq,
		ConvType:       domainconversation.ConvType(info.ConversationType),
		MessageType:    p.Type,
		Content:        p.Content,
		SendTime:       p.SendTime,
		ClientMsgID:    p.ClientMsgID,
		Status:         int8(domainmessage.Normal),
		AttachmentID:   p.AttachmentID,
		StickerID:      p.StickerID,
		PackID:         p.PackID,
	}

	pull := false
	if ev.ConvType == domainconversation.GroupChat {
		pull = h.trackGroupActivity(ctx, ev)
	}

	if err := h.delivery.Deliver(ev, pull); err != nil {
		log.Printf("推送失败：message=%s err=%v", ev.MessageID, err)
	}

	item.Complete()
	return nil
}

func (h *MessageTransferHandler) trackGroupActivity(
	ctx context.Context, ev eventport.MessagePushEvent,
) bool {
	onlineCount := h.delivery.OnlineGroupConnectionCount(ev.RecvID)
	level, err := h.groupCache.UpdateGroupActivity(ctx, ev.RecvID, 1, onlineCount, time.Now())
	if err != nil {
		level = groupcache.GroupActivityNormal
	}

	if level >= groupcache.GroupActivityWarn {
		// 当达到 warn 级别，需要对消息进行预热
		if err := h.groupCache.WarmRecentMessageEvents(ctx, ev.ConversationID, []eventport.MessagePushEvent{ev}); err != nil {
			log.Printf("活跃期预热近期消息缓存失败：conv=%s seq=%d err=%v", ev.ConversationID, ev.Seq, err)
		}
	}
	return level >= groupcache.GroupActivityActive
}
