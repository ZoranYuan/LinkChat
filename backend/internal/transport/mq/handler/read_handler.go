package handler

import (
	eventbus "IM_backend/internal/application/ports/eventbus"
	realtimeport "IM_backend/internal/application/ports/realtime"
	"IM_backend/internal/shared/protocol"
	"context"
	"encoding/json"
	"fmt"
)

type ReadHandler struct {
	delivery realtimeport.UserDelivery
}

func NewReadHandler(delivery realtimeport.UserDelivery) eventbus.Handler {
	return &ReadHandler{delivery: delivery}
}

func (handler *ReadHandler) Handle(ctx context.Context, message eventbus.IncomingEvent) error {
	var event protocol.MessageReadCommittedEvent
	if err := json.Unmarshal(message.Payload, &event); err != nil {
		return eventbus.NonRetryable(err)
	}
	if event.RecipientUserId == "" || event.ReaderId == "" || event.ConversationId == "" || event.LastReadSeq <= 0 {
		return eventbus.NonRetryable(fmt.Errorf("已读事件缺少必要字段"))
	}

	notify := protocol.MessageReadAckEvent{
		UserId:         event.ReaderId,
		ConversationId: event.ConversationId,
		LastReadSeq:    event.LastReadSeq,
		ConvType:       event.ConvType,
		Avatar:         event.Avatar,
	}
	payload, err := json.Marshal(notify)
	if err != nil {
		return err
	}

	if err := handler.delivery.DeliverToUser(
		protocol.EventReadMessageNotify,
		event.RecipientUserId,
		payload,
	); err != nil {
		return err
	}
	return nil
}
