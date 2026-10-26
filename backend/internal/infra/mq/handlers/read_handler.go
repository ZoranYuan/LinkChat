package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"IM_backend/internal/application/ports/messaging"
	realtimeport "IM_backend/internal/application/ports/realtime"
)

type ReadHandler struct {
	delivery realtimeport.UserDelivery
}

func NewReadHandler(delivery realtimeport.UserDelivery) messaging.Handler {
	return &ReadHandler{delivery: delivery}
}

func (handler *ReadHandler) Handle(ctx context.Context, message messaging.IncomingEvent) error {
	var event messaging.MessageReadCommittedEvent
	if err := json.Unmarshal(message.Payload, &event); err != nil {
		return messaging.NonRetryable(err)
	}
	if event.RecipientUserID == "" || event.ReaderID == "" || event.ConversationID == "" || event.LastReadSeq <= 0 {
		return messaging.NonRetryable(fmt.Errorf("已读事件缺少必要字段"))
	}

	notify := messaging.MessageReadAckEvent{
		UserID:         event.ReaderID,
		ConversationID: event.ConversationID,
		LastReadSeq:    event.LastReadSeq,
		ConvType:       event.ConvType,
		Avatar:         event.Avatar,
	}
	payload, err := json.Marshal(notify)
	if err != nil {
		return err
	}

	if err := handler.delivery.DeliverToUser(
		messaging.TypeReadNotify,
		event.RecipientUserID,
		payload,
	); err != nil {
		return err
	}
	return nil
}
