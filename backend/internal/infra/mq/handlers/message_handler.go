package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	messageapp "IM_backend/internal/application/message"
	"IM_backend/internal/application/ports/messaging"
)

type MessageDelivery interface {
	Deliver(
		ctx context.Context,
		eventType string,
		event messaging.MessageEvent,
		payload []byte,
	) error
}

type MessageHandler struct {
	delivery MessageDelivery
}

func NewMessageHandler(delivery MessageDelivery) *MessageHandler {
	return &MessageHandler{
		delivery: delivery,
	}
}

func (handler *MessageHandler) Handle(ctx context.Context, message messaging.IncomingEvent) error {
	var messageEvent messaging.MessageEvent
	if err := json.Unmarshal(message.Payload, &messageEvent); err != nil {
		return messaging.NonRetryable(err)
	}
	if messageEvent.MessageID == "" || messageEvent.SenderID == "" || messageEvent.RecvID == "" || messageEvent.ConversationID == "" || messageEvent.Seq <= 0 {
		return messaging.NonRetryable(fmt.Errorf("消息事件缺少必要字段"))
	}

	err := handler.delivery.Deliver(ctx, message.Name, messageEvent, message.Payload)
	if errors.Is(err, messageapp.ErrUnknownConversationType) {
		return messaging.NonRetryable(err)
	}
	return err
}
