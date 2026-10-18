package handler

import (
	messageapp "IM_backend/internal/application/message"
	eventbus "IM_backend/internal/application/ports/eventbus"
	"IM_backend/internal/shared/diagnostics"
	"IM_backend/internal/shared/protocol"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type MessageDelivery interface {
	Deliver(
		ctx context.Context,
		eventType string,
		event protocol.MessageEvent,
		payload []byte,
	) error
}

type messageDeliveryCloser interface {
	Close(ctx context.Context)
}

type MessageHandler struct {
	delivery MessageDelivery
}

func NewMessageHandler(delivery MessageDelivery) *MessageHandler {
	return &MessageHandler{
		delivery: delivery,
	}
}

func (handler *MessageHandler) Close(ctx context.Context) {
	if closer, ok := handler.delivery.(messageDeliveryCloser); ok {
		closer.Close(ctx)
	}
}

func (handler *MessageHandler) Handle(ctx context.Context, message eventbus.IncomingEvent) error {
	startedAt := time.Now()
	var messageEvent protocol.MessageEvent
	if err := json.Unmarshal(message.Payload, &messageEvent); err != nil {
		return eventbus.NonRetryable(err)
	}
	if messageEvent.MessageId == "" || messageEvent.SenderId == "" || messageEvent.RecvId == "" || messageEvent.ConversationId == "" || messageEvent.Seq <= 0 {
		return eventbus.NonRetryable(fmt.Errorf("消息事件缺少必要字段"))
	}

	decodeDuration := time.Since(startedAt)
	deliveryStartedAt := time.Now()
	err := handler.delivery.Deliver(ctx, message.Name, messageEvent, message.Payload)
	deliveryDuration := time.Since(deliveryStartedAt)
	eventAge := time.Duration(0)
	if messageEvent.SendTime > 0 {
		eventAge = time.Since(time.UnixMilli(messageEvent.SendTime))
	}
	diagnostics.Logf("stage=message_handler event_id=%s client_msg_id=%s message_id=%s conversation_id=%s seq=%d event_age_ms=%d decode_us=%d delivery_us=%d total_us=%d outcome=%s",
		message.EventID, messageEvent.ClientMsgId, messageEvent.MessageId, messageEvent.ConversationId, messageEvent.Seq, eventAge.Milliseconds(), decodeDuration.Microseconds(),
		deliveryDuration.Microseconds(), time.Since(startedAt).Microseconds(), handlerOutcome(err))
	if errors.Is(err, messageapp.ErrUnknownConversationType) {
		return eventbus.NonRetryable(err)
	}
	return err
}

func handlerOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "delivered"
}
