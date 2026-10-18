package handler

import (
	"context"
	"encoding/json"
	"testing"

	eventbus "IM_backend/internal/application/ports/eventbus"
	"IM_backend/internal/shared/protocol"
)

func TestReadHandlerUsesExplicitRecipientAndPreservesCommittedWatermark(t *testing.T) {
	event := protocol.MessageReadCommittedEvent{
		ReaderId: "reader", RecipientUserId: "sender", ConversationId: "conversation",
		LastReadSeq: 101, ConvType: protocol.PrivateChat,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	delivery := &friendRequestDeliveryStub{}
	if err := NewReadHandler(delivery).Handle(context.Background(), eventbus.IncomingEvent{
		Name: protocol.EventReadMessageCommitted, Key: []byte("reader:conversation"), Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	var notice protocol.MessageReadAckEvent
	if err := json.Unmarshal(delivery.payload, &notice); err != nil {
		t.Fatal(err)
	}
	if delivery.userID != "sender" || delivery.eventType != protocol.EventReadMessageNotify || notice.UserId != "reader" || notice.LastReadSeq != 101 || notice.ConversationId != "conversation" {
		t.Fatalf("已读通知目标或水位错误：target=%s notice=%+v", delivery.userID, notice)
	}
}

func TestReadHandlerRejectsMissingRecipientBeforeDelivery(t *testing.T) {
	payload, _ := json.Marshal(protocol.MessageReadCommittedEvent{
		ReaderId: "reader", ConversationId: "conversation", LastReadSeq: 101, ConvType: protocol.PrivateChat,
	})
	delivery := &friendRequestDeliveryStub{}
	err := NewReadHandler(delivery).Handle(context.Background(), eventbus.IncomingEvent{Payload: payload})
	if !eventbus.IsNonRetryable(err) || delivery.userID != "" {
		t.Fatalf("缺少接收者时必须拒绝投递：err=%v target=%s", err, delivery.userID)
	}
}
