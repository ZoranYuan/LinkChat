package handler

import (
	messageapp "IM_backend/internal/application/message"
	eventbus "IM_backend/internal/application/ports/eventbus"
	"IM_backend/internal/shared/protocol"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type deliveryStub struct {
	eventType string
	convID    string
	payload   []byte
	event     protocol.MessageEvent
	err       error
}

func (stub *deliveryStub) Deliver(
	_ context.Context,
	eventType string,
	event protocol.MessageEvent,
	payload []byte,
) error {
	stub.eventType = eventType
	stub.convID = event.ConversationId
	stub.payload = payload
	stub.event = event
	return stub.err
}

func TestMessageHandlerDecodesBusinessEventWithoutUsingPartitionKeyAsTarget(t *testing.T) {
	event := protocol.MessageEvent{MessageId: "m1", SenderId: "u1", RecvId: "u2", ConversationId: "c1", ConvType: protocol.PrivateChat, Seq: 2}
	payload, _ := json.Marshal(event)
	stub := &deliveryStub{}
	handler := NewMessageHandler(stub)
	defer handler.Close(context.Background())

	err := handler.Handle(context.Background(), eventbus.IncomingEvent{
		Name:    protocol.EventTypeSendMessage,
		Key:     []byte("partition-only-key"),
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("处理消息失败：%v", err)
	}
	if stub.eventType != protocol.EventTypeSendMessage || stub.convID != "c1" {
		t.Fatalf("投递事件元数据错误：event=%s conv=%s", stub.eventType, stub.convID)
	}
	if stub.event.SenderId != "u1" || stub.event.RecvId != "u2" || !bytes.Equal(stub.payload, payload) {
		t.Fatalf("投递业务事件错误：%+v", stub.event)
	}
	if stub.event.MessageId != "m1" || stub.event.Seq != 2 {
		t.Fatalf("投递事件载荷错误：%+v", stub.event)
	}
}

func TestMessageHandlerMarksMalformedPayloadPermanent(t *testing.T) {
	handler := NewMessageHandler(&deliveryStub{})
	defer handler.Close(context.Background())
	err := handler.Handle(context.Background(), eventbus.IncomingEvent{Payload: []byte("not-json")})

	var permanentError *eventbus.NonRetryableError
	if !errors.As(err, &permanentError) {
		t.Fatalf("非法消息应标记为永久错误：%v", err)
	}
}

func TestRoomMessageHasIndependentConversationID(t *testing.T) {
	event := protocol.MessageEvent{MessageId: "m", SenderId: "u", RecvId: "room", ConversationId: "conversation", ConvType: protocol.RoomChat, Seq: 1}
	payload, _ := json.Marshal(event)
	stub := &deliveryStub{}
	err := NewMessageHandler(stub).Handle(context.Background(), eventbus.IncomingEvent{Name: protocol.EventTypeSendMessage, Payload: payload})
	if err != nil || stub.event.RecvId != "room" || stub.convID != "conversation" {
		t.Fatalf("event=%+v err=%v", stub.event, err)
	}
}

func TestMessageHandlerRejectsNestedPayload(t *testing.T) {
	handler := NewMessageHandler(&deliveryStub{})
	err := handler.Handle(context.Background(), eventbus.IncomingEvent{Payload: []byte(`{"From":"a","To":"b","Payload":"e30="}`)})
	if !eventbus.IsNonRetryable(err) {
		t.Fatalf("Consumer 不应保留旧包装格式的解码路径：%v", err)
	}
}

func TestMessageHandlerMarksUnknownConversationTypePermanent(t *testing.T) {
	event := protocol.MessageEvent{MessageId: "m1", SenderId: "u1", RecvId: "u2", ConversationId: "c1", ConvType: 99, Seq: 2}
	payload, _ := json.Marshal(event)
	handler := NewMessageHandler(&deliveryStub{err: messageapp.ErrUnknownConversationType})
	defer handler.Close(context.Background())

	err := handler.Handle(context.Background(), eventbus.IncomingEvent{
		Name:    protocol.EventTypeSendMessage,
		Key:     []byte("c1"),
		Payload: payload,
	})
	var permanentError *eventbus.NonRetryableError
	if !errors.As(err, &permanentError) {
		t.Fatalf("未知会话类型应标记为永久错误：%v", err)
	}
}
