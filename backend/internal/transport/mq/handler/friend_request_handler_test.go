package handler

import (
	eventbus "IM_backend/internal/application/ports/eventbus"
	"IM_backend/internal/shared/protocol"
	"context"
	"encoding/json"
	"testing"
)

type friendRequestDeliveryStub struct {
	eventType string
	userID    string
	payload   []byte
}

func (stub *friendRequestDeliveryStub) DeliverToUser(eventType, userID string, payload []byte) error {
	stub.eventType = eventType
	stub.userID = userID
	stub.payload = payload
	return nil
}

func TestFriendRequestHandlerDeliversToPeerUser(t *testing.T) {
	notification := protocol.FriendRequestCreatedEvent{
		RequestId:       "r1",
		ApplicantUserId: "u1",
		PeerUserId:      "u2",
		Message:         "hi",
		ApplyTime:       123,
	}
	payload, _ := json.Marshal(notification)

	stub := &friendRequestDeliveryStub{}
	handler := NewFriendRequestHandler(stub)
	err := handler.Handle(context.Background(), eventbus.IncomingEvent{
		Name:    protocol.EventFriendRequestCreated,
		Key:     []byte("partition-only-key"),
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("处理好友申请提醒失败：%v", err)
	}
	if stub.eventType != protocol.EventFriendRequestCreated || stub.userID != "u2" {
		t.Fatalf("投递目标错误：event=%s user=%s", stub.eventType, stub.userID)
	}

	var got protocol.FriendRequestCreatedEvent
	if err := json.Unmarshal(stub.payload, &got); err != nil {
		t.Fatalf("解析投递载荷失败：%v", err)
	}
	if got.RequestId != notification.RequestId || got.ApplicantUserId != notification.ApplicantUserId {
		t.Fatalf("投递载荷错误：got=%+v want=%+v", got, notification)
	}
}
