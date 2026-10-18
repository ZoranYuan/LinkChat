package handler

import (
	eventbus "IM_backend/internal/application/ports/eventbus"
	realtimeport "IM_backend/internal/application/ports/realtime"
	"IM_backend/internal/shared/protocol"
	"context"
	"encoding/json"
	"fmt"
)

type FriendRequestHandler struct {
	delivery realtimeport.UserDelivery
}

func NewFriendRequestHandler(delivery realtimeport.UserDelivery) eventbus.Handler {
	return &FriendRequestHandler{delivery: delivery}
}

func (handler *FriendRequestHandler) Handle(_ context.Context, message eventbus.IncomingEvent) error {
	var event protocol.FriendRequestCreatedEvent
	if err := json.Unmarshal(message.Payload, &event); err != nil {
		return eventbus.NonRetryable(err)
	}
	if event.RequestId == "" || event.ApplicantUserId == "" || event.PeerUserId == "" {
		return eventbus.NonRetryable(fmt.Errorf("好友申请事件缺少必要字段"))
	}

	return handler.delivery.DeliverToUser(
		protocol.EventFriendRequestCreated,
		event.PeerUserId,
		message.Payload,
	)
}
