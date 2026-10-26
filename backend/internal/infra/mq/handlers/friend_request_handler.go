package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"IM_backend/internal/application/ports/messaging"
	realtimeport "IM_backend/internal/application/ports/realtime"
)

type FriendRequestHandler struct {
	delivery realtimeport.UserDelivery
}

func NewFriendRequestHandler(delivery realtimeport.UserDelivery) messaging.Handler {
	return &FriendRequestHandler{delivery: delivery}
}

func (handler *FriendRequestHandler) Handle(_ context.Context, message messaging.IncomingEvent) error {
	var event messaging.FriendRequestCreatedEvent
	if err := json.Unmarshal(message.Payload, &event); err != nil {
		return messaging.NonRetryable(err)
	}
	if event.RequestID == "" || event.ApplicantUserID == "" || event.PeerUserID == "" {
		return messaging.NonRetryable(fmt.Errorf("好友申请事件缺少必要字段"))
	}

	return handler.delivery.DeliverToUser(
		messaging.TypeFriendRequestCreated,
		event.PeerUserID,
		message.Payload,
	)
}
