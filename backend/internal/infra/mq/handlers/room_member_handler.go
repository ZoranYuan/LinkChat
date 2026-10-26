package handlers

import (
	"context"
	"encoding/json"

	"IM_backend/internal/application/ports/messaging"
	roomapp "IM_backend/internal/application/room"
)

type RoomMemberChangedHandler struct {
	application *roomapp.RoomApplication
}

func NewRoomMemberChangedHandler(application *roomapp.RoomApplication) messaging.Handler {
	return &RoomMemberChangedHandler{application: application}
}

func (handler *RoomMemberChangedHandler) Handle(ctx context.Context, message messaging.IncomingEvent) error {
	var event messaging.RoomMemberChangedEvent
	if err := json.Unmarshal(message.Payload, &event); err != nil {
		return messaging.NonRetryable(err)
	}
	if event.RoomID == "" || event.UserID == "" || event.Version <= 0 {
		return messaging.NonRetryable(roomapp.ErrInvalidMemberChangedEvent)
	}
	return handler.application.ApplyMemberChanged(ctx, event)
}
