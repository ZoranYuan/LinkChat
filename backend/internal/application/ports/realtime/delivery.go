package realtime

import "context"

type UserDelivery interface {
	DeliverToUser(eventType, userID string, payload []byte) error
}

type RoomMemberDelivery interface {
	DeliverToOnlineRoomMembers(eventType, roomID string, payload []byte, excludeUserID string) error
}

type RealtimeDelivery interface {
	UserDelivery
	RoomMemberDelivery
}

type RoomPresence interface {
	BindUserToRoom(userID, roomID string)
	UnbindUserFromRoom(userID, roomID string)
}

type RoomOnlineConnectionCounter interface {
	OnlineRoomConnectionCount(ctx context.Context, roomID string) (int, error)
}
