package realtime

type UserDelivery interface {
	DeliverToUser(eventType, userID string, payload []byte) error
}

type RoomMemberDelivery interface {
	DeliverToOnlineRoomMembers(eventType, roomID string, payload []byte, excludeUserID string) error
}

type RoomPresence interface {
	BindUserToRoom(userID, roomID string)
	UnbindUserFromRoom(userID, roomID string)
}
