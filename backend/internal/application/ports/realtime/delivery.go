package realtime

import "IM_backend/internal/application/ports/messaging"

type UserDelivery interface {
	DeliverMessageToUser(userID string, event messaging.MessagePushEvent) error
	DeliverMessageReadNotice(userID string, event messaging.MessageReadNoticeEvent) error
	DeliverFriendRequestNotice(userID string, event messaging.FriendRequestNoticeEvent) error
}

type RoomMemberDelivery interface {
	DeliverMessageToOnlineRoomMembers(roomID string, event messaging.MessagePushEvent, excludeUserID string) error
	DeliverRoomMessageNotice(roomID string, event messaging.RoomMessageNoticeEvent) error
}

type RoomPresence interface {
	BindUserToRoom(userID, roomID string)
	UnbindUserFromRoom(userID, roomID string)
}
