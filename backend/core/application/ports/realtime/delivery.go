package realtime

import eventport "IM_backend/core/application/ports/events"

type UserDelivery interface {
	DeliverMessageToUser(userID string, event eventport.MessagePushEvent) error
	DeliverMessageReadNotice(userID string, event eventport.MessageReadNoticeEvent) error
	DeliverFriendRequestNotice(userID string, event eventport.FriendRequestNoticeEvent) error
}

type GroupMemberDelivery interface {
	DeliverMessageToOnlineGroupMembers(groupID string, event eventport.MessagePushEvent, excludeUserID string) error
	DeliverGroupMessageNotice(groupID string, event eventport.GroupMessageNoticeEvent) error
}
