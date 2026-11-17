package room

import (
	keycache "IM_backend/internal/infra/cache/key"
)

func InviteKey(code string) string {
	return keycache.RoomInviteCode(code)
}

func ActivateLevelKey(roomID string) string {
	return keycache.ActivateLevelKey(roomID)
}

func InviteKeyPrefix() string {
	return keycache.RoomInviteCode("") + ":"
}

func RecentMessageSeqKey(conversationID string) string {
	return keycache.Build("conversation", "recent", "seq", conversationID)
}

// 房间ID -> 邀请码
func RoomInviteKey(roomID string) string {
	return keycache.RoomInvite(roomID)
}

func RoomMembersKey(roomID string) string {
	return keycache.RoomMembers(roomID)
}
