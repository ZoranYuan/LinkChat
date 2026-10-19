package room

import (
	cachekey "IM_backend/internal/infrastructure/persistence/redis/cache/key"
)

func InviteKey(code string) string {
	return cachekey.RoomInviteCode(code)
}

func ActivateLevelKey(roomId string) string {
	return cachekey.ActivateLevelKey(roomId)
}

func InviteKeyPrefix() string {
	return cachekey.RoomInviteCode("") + ":"
}

func RecentMessageSeqKey(roomID string) string {
	return cachekey.Build("room", "recent", "seq", roomID)
}

// 房间ID -> 邀请码
func RoomInviteKey(roomId string) string {
	return cachekey.RoomInvite(roomId)
}

func RoomMembersKey(roomID string) string {
	return cachekey.RoomMembers(roomID)
}
