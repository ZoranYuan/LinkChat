package group

import (
	keycache "IM_backend/core/infra/cache/key"
)

func InviteKey(code string) string {
	return keycache.GroupInviteCode(code)
}

func ActivateLevelKey(groupID string) string {
	return keycache.ActivateLevelKey(groupID)
}

func InviteKeyPrefix() string {
	return keycache.GroupInviteCode("") + ":"
}

func RecentMessageSeqKey(conversationID string) string {
	return keycache.Build("conversation", "recent", "seq", conversationID)
}

// 群聊ID -> 邀请码
func GroupInviteKey(groupID string) string {
	return keycache.GroupInvite(groupID)
}

func GroupMembersKey(groupID string) string {
	return keycache.GroupMembers(groupID)
}
