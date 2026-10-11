package key

import "strings"

const Prefix = "im"

func Build(parts ...string) string {
	items := make([]string, 0, len(parts)+1)
	items = append(items, Prefix)
	for _, part := range parts {
		part = strings.Trim(part, ":")
		if part == "" {
			continue
		}
		items = append(items, part)
	}
	return strings.Join(items, ":")
}

func ActivateLevelKey(roomId string) string {
	return Build("room", "activity", roomId)
}

func AuthRefreshToken(token string) string {
	return Build("auth", "token", "refresh", token)
}

func AuthRevokedSession(sessionID string) string {
	return Build("auth", "session", "revoked", sessionID)
}

func ConversationSeq(conversationId string) string {
	return Build("conversation", conversationId, "seq")
}

func FileMeta(fileId string) string {
	return Build("file", fileId, "meta")
}

func UserInfo(userId string) string {
	return Build("user", userId, "info")
}

func FriendRelation(userID, friendID string) string {
	return Build("friend", "relation", userID, friendID)
}

func RoomInviteCode(code string) string {
	return Build("room", "invite", code)
}

func RoomInvite(roomId string) string {
	return Build("room", roomId, "invite")
}

func RoomInfo(roomID string) string {
	return Build("room", roomID, "info")
}

func RoomMember(roomID, userID string) string {
	return Build("room", roomID, "member", "v2", userID)
}

func MessageDedup(senderID, clientMsgId string) string {
	return Build("msg", "dedup", senderID, clientMsgId)
}
