package key

import (
	"strings"
)

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

func ActivateLevelKey(roomID string) string {
	return Build("room", "activity", roomID)
}

func AuthRefreshToken(token string) string {
	return Build("auth", "token", "refresh", token)
}

func AuthSession(sessionID string) string {
	return Build("auth", "session", sessionID)
}

func FileMeta(fileID string) string {
	return Build("file", fileID, "meta")
}

func RoomInviteCode(code string) string {
	return Build("room", "invite", code)
}

func RoomInvite(roomID string) string {
	return Build("room", roomID, "invite")
}

func RoomInfo(roomID string) string {
	return Build("room", roomID, "info")
}

func RoomMembers(roomID string) string {
	return Build("room", roomID, "members")
}

func MessageDedup(senderID, clientMsgID string) string {
	return Build("msg", "dedup", senderID, clientMsgID)
}
