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

func ActivateLevelKey(groupID string) string {
	return Build("group", "activity", groupID)
}

func AuthRefreshToken(token string) string {
	return Build("auth", "token", "refresh", token)
}

func AuthSession(sessionID string) string {
	return Build("auth", "session", sessionID)
}

func GroupInviteCode(code string) string {
	return Build("group", "invite", code)
}

func GroupInvite(groupID string) string {
	return Build("group", groupID, "invite")
}

func GroupInfo(groupID string) string {
	return Build("group", groupID, "info")
}

func GroupMembers(groupID string) string {
	return Build("group", groupID, "members")
}

func MessageDedup(senderID, clientMsgID string) string {
	return Build("msg", "dedup", senderID, clientMsgID)
}
