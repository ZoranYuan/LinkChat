package agent

import (
	keycache "IM_backend/core/infra/cache/key"
)

func ActiveGroupUnreadSnapshotKey(userID, groupID string) string {
	return keycache.Build("summary", "active", "group", "unread", userID, groupID)
}
