package agent

import (
	keycache "IM_backend/internal/infra/cache/key"
)

func ActiveRoomUnreadSnapshotKey(userID, roomID string) string {
	return keycache.Build("summary", "active", "room", "unread", userID, roomID)
}
