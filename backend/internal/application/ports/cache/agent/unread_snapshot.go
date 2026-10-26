package agent

import (
	"context"
	"time"
)

// RoomUnreadSnapshot 保存用户进入房间前，本次摘要应该覆盖的固定消息范围。
type RoomUnreadSnapshot struct {
	RoomID      string `json:"roomId"`
	UnreadBegin int64  `json:"unreadBegin"`
	UnreadEnd   int64  `json:"unreadEnd"`
}

type RoomUnreadSnapshotCache interface {
	SetActive(ctx context.Context, userID, roomID string, snapshot RoomUnreadSnapshot, ttl time.Duration) error
	GetActive(ctx context.Context, userID, roomID string) (*RoomUnreadSnapshot, bool, error)
}
