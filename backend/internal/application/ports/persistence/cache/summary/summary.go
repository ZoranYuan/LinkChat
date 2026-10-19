package summary

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
	DeleteActive(ctx context.Context, userID, roomID string) error
}

// 用于初始化请求幂等，避免同一个 scope 创建多个摘要任务。
type SummaryScopeRunStore interface {
	GetSummaryRunIDByScope(ctx context.Context, userID, roomID string) (runID string, found bool, err error)
	ClaimSummaryRunIDByScope(ctx context.Context, userID, roomID, runID string, ttl time.Duration) (existingRunID, lockToken string, acquired bool, err error)
	RefreshSummaryRunIDByScope(ctx context.Context, userID, roomID, runID, lockToken string, ttl time.Duration) (bool, error)
	ReleaseSummaryRunIDByScope(ctx context.Context, userID, roomID, runID, lockToken string) error
}
