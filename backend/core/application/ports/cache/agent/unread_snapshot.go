package agent

import (
	"context"
	"time"
)

// GroupUnreadSnapshot 保存用户进入群聊前，本次摘要应该覆盖的固定消息范围。
type GroupUnreadSnapshot struct {
	GroupID      string `json:"groupId"`
	UnreadBegin int64  `json:"unreadBegin"`
	UnreadEnd   int64  `json:"unreadEnd"`
}

type GroupUnreadSnapshotCache interface {
	SetActive(ctx context.Context, userID, groupID string, snapshot GroupUnreadSnapshot, ttl time.Duration) error
}
