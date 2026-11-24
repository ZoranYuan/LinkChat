package agent

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	summarycache "IM_backend/core/application/ports/cache/agent"
	sharedcache "IM_backend/core/infra/cache/shared"
)

var _ summarycache.GroupUnreadSnapshotCache = (*GroupUnreadSnapshotCache)(nil)

type GroupUnreadSnapshotCache struct {
	store *sharedcache.Store
}

func NewGroupUnreadSnapshotCache(client *redis.Client) *GroupUnreadSnapshotCache {
	return &GroupUnreadSnapshotCache{store: sharedcache.NewStore(client)}
}

func (c *GroupUnreadSnapshotCache) SetActive(
	ctx context.Context,
	userID, groupID string,
	snapshot summarycache.GroupUnreadSnapshot,
	ttl time.Duration,
) error {
	if userID == "" || groupID == "" || snapshot.GroupID == "" {
		return errors.New("摘要未读快照参数不能为空")
	}
	if snapshot.GroupID != groupID {
		return errors.New("摘要未读快照群聊不匹配")
	}
	if !validUnreadRange(snapshot.UnreadBegin, snapshot.UnreadEnd) {
		return errors.New("摘要未读快照序号范围无效")
	}
	if ttl <= 0 {
		return errors.New("摘要未读快照 TTL 必须大于 0")
	}
	return c.store.SetJSON(ctx, ActiveGroupUnreadSnapshotKey(userID, groupID), snapshot, ttl)
}

func validUnreadRange(begin, end int64) bool {
	return (begin == 0 && end == 0) || (begin > 0 && end >= begin)
}
