package agent

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	summarycache "IM_backend/internal/application/ports/cache/agent"
	sharedcache "IM_backend/internal/infra/cache/shared"
)

var _ summarycache.RoomUnreadSnapshotCache = (*RoomUnreadSnapshotCache)(nil)

type RoomUnreadSnapshotCache struct {
	store *sharedcache.Store
}

func NewRoomUnreadSnapshotCache(client *redis.Client) *RoomUnreadSnapshotCache {
	return &RoomUnreadSnapshotCache{store: sharedcache.NewStore(client)}
}

func (c *RoomUnreadSnapshotCache) SetActive(
	ctx context.Context,
	userID, roomID string,
	snapshot summarycache.RoomUnreadSnapshot,
	ttl time.Duration,
) error {
	if userID == "" || roomID == "" || snapshot.RoomID == "" {
		return errors.New("摘要未读快照参数不能为空")
	}
	if snapshot.RoomID != roomID {
		return errors.New("摘要未读快照房间不匹配")
	}
	if !validUnreadRange(snapshot.UnreadBegin, snapshot.UnreadEnd) {
		return errors.New("摘要未读快照序号范围无效")
	}
	if ttl <= 0 {
		return errors.New("摘要未读快照 TTL 必须大于 0")
	}
	return c.store.SetJSON(ctx, ActiveRoomUnreadSnapshotKey(userID, roomID), snapshot, ttl)
}

func (c *RoomUnreadSnapshotCache) GetActive(
	ctx context.Context,
	userID, roomID string,
) (*summarycache.RoomUnreadSnapshot, bool, error) {
	if userID == "" || roomID == "" {
		return nil, false, errors.New("用户和房间标识不能为空")
	}

	var snapshot summarycache.RoomUnreadSnapshot
	found, err := c.store.GetJSON(ctx, ActiveRoomUnreadSnapshotKey(userID, roomID), &snapshot)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	if snapshot.RoomID != roomID || !validUnreadRange(snapshot.UnreadBegin, snapshot.UnreadEnd) {
		if err := c.store.Del(ctx, ActiveRoomUnreadSnapshotKey(userID, roomID)); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	return &snapshot, true, nil
}

func validUnreadRange(begin, end int64) bool {
	return (begin == 0 && end == 0) || (begin > 0 && end >= begin)
}
