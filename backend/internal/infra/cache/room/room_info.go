package room

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	roomcache "IM_backend/internal/application/ports/cache/room"
	domainroom "IM_backend/internal/domain/room"
	keycache "IM_backend/internal/infra/cache/key"
	sharedcache "IM_backend/internal/infra/cache/shared"
)

var _ roomcache.RoomInfoCache = (*RoomInfoCache)(nil)

type RoomInfoCache struct {
	store *sharedcache.Store
}

func NewRoomInfoCache(client *redis.Client) *RoomInfoCache {
	return &RoomInfoCache{store: sharedcache.NewStore(client)}
}

func (c *RoomInfoCache) GetRoomInfo(ctx context.Context, roomID string) (*domainroom.Room, bool, error) {
	if roomID == "" {
		return nil, false, errors.New("房间 ID 不能为空")
	}
	if c == nil || c.store == nil || c.store.Client() == nil {
		return nil, false, errors.New("房间基础信息缓存未配置")
	}
	var room domainroom.Room
	found, err := c.store.GetJSON(ctx, RoomInfoKey(roomID), &room)
	if err != nil || !found {
		return nil, found, err
	}
	if room.RoomID == "" || room.RoomID != roomID {
		return nil, false, errors.New("房间基础信息缓存中的房间 ID 无效")
	}
	return &room, true, nil
}

func (c *RoomInfoCache) SetRoomInfo(ctx context.Context, room *domainroom.Room, ttl time.Duration) error {
	if room == nil || room.RoomID == "" {
		return errors.New("房间基础信息不能为空")
	}
	if ttl <= 0 {
		return errors.New("房间基础信息缓存 TTL 必须大于 0")
	}
	if c == nil || c.store == nil || c.store.Client() == nil {
		return errors.New("房间基础信息缓存未配置")
	}
	return c.store.SetJSON(ctx, RoomInfoKey(room.RoomID), room, ttl)
}

func (c *RoomInfoCache) DeleteRoomInfo(ctx context.Context, roomID string) error {
	if roomID == "" {
		return errors.New("房间 ID 不能为空")
	}
	if c == nil || c.store == nil || c.store.Client() == nil {
		return errors.New("房间基础信息缓存未配置")
	}
	return c.store.Del(ctx, RoomInfoKey(roomID))
}

func RoomInfoKey(roomID string) string {
	return keycache.RoomInfo(roomID)
}
