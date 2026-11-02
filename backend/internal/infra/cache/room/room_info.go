package room

import (
	"context"
	"encoding/json"
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

func (c *RoomInfoCache) GetRoomInfos(ctx context.Context, roomIDs []string) (map[string]*domainroom.Room, error) {
	rooms := make(map[string]*domainroom.Room, len(roomIDs))
	if len(roomIDs) == 0 {
		return rooms, nil
	}
	keys := make([]string, len(roomIDs))
	for i, roomID := range roomIDs {
		keys[i] = RoomInfoKey(roomID)
	}
	values, err := c.store.Client().MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		if value == nil {
			continue
		}
		data, ok := value.(string)
		if !ok {
			continue
		}
		var room domainroom.Room
		if err := json.Unmarshal([]byte(data), &room); err != nil {
			return nil, err
		}
		if room.RoomID == roomIDs[i] {
			rooms[room.RoomID] = &room
		}
	}
	return rooms, nil
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

func (c *RoomInfoCache) SetRoomInfos(ctx context.Context, rooms []*domainroom.Room, ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("房间基础信息缓存 TTL 必须大于 0")
	}
	items := make(map[string][]byte, len(rooms))
	for _, room := range rooms {
		if room == nil || room.RoomID == "" {
			continue
		}
		data, err := json.Marshal(room)
		if err != nil {
			return err
		}
		items[RoomInfoKey(room.RoomID)] = data
	}
	_, err := c.store.Client().Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for key, data := range items {
			pipe.Set(ctx, key, data, ttl)
		}
		return nil
	})
	return err
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
