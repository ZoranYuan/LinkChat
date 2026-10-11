package room

import (
	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	roomentity "IM_backend/internal/domain/room/entity"
	roomvo "IM_backend/internal/domain/room/value_object"
	cachekey "IM_backend/internal/infrastructure/persistence/redis/cache/key"
	"IM_backend/internal/infrastructure/persistence/redis/cache/shared"
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ roomcache.RoomInfoCache = (*RoomInfoCache)(nil)

type RoomInfoCache struct {
	store *shared.Store
}

type roomInfoEntry struct {
	RoomID      string `json:"roomId"`
	OwnerUserID string `json:"ownerUserId"`
	Description string `json:"description"`
	RoomName    string `json:"roomName"`
	Status      int    `json:"status"`
	Avatar      string `json:"avatar"`
	MemberCount int    `json:"memberCount"`
	MaxMembers  int    `json:"maxMembers"`
}

func NewRoomInfoCache(client *redis.Client) *RoomInfoCache {
	return &RoomInfoCache{store: shared.NewStore(client)}
}

func (c *RoomInfoCache) GetRoomInfo(ctx context.Context, roomID string) (*roomentity.Room, bool, error) {
	if roomID == "" {
		return nil, false, errors.New("房间 ID 不能为空")
	}
	if c == nil || c.store == nil || c.store.Client() == nil {
		return nil, false, errors.New("房间基础信息缓存未配置")
	}
	var entry roomInfoEntry
	found, err := c.store.GetJSON(ctx, RoomInfoKey(roomID), &entry)
	if err != nil || !found {
		return nil, found, err
	}
	if entry.RoomID == "" || entry.RoomID != roomID {
		return nil, false, errors.New("房间基础信息缓存中的房间 ID 无效")
	}
	return &roomentity.Room{
		RoomId:      entry.RoomID,
		OwnerUserId: entry.OwnerUserID,
		Description: entry.Description,
		RoomName:    entry.RoomName,
		Status:      roomvo.RoomStatus(entry.Status),
		Avatar:      entry.Avatar,
		MemberCount: entry.MemberCount,
		MaxMembers:  entry.MaxMembers,
	}, true, nil
}

func (c *RoomInfoCache) SetRoomInfo(ctx context.Context, room *roomentity.Room, ttl time.Duration) error {
	if room == nil || room.RoomId == "" {
		return errors.New("房间基础信息不能为空")
	}
	if ttl <= 0 {
		return errors.New("房间基础信息缓存 TTL 必须大于 0")
	}
	if c == nil || c.store == nil || c.store.Client() == nil {
		return errors.New("房间基础信息缓存未配置")
	}
	return c.store.SetJSON(ctx, RoomInfoKey(room.RoomId), roomInfoEntry{
		RoomID:      room.RoomId,
		OwnerUserID: room.OwnerUserId,
		Description: room.Description,
		RoomName:    room.RoomName,
		Status:      int(room.Status),
		Avatar:      room.Avatar,
		MemberCount: room.MemberCount,
		MaxMembers:  room.MaxMembers,
	}, ttl)
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
	return cachekey.RoomInfo(roomID)
}
