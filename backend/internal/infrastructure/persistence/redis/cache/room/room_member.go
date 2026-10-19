package room

import (
	"IM_backend/configs"
	roomcache "IM_backend/internal/application/ports/persistence/cache/room"
	roomvo "IM_backend/internal/domain/room/value_object"
	"IM_backend/internal/infrastructure/persistence/redis/cache/shared"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ roomcache.RoomMemberCache = (*RoomMemberCache)(nil)

type RoomMemberCache struct {
	store  *shared.Store
	config configs.MessageConfig
}

func NewRoomMemberCache(client *redis.Client, config configs.MessageConfig) *RoomMemberCache {
	return &RoomMemberCache{store: shared.NewStore(client), config: config}
}

type roomMemberTTLSettings struct {
	state    time.Duration
	negative time.Duration
}

func (c *RoomMemberCache) ttlSettings() (roomMemberTTLSettings, error) {
	if c.config.RoomMemberStateTTLSeconds <= 0 {
		return roomMemberTTLSettings{}, fmt.Errorf("成员状态缓存 TTL 必须大于 0")
	}
	if c.config.RoomMemberNegativeTTLSeconds <= 0 {
		return roomMemberTTLSettings{}, fmt.Errorf("成员负缓存 TTL 必须大于 0")
	}
	return roomMemberTTLSettings{
		state:    time.Duration(c.config.RoomMemberStateTTLSeconds) * time.Second,
		negative: time.Duration(c.config.RoomMemberNegativeTTLSeconds) * time.Second,
	}, nil
}

type roomMemberEntry struct {
	Found     bool   `json:"found"`
	Status    int    `json:"status,omitempty"`
	Role      int    `json:"role,omitempty"`
	MuteUntil *int64 `json:"muteUntil,omitempty"`
	Version   int64  `json:"version,omitempty"`
}

func (c *RoomMemberCache) GetMember(
	ctx context.Context,
	roomID, userID string,
) (*roomcache.MemberState, bool, error) {
	key := RoomMembersKey(roomID)
	data, err := c.store.Client().HGet(ctx, key, userID).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var entry roomMemberEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		_, _ = c.store.Eval(ctx, deleteMemberFieldIfUnchangedScript, []string{key}, userID, string(data))
		return nil, false, fmt.Errorf("解析房间成员缓存失败：%w", err)
	}
	if !entry.Found {
		return nil, true, nil
	}
	return &roomcache.MemberState{
		Status:    roomvo.RoomUserStatus(entry.Status),
		Role:      roomvo.Role(entry.Role),
		MuteUntil: entry.MuteUntil,
		Version:   entry.Version,
	}, true, nil
}

func (c *RoomMemberCache) SetMemberIfVersionGreater(
	ctx context.Context,
	roomID, userID string,
	state *roomcache.MemberState,
) (bool, error) {
	if state == nil {
		return false, fmt.Errorf("房间成员状态不能为空")
	}
	ttls, err := c.ttlSettings()
	if err != nil {
		return false, err
	}
	entry := roomMemberEntry{
		Found: true, Status: int(state.Status), Role: int(state.Role),
		MuteUntil: state.MuteUntil, Version: state.Version,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}
	result, err := c.store.Eval(ctx, setMemberIfVersionGreaterScript, []string{RoomMembersKey(roomID)},
		userID, string(data), state.Version, int64(ttls.state/time.Second))
	if err != nil {
		return false, err
	}
	value, ok := result.(int64)
	return ok && value == 1, nil
}

func (c *RoomMemberCache) SetMemberNotFound(ctx context.Context, roomID, userID string) error {
	ttls, err := c.ttlSettings()
	if err != nil {
		return err
	}
	entry := roomMemberEntry{Found: false}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = c.store.Eval(ctx, setMemberNotFoundScript, []string{RoomMembersKey(roomID)},
		userID, string(data), int64(ttls.negative/time.Second))
	return err
}

const deleteMemberFieldIfUnchangedScript = `
	if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
		return redis.call('HDEL', KEYS[1], ARGV[1])
	end
	return 0
`

const setMemberIfVersionGreaterScript = `
	local currentData = redis.call('HGET', KEYS[1], ARGV[1])
	if currentData then
		local ok, current = pcall(cjson.decode, currentData)
		if ok and current and current.found == true and tonumber(current.version or 0) >= tonumber(ARGV[3]) then
			return 0
		end
	end
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
	redis.call('HEXPIRE', KEYS[1], ARGV[4], 'FIELDS', 1, ARGV[1])
	return 1
`

const setMemberNotFoundScript = `
	local currentData = redis.call('HGET', KEYS[1], ARGV[1])
	if currentData then
		local ok, current = pcall(cjson.decode, currentData)
		if ok and current and current.found == true then
			return 0
		end
	end
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
	redis.call('HEXPIRE', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
	return 1
`
