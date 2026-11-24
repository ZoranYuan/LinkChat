package group

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"IM_backend/configs"
	groupcache "IM_backend/core/application/ports/cache/group"
	domaingroup "IM_backend/core/domain/group"
	sharedcache "IM_backend/core/infra/cache/shared"
)

var _ groupcache.GroupMemberCache = (*GroupMemberCache)(nil)

type GroupMemberCache struct {
	store  *sharedcache.Store
	config configs.MessageConfig
}

func NewGroupMemberCache(client *redis.Client, config configs.MessageConfig) *GroupMemberCache {
	return &GroupMemberCache{store: sharedcache.NewStore(client), config: config}
}

type groupMemberEntry struct {
	Found     bool   `json:"found"`
	Status    int    `json:"status,omitempty"`
	Role      int    `json:"role,omitempty"`
	MuteUntil *int64 `json:"muteUntil,omitempty"`
	Version   int64  `json:"version,omitempty"`
}

func (c *GroupMemberCache) GetMember(
	ctx context.Context,
	groupID, userID string,
) (*groupcache.MemberState, bool, error) {
	key := GroupMembersKey(groupID)
	data, err := c.store.Client().HGet(ctx, key, userID).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var entry groupMemberEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		_, _ = c.store.Eval(ctx, deleteMemberFieldIfUnchangedScript, []string{key}, userID, string(data))
		return nil, false, fmt.Errorf("解析群聊成员缓存失败：%w", err)
	}
	if !entry.Found {
		return nil, true, nil
	}
	return &groupcache.MemberState{
		Status:    domaingroup.GroupMemberStatus(entry.Status),
		Role:      domaingroup.Role(entry.Role),
		MuteUntil: entry.MuteUntil,
		Version:   entry.Version,
	}, true, nil
}

func (c *GroupMemberCache) SetMemberIfVersionGreater(
	ctx context.Context,
	groupID, userID string,
	state *groupcache.MemberState,
) (bool, error) {
	if state == nil {
		return false, fmt.Errorf("群聊成员状态不能为空")
	}
	if c.config.GroupMemberStateTTLSeconds <= 0 {
		return false, fmt.Errorf("成员状态缓存 TTL 必须大于 0")
	}
	entry := groupMemberEntry{
		Found: true, Status: int(state.Status), Role: int(state.Role),
		MuteUntil: state.MuteUntil, Version: state.Version,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}
	result, err := c.store.Eval(ctx, setMemberIfVersionGreaterScript, []string{GroupMembersKey(groupID)},
		userID, string(data), state.Version, c.config.GroupMemberStateTTLSeconds)
	if err != nil {
		return false, err
	}
	value, ok := result.(int64)
	return ok && value == 1, nil
}

func (c *GroupMemberCache) SetMemberNotFound(ctx context.Context, groupID, userID string) error {
	if c.config.GroupMemberNegativeTTLSeconds <= 0 {
		return fmt.Errorf("成员负缓存 TTL 必须大于 0")
	}
	entry := groupMemberEntry{Found: false}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = c.store.Eval(ctx, setMemberNotFoundScript, []string{GroupMembersKey(groupID)},
		userID, string(data), c.config.GroupMemberNegativeTTLSeconds)
	return err
}

func (c *GroupMemberCache) DeleteMember(ctx context.Context, groupID, userID string) error {
	return c.store.Client().HDel(ctx, GroupMembersKey(groupID), userID).Err()
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
