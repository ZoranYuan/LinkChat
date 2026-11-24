package group

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	groupcache "IM_backend/core/application/ports/cache/group"
	domaingroup "IM_backend/core/domain/group"
	keycache "IM_backend/core/infra/cache/key"
	sharedcache "IM_backend/core/infra/cache/shared"
)

var _ groupcache.GroupInfoCache = (*GroupInfoCache)(nil)

type GroupInfoCache struct {
	store *sharedcache.Store
}

func NewGroupInfoCache(client *redis.Client) *GroupInfoCache {
	return &GroupInfoCache{store: sharedcache.NewStore(client)}
}

func (c *GroupInfoCache) GetGroupInfo(ctx context.Context, groupID string) (*domaingroup.Group, bool, error) {
	if groupID == "" {
		return nil, false, errors.New("群聊 ID 不能为空")
	}
	if c == nil || c.store == nil || c.store.Client() == nil {
		return nil, false, errors.New("群聊基础信息缓存未配置")
	}
	var group domaingroup.Group
	found, err := c.store.GetJSON(ctx, GroupInfoKey(groupID), &group)
	if err != nil || !found {
		return nil, found, err
	}
	if group.GroupID == "" || group.GroupID != groupID {
		return nil, false, errors.New("群聊基础信息缓存中的群聊 ID 无效")
	}
	return &group, true, nil
}

func (c *GroupInfoCache) GetGroupInfos(ctx context.Context, groupIDs []string) (map[string]*domaingroup.Group, error) {
	groups := make(map[string]*domaingroup.Group, len(groupIDs))
	if len(groupIDs) == 0 {
		return groups, nil
	}
	keys := make([]string, len(groupIDs))
	for i, groupID := range groupIDs {
		keys[i] = GroupInfoKey(groupID)
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
		var group domaingroup.Group
		if err := json.Unmarshal([]byte(data), &group); err != nil {
			return nil, err
		}
		if group.GroupID == groupIDs[i] {
			groups[group.GroupID] = &group
		}
	}
	return groups, nil
}

func (c *GroupInfoCache) SetGroupInfo(ctx context.Context, group *domaingroup.Group, ttl time.Duration) error {
	if group == nil || group.GroupID == "" {
		return errors.New("群聊基础信息不能为空")
	}
	if ttl <= 0 {
		return errors.New("群聊基础信息缓存 TTL 必须大于 0")
	}
	if c == nil || c.store == nil || c.store.Client() == nil {
		return errors.New("群聊基础信息缓存未配置")
	}
	return c.store.SetJSON(ctx, GroupInfoKey(group.GroupID), group, ttl)
}

func (c *GroupInfoCache) SetGroupInfos(ctx context.Context, groups []*domaingroup.Group, ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("群聊基础信息缓存 TTL 必须大于 0")
	}
	items := make(map[string][]byte, len(groups))
	for _, group := range groups {
		if group == nil || group.GroupID == "" {
			continue
		}
		data, err := json.Marshal(group)
		if err != nil {
			return err
		}
		items[GroupInfoKey(group.GroupID)] = data
	}
	_, err := c.store.Client().Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for key, data := range items {
			pipe.Set(ctx, key, data, ttl)
		}
		return nil
	})
	return err
}

func (c *GroupInfoCache) DeleteGroupInfo(ctx context.Context, groupID string) error {
	if groupID == "" {
		return errors.New("群聊 ID 不能为空")
	}
	if c == nil || c.store == nil || c.store.Client() == nil {
		return errors.New("群聊基础信息缓存未配置")
	}
	return c.store.Del(ctx, GroupInfoKey(groupID))
}

func GroupInfoKey(groupID string) string {
	return keycache.GroupInfo(groupID)
}
