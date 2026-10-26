package message

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	messagecache "IM_backend/internal/application/ports/cache/message"
	sharedcache "IM_backend/internal/infra/cache/shared"
)

var _ messagecache.MessageCache = (*MessageCache)(nil)

type MessageCache struct {
	store *sharedcache.Store
}

func NewMessageCache(client *redis.Client) *MessageCache {
	return &MessageCache{store: sharedcache.NewStore(client)}
}

// SetDedupEntries 使用一次 Pipeline 写入幂等结果，SET NX 避免覆盖已保存的结果。
func (c *MessageCache) SetDedupEntries(ctx context.Context, entries []messagecache.DedupEntryWrite, ttl time.Duration) error {
	if len(entries) == 0 {
		return nil
	}
	pipe := c.store.Client().Pipeline()
	for _, item := range entries {
		if item.Key.SenderID == "" || item.Key.ClientMsgID == "" {
			continue
		}
		value, err := json.Marshal(item.Entry)
		if err != nil {
			return err
		}
		pipe.SetNX(ctx, MessageDedupKey(item.Key.SenderID, item.Key.ClientMsgID), value, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// GetDedupEntries 合并独立 GET 的网络往返，同一发送标识只查询一次；未命中或无法解析的记录按缓存未命中处理。
func (c *MessageCache) GetDedupEntries(ctx context.Context, keys []messagecache.DedupKey) (map[messagecache.DedupKey]messagecache.DedupEntry, error) {
	entries := make(map[messagecache.DedupKey]messagecache.DedupEntry, len(keys))
	if len(keys) == 0 {
		return entries, nil
	}

	pipe := c.store.Client().Pipeline()
	commands := make(map[messagecache.DedupKey]*redis.StringCmd, len(keys))
	for _, key := range keys {
		if key.SenderID == "" || key.ClientMsgID == "" {
			continue
		}
		if _, exists := commands[key]; exists {
			continue
		}
		commands[key] = pipe.Get(ctx, MessageDedupKey(key.SenderID, key.ClientMsgID))
	}
	if len(commands) == 0 {
		return entries, nil
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for key, command := range commands {
		value, err := command.Bytes()
		if err != nil || len(value) == 0 {
			continue
		}
		var entry messagecache.DedupEntry
		if err := json.Unmarshal(value, &entry); err != nil {
			continue
		}
		entries[key] = entry
	}
	return entries, nil
}
