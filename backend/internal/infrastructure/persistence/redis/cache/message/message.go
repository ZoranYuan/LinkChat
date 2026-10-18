package message

import (
	messagecache "IM_backend/internal/application/ports/persistence/cache/message"
	"IM_backend/internal/infrastructure/persistence/redis/cache/shared"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ messagecache.MessageCache = (*MessageCache)(nil)

type MessageCache struct {
	store *shared.Store
}

func NewMessageCache(client *redis.Client) *MessageCache {
	return &MessageCache{store: shared.NewStore(client)}
}

func (c *MessageCache) SetDedupEntry(
	ctx context.Context,
	senderID,
	clientMsgID string,
	entry messagecache.DedupEntry,
	ttl time.Duration,
) (bool, error) {
	value, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}
	return c.store.SetNXString(ctx, MessageDedupKey(senderID, clientMsgID), string(value), ttl)
}

// SetDedupEntries 使用一次 Pipeline 批量写入幂等缓存，并保留 SET NX 的防覆盖语义。
func (c *MessageCache) SetDedupEntries(ctx context.Context, entries []messagecache.DedupEntryWrite, ttl time.Duration) error {
	if len(entries) == 0 {
		return nil
	}

	values := make([]string, len(entries))
	for i, item := range entries {
		value, err := json.Marshal(item.Entry)
		if err != nil {
			return err
		}
		values[i] = string(value)
	}

	pipe := c.store.Client().Pipeline()
	for i, item := range entries {
		if item.Key.SenderID == "" || item.Key.ClientMsgID == "" {
			continue
		}
		pipe.SetNX(ctx, MessageDedupKey(item.Key.SenderID, item.Key.ClientMsgID), values[i], ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *MessageCache) GetDedupEntry(ctx context.Context, senderID, clientMsgID string) (messagecache.DedupEntry, error) {
	value, err := c.store.GetString(ctx, MessageDedupKey(senderID, clientMsgID))
	if err != nil || value == "" {
		return messagecache.DedupEntry{}, err
	}

	var entry messagecache.DedupEntry
	if err := json.Unmarshal([]byte(value), &entry); err != nil {
		return messagecache.DedupEntry{}, err
	}
	return entry, nil
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
