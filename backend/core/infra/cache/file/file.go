package file

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	filecache "IM_backend/core/application/ports/cache/file"
	sharedcache "IM_backend/core/infra/cache/shared"
)

type FileCache struct {
	store *sharedcache.Store
}

func NewFileCache(client *redis.Client) filecache.FileCache {
	return &FileCache{store: sharedcache.NewStore(client)}
}

func (c *FileCache) GetAttachmentURLBatch(ctx context.Context, attachmentIDs []string) (map[string]*filecache.AttachmentURL, error) {
	result := make(map[string]*filecache.AttachmentURL, len(attachmentIDs))
	if len(attachmentIDs) == 0 {
		return result, nil
	}

	pipe := c.store.Client().Pipeline()
	commands := make(map[string]*redis.StringCmd, len(attachmentIDs))
	for _, attachmentID := range attachmentIDs {
		if attachmentID == "" {
			continue
		}
		commands[attachmentID] = pipe.Get(ctx, AttachmentURLKey(attachmentID))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	now := time.Now().Unix()
	for attachmentID, command := range commands {
		value, err := command.Bytes()
		if err != nil {
			continue
		}
		var item filecache.AttachmentURL
		if err := json.Unmarshal(value, &item); err != nil || item.MediaURL == "" || item.ExpiresAt <= now {
			continue
		}
		if item.AttachmentID == "" {
			item.AttachmentID = attachmentID
		}
		result[attachmentID] = &item
	}
	return result, nil
}

func (c *FileCache) SetAttachmentURLBatch(ctx context.Context, values []*filecache.AttachmentURL, ttl time.Duration) error {
	if len(values) == 0 || ttl <= 0 {
		return nil
	}

	pipe := c.store.Client().Pipeline()
	for _, value := range values {
		if value == nil || value.AttachmentID == "" || value.MediaURL == "" {
			continue
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		pipe.Set(ctx, AttachmentURLKey(value.AttachmentID), encoded, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}
