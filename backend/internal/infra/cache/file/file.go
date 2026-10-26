package file

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	filecache "IM_backend/internal/application/ports/cache/file"
	domainfile "IM_backend/internal/domain/file"
	sharedcache "IM_backend/internal/infra/cache/shared"
)

type FileCache struct {
	store *sharedcache.Store
}

func NewFileCache(client *redis.Client) filecache.FileCache {
	return &FileCache{store: sharedcache.NewStore(client)}
}

func (c *FileCache) SetFileMetadata(ctx context.Context, file *domainfile.File, ttl time.Duration) error {
	if file == nil || file.FileID == "" {
		return nil
	}
	return c.store.SetJSON(ctx, FileMetadataKey(file.FileID), file, ttl)
}

func (c *FileCache) GetFileMetadata(ctx context.Context, fileID string) (*domainfile.File, error) {
	var file domainfile.File
	ok, err := c.store.GetJSON(ctx, FileMetadataKey(fileID), &file)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return &file, nil
}

func (c *FileCache) SetFileByUploaderAndHash(
	ctx context.Context,
	uploaderID string,
	fileHash string,
	file *domainfile.File,
	ttl time.Duration,
) error {
	if uploaderID == "" || fileHash == "" || file == nil || file.FileID == "" {
		return nil
	}
	return c.store.SetJSON(ctx, FileByUploaderAndHashKey(uploaderID, fileHash), file, ttl)
}

func (c *FileCache) GetFileByUploaderAndHash(
	ctx context.Context,
	uploaderID string,
	fileHash string,
) (*domainfile.File, error) {
	if uploaderID == "" || fileHash == "" {
		return nil, nil
	}

	var file domainfile.File
	ok, err := c.store.GetJSON(ctx, FileByUploaderAndHashKey(uploaderID, fileHash), &file)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return &file, nil
}

func (c *FileCache) DeleteFileByUploaderAndHash(ctx context.Context, uploaderID string, fileHash string) error {
	if uploaderID == "" || fileHash == "" {
		return nil
	}
	return c.store.Del(ctx, FileByUploaderAndHashKey(uploaderID, fileHash))
}

func (c *FileCache) GetAttachmentFileCardBatch(ctx context.Context, attachmentIDs []string) (map[string]*filecache.AttachmentFileCard, error) {
	result := make(map[string]*filecache.AttachmentFileCard, len(attachmentIDs))
	if len(attachmentIDs) == 0 {
		return result, nil
	}

	pipe := c.store.Client().Pipeline()
	commands := make(map[string]*redis.StringCmd, len(attachmentIDs))
	for _, attachmentID := range attachmentIDs {
		if attachmentID == "" {
			continue
		}
		commands[attachmentID] = pipe.Get(ctx, AttachmentFileCardKey(attachmentID))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	for attachmentID, command := range commands {
		value, err := command.Bytes()
		if err != nil {
			continue
		}
		var card filecache.AttachmentFileCard
		if err := json.Unmarshal(value, &card); err != nil {
			continue
		}
		if card.AttachmentID == "" {
			card.AttachmentID = attachmentID
		}
		result[attachmentID] = &card
	}
	return result, nil
}

func (c *FileCache) SetAttachmentFileCardBatch(ctx context.Context, values []*filecache.AttachmentFileCard, ttl time.Duration) error {
	if len(values) == 0 || ttl <= 0 {
		return nil
	}

	pipe := c.store.Client().Pipeline()
	for _, value := range values {
		if value == nil || value.AttachmentID == "" || value.FileID == "" {
			continue
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		pipe.Set(ctx, AttachmentFileCardKey(value.AttachmentID), encoded, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
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

func (c *FileCache) DeleteFileMetadata(ctx context.Context, fileID string) error {
	return c.store.Del(ctx, FileMetadataKey(fileID))
}

func (c *FileCache) RenewFileCompleteLock(ctx context.Context, uploadID string, token string, ttl time.Duration) (bool, error) {
	luaScript := `
		local token = ARGV[1]
		local ttl = ARGV[2]

		if redis.call("GET", KEYS[1]) == token then
			return redis.call("PEXPIRE", KEYS[1], ttl)
		end
		return 0
	`

	result, err := c.store.Eval(
		ctx,
		luaScript,
		[]string{FileCompleteLockKey(uploadID)},
		token,
		ttl.Milliseconds(),
	)
	if err != nil {
		return false, err
	}
	renewed, ok := result.(int64)
	return ok && renewed == 1, nil
}

func (c *FileCache) SetUploadMeta(ctx context.Context, meta filecache.UploadMeta, ttl time.Duration) error {
	return c.store.SetJSON(ctx, UploadMetaKey(meta.UploadID), meta, ttl)
}

func (c *FileCache) GetUploadMeta(ctx context.Context, uploadID string) (*filecache.UploadMeta, error) {
	var meta filecache.UploadMeta
	ok, err := c.store.GetJSON(ctx, UploadMetaKey(uploadID), &meta)
	if err != nil || !ok {
		return nil, err
	}
	return &meta, nil
}

func (c *FileCache) AcquireFileInitLock(ctx context.Context, uploaderID string, fileHash string, ttl time.Duration) (string, bool, error) {
	token, err := newLockToken()
	if err != nil {
		return "", false, err
	}
	locked, err := c.store.SetNXString(ctx, FileInitLockKey(uploaderID, fileHash), token, ttl)
	return token, locked, err
}

func (c *FileCache) ReleaseFileInitLock(ctx context.Context, uploaderID string, fileHash string, token string) error {
	return c.releaseLock(ctx, FileInitLockKey(uploaderID, fileHash), token)
}

func (c *FileCache) AcquireFileCompleteLock(ctx context.Context, uploadID string, ttl time.Duration) (string, bool, error) {
	token, err := newLockToken()
	if err != nil {
		return "", false, err
	}
	locked, err := c.store.SetNXString(ctx, FileCompleteLockKey(uploadID), token, ttl)
	return token, locked, err
}

func (c *FileCache) ReleaseFileCompleteLock(ctx context.Context, uploadID string, token string) error {
	return c.releaseLock(ctx, FileCompleteLockKey(uploadID), token)
}

func (c *FileCache) releaseLock(ctx context.Context, key string, token string) error {
	const script = `
		local value = redis.call("GET", KEYS[1])
		if value == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
			return 0
	`
	_, err := c.store.Client().Eval(ctx, script, []string{key}, token).Result()
	return err
}

func newLockToken() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

func (c *FileCache) DeleteUploadMeta(ctx context.Context, uploadID string) error {
	return c.store.Del(ctx, UploadMetaKey(uploadID))
}

func (c *FileCache) SetActiveFileUpload(ctx context.Context, uploaderID string, fileHash string, uploadID string, ttl time.Duration) error {
	if uploaderID == "" || fileHash == "" || uploadID == "" {
		return nil
	}
	return c.store.SetString(ctx, ActiveFileUploadKey(uploaderID, fileHash), uploadID, ttl)
}

func (c *FileCache) GetActiveFileUploadID(ctx context.Context, uploaderID string, fileHash string) (string, error) {
	if fileHash == "" {
		return "", nil
	}
	return c.store.GetString(ctx, ActiveFileUploadKey(uploaderID, fileHash))
}

func (c *FileCache) DeleteActiveFileUploadIfMatches(ctx context.Context, uploaderID string, fileHash string, uploadID string) error {
	if fileHash == "" || uploadID == "" {
		return nil
	}

	const script = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0
	`
	_, err := c.store.Eval(ctx, script, []string{ActiveFileUploadKey(uploaderID, fileHash)}, uploadID)
	return err
}
