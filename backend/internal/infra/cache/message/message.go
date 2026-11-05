package message

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	domainmessage "IM_backend/internal/domain/message"
)

const (
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
)

type DedupEntry struct {
	SenderID     string
	MessageID    string
	RequestHash  string
	Status       string
	UpdatedAt    int64
	Seq          int64
	AttachmentID string
	Error        string
}

type MessageCache struct{ client *redis.Client }

func NewMessageCache(client *redis.Client) *MessageCache {
	return &MessageCache{client: client}
}

var getOrCreateDedupScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('HSET', KEYS[1],
        'messageId', ARGV[1], 'requestHash', ARGV[2], 'status', ARGV[3],
        'updatedAt', ARGV[4], 'seq', '0', 'attachmentId', '', 'error', '')
    redis.call('PEXPIRE', KEYS[1], ARGV[5])
elseif redis.call('HGET', KEYS[1], 'requestHash') ~= ARGV[2] then
    return {}
end
return {
    redis.call('HGET', KEYS[1], 'messageId'),
    redis.call('HGET', KEYS[1], 'status'),
    redis.call('HGET', KEYS[1], 'updatedAt'),
    redis.call('HGET', KEYS[1], 'seq'),
    redis.call('HGET', KEYS[1], 'attachmentId'),
    redis.call('HGET', KEYS[1], 'error')
}
`)

// GetOrCreateDedupEntry 原子登记请求，重复请求复用原消息 ID 和状态。
func (c *MessageCache) GetOrCreateDedupEntry(ctx context.Context, clientMsgID string, entry DedupEntry, ttl time.Duration) (DedupEntry, error) {
	values, err := getOrCreateDedupScript.Run(ctx, c.client, []string{MessageDedupKey(entry.SenderID, clientMsgID)}, entry.MessageID, entry.RequestHash, StatusProcessing, time.Now().UnixMilli(), ttl.Milliseconds()).StringSlice()
	if err != nil {
		return DedupEntry{}, err
	}
	if len(values) == 0 {
		return DedupEntry{}, domainmessage.ErrClientMessageConflict
	}
	updatedAt, err := strconv.ParseInt(values[2], 10, 64)
	if err != nil {
		return DedupEntry{}, err
	}
	seq, err := strconv.ParseInt(values[3], 10, 64)
	if err != nil {
		return DedupEntry{}, err
	}
	entry.MessageID = values[0]
	entry.Status = values[1]
	entry.UpdatedAt = updatedAt
	entry.Seq = seq
	entry.AttachmentID = values[4]
	entry.Error = values[5]
	return entry, nil
}

const updateDedupScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
    if redis.call('HGET', KEYS[1], 'requestHash') ~= ARGV[2] then return 0 end
    if redis.call('HGET', KEYS[1], 'status') ~= 'processing' then return 0 end
    if redis.call('HGET', KEYS[1], 'messageId') ~= ARGV[1] and ARGV[3] ~= 'completed' then return 0 end
end
redis.call('HSET', KEYS[1],
    'messageId', ARGV[1], 'requestHash', ARGV[2], 'status', ARGV[3],
    'updatedAt', ARGV[4], 'seq', ARGV[5], 'attachmentId', ARGV[6], 'error', ARGV[7])
redis.call('PEXPIRE', KEYS[1], ARGV[8])
return 1
`

// UpdateDedupEntries 批量回写终态；数据库已有消息可修正缓存失效后重新生成的候选 ID。
func (c *MessageCache) UpdateDedupEntries(ctx context.Context, entries map[string]DedupEntry, ttl time.Duration) error {
	if len(entries) == 0 {
		return nil
	}
	pipe := c.client.Pipeline()
	now := time.Now().UnixMilli()
	for key, entry := range entries {
		pipe.Eval(ctx, updateDedupScript, []string{key}, entry.MessageID, entry.RequestHash, entry.Status, now, entry.Seq, entry.AttachmentID, entry.Error, ttl.Milliseconds())
	}
	_, err := pipe.Exec(ctx)
	return err
}
