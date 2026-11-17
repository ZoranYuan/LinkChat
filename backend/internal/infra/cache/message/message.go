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
        'messageId', '', 'requestHash', ARGV[1], 'status', ARGV[2],
        'updatedAt', ARGV[3], 'seq', '0', 'attachmentId', '', 'error', '')
    redis.call('PEXPIRE', KEYS[1], ARGV[4])
    return {
        '1',
        redis.call('HGET', KEYS[1], 'messageId'),
        redis.call('HGET', KEYS[1], 'status'),
        redis.call('HGET', KEYS[1], 'updatedAt'),
        redis.call('HGET', KEYS[1], 'seq'),
        redis.call('HGET', KEYS[1], 'attachmentId'),
        redis.call('HGET', KEYS[1], 'error')
    }
elseif redis.call('HGET', KEYS[1], 'requestHash') ~= ARGV[1] then
    return {}
end
return {
    '0',
    redis.call('HGET', KEYS[1], 'messageId'),
    redis.call('HGET', KEYS[1], 'status'),
    redis.call('HGET', KEYS[1], 'updatedAt'),
    redis.call('HGET', KEYS[1], 'seq'),
    redis.call('HGET', KEYS[1], 'attachmentId'),
    redis.call('HGET', KEYS[1], 'error')
}
`)

// GetOrCreateDedupEntry 原子登记请求；相同指纹复用现有状态，不同指纹视为 clientMsgID 冲突。
func (c *MessageCache) GetOrCreateDedupEntry(ctx context.Context, clientMsgID string, entry DedupEntry, ttl time.Duration) (DedupEntry, bool, error) {
	values, err := getOrCreateDedupScript.Run(ctx, c.client, []string{MessageDedupKey(entry.SenderID, clientMsgID)}, entry.RequestHash, StatusProcessing, time.Now().UnixMilli(), ttl.Milliseconds()).StringSlice()
	if err != nil {
		return DedupEntry{}, false, err
	}
	if len(values) == 0 {
		return DedupEntry{}, false, domainmessage.ErrClientMessageConflict
	}
	updatedAt, err := strconv.ParseInt(values[3], 10, 64)
	if err != nil {
		return DedupEntry{}, false, err
	}
	seq, err := strconv.ParseInt(values[4], 10, 64)
	if err != nil {
		return DedupEntry{}, false, err
	}
	entry.MessageID = values[1]
	entry.Status = values[2]
	entry.UpdatedAt = updatedAt
	entry.Seq = seq
	entry.AttachmentID = values[5]
	entry.Error = values[6]
	return entry, values[0] == "1", nil
}

func (c *MessageCache) GetDedupEntry(ctx context.Context, senderID, clientMsgID string) (DedupEntry, bool, error) {
	values, err := c.client.HGetAll(ctx, MessageDedupKey(senderID, clientMsgID)).Result()
	if err != nil {
		return DedupEntry{}, false, err
	}
	if len(values) == 0 {
		return DedupEntry{}, false, nil
	}

	updatedAt, err := strconv.ParseInt(values["updatedAt"], 10, 64)
	if err != nil {
		return DedupEntry{}, false, err
	}
	seq, err := strconv.ParseInt(values["seq"], 10, 64)
	if err != nil {
		return DedupEntry{}, false, err
	}

	return DedupEntry{
		SenderID:     senderID,
		MessageID:    values["messageId"],
		RequestHash:  values["requestHash"],
		Status:       values["status"],
		UpdatedAt:    updatedAt,
		Seq:          seq,
		AttachmentID: values["attachmentId"],
		Error:        values["error"],
	}, true, nil
}

const updateDedupScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
    if redis.call('HGET', KEYS[1], 'requestHash') ~= ARGV[2] then return 0 end
    if redis.call('HGET', KEYS[1], 'status') ~= 'processing' then return 0 end
end
redis.call('HSET', KEYS[1],
    'messageId', ARGV[1], 'requestHash', ARGV[2], 'status', ARGV[3],
    'updatedAt', ARGV[4], 'seq', ARGV[5], 'attachmentId', ARGV[6], 'error', ARGV[7])
redis.call('PEXPIRE', KEYS[1], ARGV[8])
return 1
`

// UpdateDedupEntries 在数据库事务提交后批量回写最终状态；缓存缺失时以数据库结果重建。
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
