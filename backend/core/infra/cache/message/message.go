package message

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	domainmessage "IM_backend/core/domain/message"
)

const (
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
)

type DedupEntry struct {
	SenderID     string `json:"senderId"`
	MessageID    string `json:"messageId"`
	RequestHash  string `json:"requestHash"`
	Status       string `json:"status"`
	SendTime     int64  `json:"sendTime"` // 消息发送时间（毫秒），回填时取 message.SendTime
	Seq          int64  `json:"seq"`
	AttachmentID string `json:"attachmentId,omitempty"`
	StickerID    string `json:"stickerId,omitempty"`
	PackID       string `json:"packId,omitempty"`
	Error        string `json:"error,omitempty"`
}

type MessageCache struct{ client *redis.Client }

func NewMessageCache(client *redis.Client) *MessageCache {
	return &MessageCache{client: client}
}

var getOrCreateDedupScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('HSET', KEYS[1],
        'messageId', '', 'requestHash', ARGV[1], 'status', ARGV[2],
        'sendTime', ARGV[3], 'seq', '0', 'attachmentId', '', 'error', '')
    redis.call('PEXPIRE', KEYS[1], ARGV[4])
    return {
        '1',
        redis.call('HGET', KEYS[1], 'messageId'),
        redis.call('HGET', KEYS[1], 'status'),
        redis.call('HGET', KEYS[1], 'sendTime'),
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
    redis.call('HGET', KEYS[1], 'sendTime'),
    redis.call('HGET', KEYS[1], 'seq'),
    redis.call('HGET', KEYS[1], 'attachmentId'),
    redis.call('HGET', KEYS[1], 'error')
}
`)

// GetOrCreateDedupEntry 原子登记请求；相同指纹复用现有状态，不同指纹视为 clientMsgID 冲突。
func (c *MessageCache) GetOrCreateDedupEntry(ctx context.Context, senderID, clientMsgID string, entry DedupEntry, ttl time.Duration) (DedupEntry, bool, error) {
	values, err := getOrCreateDedupScript.Run(ctx, c.client, []string{MessageDedupKey(senderID, clientMsgID)}, entry.RequestHash, StatusProcessing, time.Now().UnixMilli(), ttl.Milliseconds()).StringSlice()
	if err != nil {
		return DedupEntry{}, false, err
	}
	if len(values) == 0 {
		return DedupEntry{}, false, domainmessage.ErrClientMessageConflict
	}
	sendTime, err := strconv.ParseInt(values[3], 10, 64)
	if err != nil {
		return DedupEntry{}, false, err
	}
	seq, err := strconv.ParseInt(values[4], 10, 64)
	if err != nil {
		return DedupEntry{}, false, err
	}
	entry.MessageID = values[1]
	entry.Status = values[2]
	entry.SendTime = sendTime
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

	sendTime, err := strconv.ParseInt(values["sendTime"], 10, 64)
	if err != nil {
		return DedupEntry{}, false, err
	}
	seq, err := strconv.ParseInt(values["seq"], 10, 64)
	if err != nil {
		return DedupEntry{}, false, err
	}

	return DedupEntry{
		MessageID:    values["messageId"],
		RequestHash:  values["requestHash"],
		Status:       values["status"],
		SendTime:     sendTime,
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
		'sendTime', ARGV[4], 'seq', ARGV[5], 'attachmentId', ARGV[6], 'error', ARGV[7])
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
