package send

import (
	"context"
	"encoding/json"
	"time"

	"IM_backend/configs"
	messagecacheport "IM_backend/core/application/ports/cache/message"
	messagecache "IM_backend/core/infra/cache/message"
	"IM_backend/core/infra/mq/kafka"
)

type MessageRedisFillHandler struct {
	messageCache messagecacheport.MessageCache
	config       configs.Config
}

func NewMessageRedisFillHandler(messageCache messagecacheport.MessageCache, config configs.Config) *MessageRedisFillHandler {
	return &MessageRedisFillHandler{messageCache: messageCache, config: config}
}

func (h *MessageRedisFillHandler) Topic() string { return h.config.Kafka.Topics.MessageSendCache }

func (h *MessageRedisFillHandler) Handle(ctx context.Context, item *kafka.MessageItem) error {
	var entry messagecache.DedupEntry
	if err := json.Unmarshal(item.Record.Value, &entry); err != nil {
		item.Complete()
		return nil
	}
	clientMsgID := string(item.Record.Key)
	ttl := time.Duration(h.config.Cache.MessageDedup.TTLSeconds) * time.Second
	err := h.messageCache.UpdateDedupEntries(ctx, map[string]messagecache.DedupEntry{
		messagecache.MessageDedupKey(entry.SenderID, clientMsgID): entry,
	}, ttl)
	if err != nil {
		return err
	}
	item.Complete()
	return nil
}
