package message

import (
	"context"
	"time"

	messagecache "IM_backend/core/infra/cache/message"
)

type MessageCache interface {
	GetOrCreateDedupEntry(ctx context.Context, senderID, clientMsgID string, entry messagecache.DedupEntry, ttl time.Duration) (messagecache.DedupEntry, bool, error)
	GetDedupEntry(ctx context.Context, senderID, clientMsgID string) (messagecache.DedupEntry, bool, error)
	UpdateDedupEntries(ctx context.Context, entries map[string]messagecache.DedupEntry, ttl time.Duration) error
}
