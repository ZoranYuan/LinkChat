package message

import (
	"context"
	"time"

	messagecache "IM_backend/internal/infra/cache/message"
)

type MessageCache interface {
	GetOrCreateDedupEntry(ctx context.Context, clientMsgID string, entry messagecache.DedupEntry, ttl time.Duration) (messagecache.DedupEntry, error)
	UpdateDedupEntries(ctx context.Context, entries map[string]messagecache.DedupEntry, ttl time.Duration) error
}
