package inbox

import (
	"context"
)

type CreateRequest struct {
	EventType   string
	EventID     string
	RequestHash string
}

type Key struct {
	EventType string
	EventID   string
}

type StatusRecord struct {
	RequestHash string
}

type InboxRepository interface {
	FindByKey(ctx context.Context, key Key) (*StatusRecord, error)
	FindByEventIDs(ctx context.Context, eventType string, eventIDs []string) (map[string]string, error)
	CreateBatch(ctx context.Context, requests []CreateRequest) error
	WithTx(tx any) InboxRepository
}
