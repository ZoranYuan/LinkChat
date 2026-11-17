package inbox

import (
	"context"
	"errors"
	"time"
)

const (
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusDead       = "dead"
)

var ErrClaimBusy = errors.New("Inbox 认领租约仍有效")

type ClaimRequest struct {
	SenderID    string
	ClientMsgID string
	RequestHash string
}

type Key struct {
	SenderID    string
	ClientMsgID string
}

type StatusRecord struct {
	RequestHash string
	Status      string
	LastError   string
}

type InboxRepository interface {
	FindByKey(ctx context.Context, key Key) (*StatusRecord, error)
	ClaimBatch(ctx context.Context, requests []ClaimRequest, batchToken string, now, staleBefore time.Time) ([]Key, []Key, error)
	MarkCompletedBatch(ctx context.Context, keys []Key, lockToken string) error
	MarkDeadBatch(ctx context.Context, keys []Key, lockToken, lastError string) error
	WithTx(tx any) InboxRepository
}
