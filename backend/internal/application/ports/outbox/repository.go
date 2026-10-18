package outbox

import (
	"context"
	"time"
)

// Lease 保存每条已领取记录的租约凭据，批量更新时逐条校验，避免覆盖其他领取者的状态。
type Lease struct {
	EventID   string
	LockToken string
}

type RetryUpdate struct {
	Lease
	NextRetryAt time.Time
	LastError   string
}

type DeadUpdate struct {
	Lease
	LastError string
}

type OutboxRepository interface {
	Create(ctx context.Context, outbox *Entry) error
	CreateBatch(ctx context.Context, outboxes []*Entry) error
	ClaimPending(
		ctx context.Context,
		now time.Time,
		staleBefore time.Time,
		limit int,
	) ([]*Entry, error)
	MarkSentBatch(ctx context.Context, leases []Lease, sentAt time.Time) error
	MarkRetryBatch(ctx context.Context, updates []RetryUpdate) error
	MarkDeadBatch(ctx context.Context, updates []DeadUpdate) error
	WithTx(tx any) OutboxRepository
}
