package inbox

import (
	"context"
	"time"
)

// InboxRepository 定义消费者对 Inbox 幂等记录的抢占与状态迁移操作。
type InboxRepository interface {
	TryClaim(ctx context.Context, eventID, eventType string, now, staleBefore time.Time) (bool, string, int, error)
	TryClaimBatch(ctx context.Context, events []ClaimEvent, lockToken string, now time.Time) error
	MarkRetry(ctx context.Context, eventID, lockToken, lastError string) error
	MarkCompleted(ctx context.Context, eventID, lockToken string, finishedAt time.Time) error
	MarkDead(ctx context.Context, eventID, lockToken string, lastError string, finishedAt time.Time) error
	RenewBatch(ctx context.Context, eventIDs []string, lockToken string, now time.Time) error
	CompleteBatch(ctx context.Context, eventIDs []string, lockToken string, now time.Time) error
	ReleaseBatch(ctx context.Context, eventIDs []string, lockToken string, undoAttempt bool) error
	WithTx(tx any) InboxRepository
}
