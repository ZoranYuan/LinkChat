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

var ErrLeaseLost = errors.New("inbox lease lost")
var ErrEventInProgress = errors.New("事件正在处理中")
var ErrEventIDRequired = errors.New("Inbox event_id 不能为空")

// InboxRepository 定义消费者对 Inbox 幂等记录的抢占与状态迁移操作。
type InboxRepository interface {
	TryClaim(ctx context.Context, eventID, eventType string, now, staleBefore time.Time) (bool, string, int, error)
	MarkRetry(ctx context.Context, eventID, lockToken, lastError string) error
	MarkCompleted(ctx context.Context, eventID, lockToken string, finishedAt time.Time) error
	MarkDead(ctx context.Context, eventID, lockToken string, lastError string, finishedAt time.Time) error
	RenewBatch(ctx context.Context, eventIDs []string, lockToken string, now time.Time) error
	WithTx(tx any) InboxRepository
}
