package outbox

import (
	"context"
	"strings"
	"time"

	idport "IM_backend/internal/application/ports/id"
	outboxport "IM_backend/internal/application/ports/outbox"
	"IM_backend/internal/infrastructure/persistence/mysql/model"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OutboxRepository struct {
	db          *gorm.DB
	idGenerator idport.Generator
}

func NewOutboxRepository(db *gorm.DB, idGenerator idport.Generator) *OutboxRepository {
	return &OutboxRepository{db: db, idGenerator: idGenerator}
}

func (r *OutboxRepository) WithTx(tx any) outboxport.OutboxRepository {
	return &OutboxRepository{db: tx.(*gorm.DB), idGenerator: r.idGenerator}
}

func toModel(e *outboxport.Entry) *model.OutboxRecord {
	if e == nil {
		return nil
	}

	var lockedAt *time.Time
	if e.LockedAt != nil {
		t := *e.LockedAt
		lockedAt = &t
	}

	var sentAt *time.Time
	if e.SentAt != nil {
		t := *e.SentAt
		sentAt = &t
	}

	return &model.OutboxRecord{
		EventID:      e.EventID,
		EventType:    e.EventType,
		PartitionKey: e.PartitionKey,
		Payload:      e.Payload,
		Status:       e.Status,
		RetryCount:   e.RetryCount,
		NextRetryAt:  e.NextRetryAt,
		LockedAt:     lockedAt,
		LockToken:    e.LockToken,
		LastError:    e.LastError,
		SentAt:       sentAt,
	}
}

func toEntry(m *model.OutboxRecord) *outboxport.Entry {
	if m == nil {
		return nil
	}

	var lockedAt *time.Time
	if m.LockedAt != nil {
		t := *m.LockedAt
		lockedAt = &t
	}

	var sentAt *time.Time
	if m.SentAt != nil {
		t := *m.SentAt
		sentAt = &t
	}

	return &outboxport.Entry{
		EventID:      m.EventID,
		EventType:    m.EventType,
		PartitionKey: m.PartitionKey,
		Payload:      m.Payload,
		Status:       m.Status,
		RetryCount:   m.RetryCount,
		NextRetryAt:  m.NextRetryAt,
		LockedAt:     lockedAt,
		LockToken:    m.LockToken,
		LastError:    m.LastError,
		SentAt:       sentAt,
		CreatedAt:    m.CreatedAt,
		UpdatedAt:    m.UpdatedAt,
	}
}

func (r *OutboxRepository) Create(ctx context.Context, outbox *outboxport.Entry) error {
	if outbox == nil {
		return nil
	}
	if outbox.EventID == "" {
		id, err := r.idGenerator.Generate()
		if err != nil {
			return err
		}
		outbox.EventID = id
	}
	if outbox.Status == "" {
		outbox.Status = outboxport.StatusPending
	}
	if outbox.NextRetryAt.IsZero() {
		outbox.NextRetryAt = time.Now()
	}
	m := toModel(outbox)
	return r.db.WithContext(ctx).Create(m).Error
}

func (r *OutboxRepository) CreateBatch(ctx context.Context, outboxes []*outboxport.Entry) error {
	if len(outboxes) == 0 {
		return nil
	}
	now := time.Now()
	rows := make([]*model.OutboxRecord, 0, len(outboxes))
	for _, entry := range outboxes {
		if entry == nil {
			continue
		}
		if entry.EventID == "" {
			id, err := r.idGenerator.Generate()
			if err != nil {
				return err
			}
			entry.EventID = id
		}
		if entry.Status == "" {
			entry.Status = outboxport.StatusPending
		}
		if entry.NextRetryAt.IsZero() {
			entry.NextRetryAt = now
		}
		rows = append(rows, toModel(entry))
	}
	if len(rows) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Create(&rows).Error
}

func (r *OutboxRepository) ClaimPending(
	ctx context.Context,
	now time.Time,
	staleBefore time.Time,
	limit int,
) ([]*outboxport.Entry, error) {
	if limit <= 0 {
		return []*outboxport.Entry{}, nil
	}
	var models []*model.OutboxRecord
	err := r.db.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Where(
			"(status = ? AND next_retry_at <= ?) OR (status = ? AND locked_at IS NOT NULL AND locked_at <= ?)",
			outboxport.StatusPending,
			now,
			outboxport.StatusProcessing,
			staleBefore,
		).
		Order("next_retry_at ASC, created_at ASC").
		Limit(limit).
		Find(&models).Error
	if err != nil {
		return nil, err
	}
	if len(models) == 0 {
		return []*outboxport.Entry{}, nil
	}
	ids := make([]string, 0, len(models))
	for _, m := range models {
		ids = append(ids, m.EventID)
	}
	lockAt := now
	lockToken := uuid.NewString()
	if err := r.db.WithContext(ctx).
		Model(&model.OutboxRecord{}).
		Where("event_id IN ?", ids).
		Updates(map[string]interface{}{
			"status":     outboxport.StatusProcessing,
			"locked_at":  &lockAt,
			"lock_token": lockToken,
			"last_error": "",
		}).Error; err != nil {
		return nil, err
	}
	result := make([]*outboxport.Entry, 0, len(models))
	for _, m := range models {
		m.Status = outboxport.StatusProcessing
		m.LockedAt = &lockAt
		m.LockToken = lockToken
		m.LastError = ""
		result = append(result, toEntry(m))
	}
	return result, nil
}

// MarkSentBatch 使用每条记录的 eventID + lockToken，一条 SQL 确认成功事件。
func (r *OutboxRepository) MarkSentBatch(ctx context.Context, leases []outboxport.Lease, sentAt time.Time) error {
	return r.updateLeasedBatch(ctx, leases, map[string]interface{}{
		"status": outboxport.StatusSent, "sent_at": sentAt, "locked_at": nil, "lock_token": "",
	})
}

// MarkRetryBatch 一条 SQL 更新重试状态，保留每条记录的错误和下次重试时间。
func (r *OutboxRepository) MarkRetryBatch(ctx context.Context, updates []outboxport.RetryUpdate) error {
	leases := make([]outboxport.Lease, 0, len(updates))
	errorArgs := make([]interface{}, 0, len(updates)*2)
	retryArgs := make([]interface{}, 0, len(updates)*2)
	for _, update := range updates {
		leases = append(leases, update.Lease)
		errorArgs = append(errorArgs, update.EventID, update.LastError)
		retryArgs = append(retryArgs, update.EventID, update.NextRetryAt)
	}
	return r.updateLeasedBatch(ctx, leases, map[string]interface{}{
		"status": outboxport.StatusPending, "locked_at": nil, "lock_token": "",
		"retry_count":   gorm.Expr("retry_count + 1"),
		"last_error":    gorm.Expr("CASE event_id "+strings.Repeat("WHEN ? THEN ? ", len(updates))+"ELSE last_error END", errorArgs...),
		"next_retry_at": gorm.Expr("CASE event_id "+strings.Repeat("WHEN ? THEN ? ", len(updates))+"ELSE next_retry_at END", retryArgs...),
	})
}

// MarkDeadBatch 一条 SQL 标记达到重试上限的记录，保留各自的错误原因。
func (r *OutboxRepository) MarkDeadBatch(ctx context.Context, updates []outboxport.DeadUpdate) error {
	leases := make([]outboxport.Lease, 0, len(updates))
	errorArgs := make([]interface{}, 0, len(updates)*2)
	for _, update := range updates {
		leases = append(leases, update.Lease)
		errorArgs = append(errorArgs, update.EventID, update.LastError)
	}
	return r.updateLeasedBatch(ctx, leases, map[string]interface{}{
		"status": outboxport.StatusDead, "locked_at": nil, "lock_token": "",
		"last_error": gorm.Expr("CASE event_id "+strings.Repeat("WHEN ? THEN ? ", len(updates))+"ELSE last_error END", errorArgs...),
	})
}

func (r *OutboxRepository) updateLeasedBatch(ctx context.Context, leases []outboxport.Lease, assignments map[string]interface{}) error {
	if len(leases) == 0 {
		return nil
	}
	pairs := make([][]interface{}, 0, len(leases))
	seen := make(map[string]string, len(leases))
	for _, lease := range leases {
		if lease.EventID == "" || lease.LockToken == "" {
			return outboxport.ErrLeaseLost
		}
		if token, exists := seen[lease.EventID]; exists {
			if token != lease.LockToken {
				return outboxport.ErrLeaseLost
			}
			continue
		}
		seen[lease.EventID] = lease.LockToken
		pairs = append(pairs, []interface{}{lease.EventID, lease.LockToken})
	}
	result := r.db.WithContext(ctx).Model(&model.OutboxRecord{}).
		Where("status = ? AND (event_id, lock_token) IN ?", outboxport.StatusProcessing, pairs).
		Updates(assignments)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(pairs)) {
		return outboxport.ErrLeaseLost
	}
	return nil
}
