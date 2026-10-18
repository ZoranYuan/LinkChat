package inbox

import (
	inboxport "IM_backend/internal/application/ports/inbox"
	"IM_backend/internal/infrastructure/persistence/mysql/model"
	"context"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrEventInProgress = inboxport.ErrEventInProgress

type InboxRepository struct {
	db *gorm.DB
}

func NewInboxRepository(db *gorm.DB) *InboxRepository {
	return &InboxRepository{db: db}
}

func (r *InboxRepository) WithTx(tx any) inboxport.InboxRepository {
	return &InboxRepository{db: tx.(*gorm.DB)}
}

func (r *InboxRepository) TryClaimBatch(ctx context.Context, events []inboxport.ClaimEvent, lockToken string, now time.Time) error {
	if len(events) == 0 {
		return nil
	}
	rows := make([]model.InboxRecord, 0, len(events))
	for _, event := range events {
		if event.EventID == "" {
			return inboxport.ErrEventIDRequired
		}
		rows = append(rows, model.InboxRecord{EventID: event.EventID, EventType: event.EventType, Status: inboxport.StatusProcessing, LockedAt: &now, LockToken: lockToken, AttemptCount: 1, CreatedAt: now, UpdatedAt: now})
	}
	result := r.db.WithContext(ctx).Create(&rows)
	if result.Error != nil {
		var duplicate *mysql.MySQLError
		if errors.As(result.Error, &duplicate) && duplicate.Number == 1062 {
			return inboxport.ErrBatchConflict
		}
		return result.Error
	}
	if result.RowsAffected != int64(len(rows)) {
		return inboxport.ErrBatchConflict
	}
	return nil
}

func (r *InboxRepository) RenewBatch(ctx context.Context, eventIDs []string, lockToken string, now time.Time) error {
	if len(eventIDs) == 0 {
		return nil
	}
	// 不用更新行数判断续期成功，同一数据库时间精度内重复续期可能影响零行。
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rows []model.InboxRecord
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("event_id").Where("event_id IN ? AND status = ? AND lock_token = ?", eventIDs, inboxport.StatusProcessing, lockToken).Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) != len(eventIDs) {
			return inboxport.ErrLeaseLost
		}
		return tx.Model(&model.InboxRecord{}).Where("event_id IN ? AND status = ? AND lock_token = ?", eventIDs, inboxport.StatusProcessing, lockToken).Update("locked_at", now).Error
	})
}

func (r *InboxRepository) CompleteBatch(ctx context.Context, eventIDs []string, lockToken string, now time.Time) error {
	if len(eventIDs) == 0 {
		return nil
	}
	result := r.db.WithContext(ctx).Model(&model.InboxRecord{}).Where("event_id IN ? AND status = ? AND lock_token = ?", eventIDs, inboxport.StatusProcessing, lockToken).Updates(map[string]any{"status": inboxport.StatusCompleted, "locked_at": nil, "lock_token": "", "finished_at": now})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(eventIDs)) {
		return inboxport.ErrLeaseLost
	}
	return nil
}

func (r *InboxRepository) ReleaseBatch(ctx context.Context, eventIDs []string, lockToken string, undoAttempt bool) error {
	if len(eventIDs) == 0 {
		return nil
	}
	// 尚未执行的事件不算一次业务尝试，释放时撤销预占的 attempt_count。
	updates := map[string]any{"locked_at": nil, "lock_token": ""}
	if undoAttempt {
		updates["attempt_count"] = gorm.Expr("attempt_count - 1")
	}
	return r.db.WithContext(ctx).Model(&model.InboxRecord{}).Where("event_id IN ? AND status = ? AND lock_token = ?", eventIDs, inboxport.StatusProcessing, lockToken).Updates(updates).Error
}

func (r *InboxRepository) TryClaim(ctx context.Context, eventID, eventType string, now, staleBefore time.Time) (bool, string, int, error) {
	if eventID == "" {
		return false, "", 0, inboxport.ErrEventIDRequired
	}

	claimed := false
	lockToken := uuid.NewString()
	result := r.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&model.InboxRecord{
		EventID:      eventID,
		EventType:    eventType,
		Status:       inboxport.StatusProcessing,
		LockedAt:     &now,
		LockToken:    lockToken,
		AttemptCount: 1,
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if result.Error != nil {
		return false, "", 0, result.Error
	}
	if result.RowsAffected == 1 {
		return true, lockToken, 1, nil
	}

	// 非第一次
	var record model.InboxRecord
	if err := r.db.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("event_id = ?", eventID).First(&record).Error; err != nil {
		return false, "", 0, err
	}
	switch record.Status {
	case inboxport.StatusCompleted, inboxport.StatusDead:
		return false, "", record.AttemptCount, nil
	case inboxport.StatusProcessing:
		// 未到过期时间
		if record.LockedAt != nil && record.LockedAt.After(staleBefore) {
			return false, "", record.AttemptCount, inboxport.ErrEventInProgress
		}
	}

	result = r.db.WithContext(ctx).Model(&model.InboxRecord{}).
		Where("event_id = ?", eventID).
		Updates(map[string]any{
			"status":        inboxport.StatusProcessing,
			"locked_at":     &now,
			"lock_token":    lockToken,
			"attempt_count": gorm.Expr("attempt_count + 1"),
			"last_error":    "",
		})
	if result.Error != nil {
		return false, "", 0, result.Error
	}
	claimed = result.RowsAffected == 1
	if !claimed {
		return false, "", record.AttemptCount, inboxport.ErrLeaseLost
	}
	return claimed, lockToken, record.AttemptCount + 1, nil
}

// MarkRetry 释放当前消费者的 Inbox 租约，让当前消息可以在本分区内重试。
// 只有持有当前 lockToken 的消费者才能释放，避免误释放其他消费者的新租约。
func (r *InboxRepository) MarkRetry(ctx context.Context, eventID, lockToken, lastError string) error {
	result := r.db.WithContext(ctx).
		Model(&model.InboxRecord{}).
		Where("event_id = ? AND status = ? AND lock_token = ?", eventID, inboxport.StatusProcessing, lockToken).
		Updates(map[string]any{
			"locked_at":  nil,
			"lock_token": "",
			"last_error": lastError,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return inboxport.ErrLeaseLost
	}
	return nil
}

func (r *InboxRepository) MarkCompleted(ctx context.Context, eventID, lockToken string, finishedAt time.Time) error {
	result := r.db.WithContext(ctx).
		Model(&model.InboxRecord{}).
		Where("event_id = ? AND status = ? AND lock_token = ?", eventID, inboxport.StatusProcessing, lockToken).
		Updates(map[string]any{
			"status":      inboxport.StatusCompleted,
			"locked_at":   nil,
			"lock_token":  "",
			"finished_at": finishedAt,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		var record model.InboxRecord
		if err := r.db.WithContext(ctx).
			Select("status").
			Where("event_id = ?", eventID).
			First(&record).Error; err != nil {
			return err
		}
		// 更新请求可能已经成功，但响应在网络中丢失；重复完成视为幂等成功。
		if record.Status == inboxport.StatusCompleted {
			return nil
		}
		return inboxport.ErrLeaseLost
	}
	return nil
}

func (r *InboxRepository) MarkDead(ctx context.Context, eventID, lockToken, lastError string, finishedAt time.Time) error {
	result := r.db.WithContext(ctx).
		Model(&model.InboxRecord{}).
		Where("event_id = ? AND status = ? AND lock_token = ?", eventID, inboxport.StatusProcessing, lockToken).
		Updates(map[string]any{
			"status":      inboxport.StatusDead,
			"locked_at":   nil,
			"lock_token":  "",
			"last_error":  lastError,
			"finished_at": finishedAt,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		var record model.InboxRecord
		if err := r.db.WithContext(ctx).
			Select("status").
			Where("event_id = ?", eventID).
			First(&record).Error; err != nil {
			return err
		}
		// DLQ 发布和状态更新之间可能发生响应丢失，重复标记 dead 不应再次失败。
		if record.Status == inboxport.StatusDead {
			return nil
		}
		return inboxport.ErrLeaseLost
	}
	return nil
}
