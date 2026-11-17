package infrainbox

import (
	"context"
	"errors"
	"strings"
	"time"

	inboxport "IM_backend/internal/application/ports/inbox"

	"gorm.io/gorm"
)

type InboxRepository struct {
	db *gorm.DB
}

func NewInboxRepository(db *gorm.DB) *InboxRepository {
	return &InboxRepository{db: db}
}

func (r *InboxRepository) WithTx(tx any) inboxport.InboxRepository {
	return &InboxRepository{db: tx.(*gorm.DB)}
}

func (r *InboxRepository) FindByKey(ctx context.Context, key inboxport.Key) (*inboxport.StatusRecord, error) {
	var row InboxModel
	err := r.db.WithContext(ctx).
		Select("request_hash", "status", "last_error").
		Where("sender_id = ? AND client_msg_id = ?", key.SenderID, key.ClientMsgID).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &inboxport.StatusRecord{
		RequestHash: row.RequestHash,
		Status:      row.Status,
		LastError:   row.LastError,
	}, nil
}

func (r *InboxRepository) ClaimBatch(
	ctx context.Context,
	requests []inboxport.ClaimRequest,
	batchToken string,
	now, staleBefore time.Time,
) ([]inboxport.Key, []inboxport.Key, error) {
	if len(requests) == 0 {
		return nil, nil, nil
	}

	values := make([]string, len(requests))
	args := make([]any, 0, len(requests)*6+2)
	requestsByKey := make(map[inboxport.Key]inboxport.ClaimRequest, len(requests))
	for i, request := range requests {
		values[i] = "(?, ?, ?, ?, ?, ?)"
		args = append(args, request.SenderID, request.ClientMsgID, request.RequestHash,
			inboxport.StatusProcessing, batchToken, now)
		requestsByKey[inboxport.Key{SenderID: request.SenderID, ClientMsgID: request.ClientMsgID}] = request
	}

	canClaim := `inboxes.status = 'processing'
		AND inboxes.request_hash = incoming.request_hash
		AND (inboxes.locked_at IS NULL OR inboxes.locked_at <= ?)`
	args = append(args, staleBefore, staleBefore)
	query := `INSERT INTO inboxes
		(sender_id, client_msg_id, request_hash, status, lock_token, locked_at)
		VALUES ` + strings.Join(values, ",") + ` AS incoming
		ON DUPLICATE KEY UPDATE
		lock_token = IF(` + canClaim + `, incoming.lock_token, inboxes.lock_token),
		locked_at = IF(` + canClaim + `, incoming.locked_at, inboxes.locked_at)`
	if err := r.db.WithContext(ctx).Exec(query, args...).Error; err != nil {
		return nil, nil, err
	}

	pairs := make([][]any, len(requests))
	for i, request := range requests {
		pairs[i] = []any{request.SenderID, request.ClientMsgID}
	}
	var rows []InboxModel

	// 找出插入成功的任务
	if err := r.db.WithContext(ctx).
		Select("sender_id", "client_msg_id", "request_hash", "status", "lock_token").
		Where("(sender_id, client_msg_id) IN ?", pairs).
		Find(&rows).Error; err != nil {
		return nil, nil, err
	}

	claimed := make([]inboxport.Key, 0, len(rows))
	conflicts := make([]inboxport.Key, 0)
	for _, row := range rows {
		key := inboxport.Key{SenderID: row.SenderID, ClientMsgID: row.ClientMsgID}
		request := requestsByKey[key]
		if row.RequestHash != request.RequestHash {
			conflicts = append(conflicts, key)
			continue
		}
		if row.Status != inboxport.StatusProcessing {
			continue
		}
		if row.LockToken != batchToken {
			continue
		}
		claimed = append(claimed, key)
	}
	return claimed, conflicts, nil
}

func (r *InboxRepository) MarkCompletedBatch(ctx context.Context, keys []inboxport.Key, lockToken string) error {
	return r.updateBatch(ctx, keys, lockToken, map[string]any{
		"status":     inboxport.StatusCompleted,
		"lock_token": "",
		"locked_at":  nil,
		"last_error": "",
	})
}

func (r *InboxRepository) MarkDeadBatch(ctx context.Context, keys []inboxport.Key, lockToken, lastError string) error {
	return r.updateBatch(ctx, keys, lockToken, map[string]any{
		"status":     inboxport.StatusDead,
		"lock_token": "",
		"locked_at":  nil,
		"last_error": lastError,
	})
}

func (r *InboxRepository) updateBatch(ctx context.Context, keys []inboxport.Key, lockToken string, updates map[string]any) error {
	pairs := make([][]any, len(keys))
	for i, key := range keys {
		pairs[i] = []any{key.SenderID, key.ClientMsgID}
	}
	if len(pairs) == 0 {
		return nil
	}
	result := r.db.WithContext(ctx).Model(&InboxModel{}).
		Where("(sender_id, client_msg_id) IN ? AND status = ? AND lock_token = ?", pairs, inboxport.StatusProcessing, lockToken).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	return nil
}
