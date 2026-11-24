package infrainbox

import (
	"context"
	"errors"

	inboxport "IM_backend/core/application/ports/inbox"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

// 由前端回查任务状态
func (r *InboxRepository) FindByKey(ctx context.Context, key inboxport.Key) (*inboxport.StatusRecord, error) {
	var row InboxModel
	err := r.db.WithContext(ctx).
		Select("request_hash").
		Where("event_type = ? AND event_id = ?", key.EventType, key.EventID).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &inboxport.StatusRecord{RequestHash: row.RequestHash}, nil
}

func (r *InboxRepository) FindByEventIDs(
	ctx context.Context, eventType string, eventIDs []string,
) (map[string]string, error) {
	if len(eventIDs) == 0 {
		return map[string]string{}, nil
	}
	var rows []InboxModel
	// 当前读，避免竞态
	if err := r.db.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("event_id, request_hash").
		Where("event_type = ? AND event_id IN ?", eventType, eventIDs).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rows))
	for _, row := range rows {
		out[row.EventID] = row.RequestHash
	}
	return out, nil
}

func (r *InboxRepository) CreateBatch(ctx context.Context, reqs []inboxport.CreateRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	rows := make([]InboxModel, 0, len(reqs))
	for _, q := range reqs {
		rows = append(rows, InboxModel{
			EventType: q.EventType, EventID: q.EventID, RequestHash: q.RequestHash,
		})
	}
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&rows).Error
}
