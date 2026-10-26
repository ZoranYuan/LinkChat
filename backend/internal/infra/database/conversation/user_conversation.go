package infraconversation

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	domainconversation "IM_backend/internal/domain/conversation"
)

type UserConversationRepository struct {
	db *gorm.DB
}

func NewUserConversationRepository(db *gorm.DB) *UserConversationRepository {
	return &UserConversationRepository{db: db}
}

func (r *UserConversationRepository) upsert(
	ctx context.Context,
	m *UserConversationModel,
	assignments map[string]interface{},
) error {
	return r.db.WithContext(ctx).
		Model(&UserConversationModel{}).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "user_id"},
				{Name: "conversation_id"},
			},
			DoUpdates: clause.Assignments(assignments),
		}).
		Create(m).Error
}

func (r *UserConversationRepository) UpdateReadSeq(
	ctx context.Context,
	uc *domainconversation.UserConversation,
) error {
	m := toUserConversationModel(uc)

	return r.upsert(ctx, m, map[string]interface{}{
		"last_read_seq": gorm.Expr(
			"GREATEST(last_read_seq, VALUES(last_read_seq))",
		),
	})
}

// UpdateReadSeqBatch 一次推进本批发送者的已读水位，重复记录通过 GREATEST 保留最大值。
func (r *UserConversationRepository) UpdateReadSeqBatch(
	ctx context.Context,
	updates []domainconversation.ReadSeqUpdate,
) error {
	if len(updates) == 0 {
		return nil
	}

	rows := make([]UserConversationModel, len(updates))
	for i, update := range updates {
		rows[i] = UserConversationModel{
			UserID:         update.UserID,
			ConversationID: update.ConversationID,
			LastReadSeq:    update.LastReadSeq,
		}
	}

	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "user_id"},
				{Name: "conversation_id"},
			},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"last_read_seq": gorm.Expr("GREATEST(last_read_seq, VALUES(last_read_seq))"),
			}),
		}).
		Create(&rows).Error
}

// AdvanceReadSeq atomically advances the read cursor and reports whether the
// stored value actually moved forward. The result controls read notifications.
func (r *UserConversationRepository) AdvanceReadSeq(
	ctx context.Context,
	uc *domainconversation.UserConversation,
) (bool, error) {
	m := toUserConversationModel(uc)
	result := r.db.WithContext(ctx).
		Model(&UserConversationModel{}).
		Where("user_id = ? AND conversation_id = ? AND last_read_seq < ?", m.UserID, m.ConversationID, m.LastReadSeq).
		Updates(map[string]interface{}{"last_read_seq": m.LastReadSeq})
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected > 0 {
		return true, nil
	}

	var count int64
	if err := r.db.WithContext(ctx).
		Model(&UserConversationModel{}).
		Where("user_id = ? AND conversation_id = ?", m.UserID, m.ConversationID).
		Count(&count).Error; err != nil {
		return false, err
	}
	if count == 0 {
		return false, domainconversation.ErrConversationNotCreated
	}
	return false, nil
}

func (r *UserConversationRepository) CreateUserConversation(
	ctx context.Context,
	uc *domainconversation.UserConversation,
) error {
	m := toUserConversationModel(uc)

	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "user_id"}, {Name: "conversation_id"}},
			DoNothing: true,
		}).
		Create(&m)

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return domainconversation.ErrDuplicateCreation
	}

	return nil
}

func (r *UserConversationRepository) WithTx(tx any) domainconversation.UserConversationRepository {
	return &UserConversationRepository{
		db: tx.(*gorm.DB),
	}
}

func (r *UserConversationRepository) FindUserConversationByUserIDAndConversationID(
	ctx context.Context,
	userID string,
	conversationID string,
) (*domainconversation.UserConversation, error) {

	var m UserConversationModel

	err := r.db.WithContext(ctx).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		First(&m).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domainconversation.ErrConversationNotCreated
		}
		return nil, err
	}

	return toUserConversationDomain(&m), nil
}

func (r *UserConversationRepository) CaptureRoomUnreadSnapshot(
	ctx context.Context,
	userID, conversationID string,
	latestSeq int64,
) (*domainconversation.UserConversation, error) {
	if userID == "" || conversationID == "" || latestSeq < 0 {
		return nil, errors.New("房间未读快照参数无效")
	}
	var row UserConversationModel
	err := r.db.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domainconversation.ErrConversationNotCreated
	}
	if err != nil {
		return nil, err
	}

	beginSeq, endSeq := int64(0), int64(0)
	if latestSeq > row.LastReadSeq {
		beginSeq, endSeq = row.LastReadSeq+1, latestSeq
	}
	if err := r.db.WithContext(ctx).
		Model(&UserConversationModel{}).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		Updates(map[string]any{
			"last_read_seq":            max(row.LastReadSeq, latestSeq),
			"summary_unread_begin_seq": beginSeq,
			"summary_unread_end_seq":   endSeq,
		}).Error; err != nil {
		return nil, err
	}
	row.LastReadSeq = max(row.LastReadSeq, latestSeq)
	row.SummaryUnreadBeginSeq = beginSeq
	row.SummaryUnreadEndSeq = endSeq
	return toUserConversationDomain(&row), nil
}

func (r *UserConversationRepository) ListUserConversationsByUserID(
	ctx context.Context,
	userID string,
) ([]*domainconversation.UserConversation, error) {

	var models []*UserConversationModel

	err := r.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Find(&models).Error

	if err != nil {
		return nil, err
	}

	result := make([]*domainconversation.UserConversation, 0, len(models))
	for _, m := range models {
		result = append(result, toUserConversationDomain(m))
	}

	return result, nil
}

func (r *UserConversationRepository) DelUserConversation(ctx context.Context, userID string, conversationID string) error {
	return r.db.WithContext(ctx).
		Where("user_id = ? AND conversation_id =? ", userID, conversationID).
		Delete(&UserConversationModel{}).Error
}
