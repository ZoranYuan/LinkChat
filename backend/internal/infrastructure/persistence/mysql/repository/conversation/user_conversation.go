package conversation

import (
	"context"
	"errors"

	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	"IM_backend/internal/infrastructure/persistence/mysql/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type UserConversationRepository struct {
	db *gorm.DB
}

func NewUserConversationRepository(db *gorm.DB) *UserConversationRepository {
	return &UserConversationRepository{db: db}
}

func (r *UserConversationRepository) upsert(
	ctx context.Context,
	m *model.UserConversation,
	assignments map[string]interface{},
) error {
	return r.db.WithContext(ctx).
		Model(&model.UserConversation{}).
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
	uc *conversationentity.UserConversation,
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
	updates []conversationrepo.ReadSeqUpdate,
) error {
	if len(updates) == 0 {
		return nil
	}

	rows := make([]model.UserConversation, len(updates))
	for i, update := range updates {
		rows[i] = model.UserConversation{
			UserId:         update.UserID,
			ConversationId: update.ConversationID,
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
	uc *conversationentity.UserConversation,
) (bool, error) {
	m := toUserConversationModel(uc)
	result := r.db.WithContext(ctx).
		Model(&model.UserConversation{}).
		Where("user_id = ? AND conversation_id = ? AND last_read_seq < ?", m.UserId, m.ConversationId, m.LastReadSeq).
		Updates(map[string]interface{}{"last_read_seq": m.LastReadSeq})
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected > 0 {
		return true, nil
	}

	var count int64
	if err := r.db.WithContext(ctx).
		Model(&model.UserConversation{}).
		Where("user_id = ? AND conversation_id = ?", m.UserId, m.ConversationId).
		Count(&count).Error; err != nil {
		return false, err
	}
	if count == 0 {
		return false, conversationentity.ErrConversationNotCreated
	}
	return false, nil
}

func (r *UserConversationRepository) CreateUserConversation(
	ctx context.Context,
	uc *conversationentity.UserConversation,
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
		return conversationentity.ErrDuplicateCreation
	}

	return nil
}

func (r *UserConversationRepository) WithTx(tx any) conversationrepo.UserConversationRepository {
	return &UserConversationRepository{
		db: tx.(*gorm.DB),
	}
}

func (r *UserConversationRepository) GetUsersByConversationID(ctx context.Context, conversationID string) ([]string, error) {
	var userIds []string

	err := r.db.
		WithContext(ctx).
		Model(&model.UserConversation{}).
		Where("conversation_id = ?", conversationID).
		Pluck("user_id", &userIds).Error

	return userIds, err
}

func (r *UserConversationRepository) GetUserConversationsByUserId(ctx context.Context, userId string) ([]*conversationentity.UserConversation, error) {
	if userId == "" {
		return nil, errors.New("用户标识不能为空")
	}

	var userConversations []*model.UserConversation

	err := r.db.
		WithContext(ctx).
		Model(&model.UserConversation{}).
		Where("user_id = ?", userId).
		Find(&userConversations).
		Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
	}

	result := make([]*conversationentity.UserConversation, 0, len(userConversations))
	for _, userConversation := range userConversations {
		result = append(result, toUserConversationDomain(userConversation))
	}

	return result, nil
}

func (r *UserConversationRepository) GetUserConversation(
	ctx context.Context,
	userId string,
	conversationId string,
) (*conversationentity.UserConversation, error) {

	var m model.UserConversation

	err := r.db.WithContext(ctx).
		Where("user_id = ? AND conversation_id = ?", userId, conversationId).
		First(&m).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, conversationentity.ErrConversationNotCreated
		}
		return nil, err
	}

	return toUserConversationDomain(&m), nil
}

func (r *UserConversationRepository) CaptureRoomUnreadSnapshot(
	ctx context.Context,
	userID, conversationID string,
	latestSeq int64,
) (*conversationentity.UserConversation, error) {
	if userID == "" || conversationID == "" || latestSeq < 0 {
		return nil, errors.New("房间未读快照参数无效")
	}
	var row model.UserConversation
	err := r.db.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("user_id = ? AND conversation_id = ?", userID, conversationID).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, conversationentity.ErrConversationNotCreated
	}
	if err != nil {
		return nil, err
	}

	beginSeq, endSeq := int64(0), int64(0)
	if latestSeq > row.LastReadSeq {
		beginSeq, endSeq = row.LastReadSeq+1, latestSeq
	}
	if err := r.db.WithContext(ctx).
		Model(&model.UserConversation{}).
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

func (r *UserConversationRepository) ListByUser(
	ctx context.Context,
	userId string,
) ([]*conversationentity.UserConversation, error) {

	var models []*model.UserConversation

	err := r.db.WithContext(ctx).
		Where("user_id = ?", userId).
		Find(&models).Error

	if err != nil {
		return nil, err
	}

	result := make([]*conversationentity.UserConversation, 0, len(models))
	for _, m := range models {
		result = append(result, toUserConversationDomain(m))
	}

	return result, nil
}

func (r *UserConversationRepository) DelUserConversation(ctx context.Context, userId string, conversationId string) error {
	return r.db.WithContext(ctx).
		Where("user_id = ? AND conversation_id =? ", userId, conversationId).
		Delete(&model.UserConversation{}).Error
}
