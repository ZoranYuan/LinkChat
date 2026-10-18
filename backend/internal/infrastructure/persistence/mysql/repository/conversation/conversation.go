package conversation

import (
	"context"
	"errors"

	conversationrepo "IM_backend/internal/application/ports/persistence/repository/conversation"
	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
	"IM_backend/internal/infrastructure/persistence/mysql/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ConversationRepository struct {
	db *gorm.DB
}

func NewConversationRepository(db *gorm.DB) *ConversationRepository {
	return &ConversationRepository{db: db}
}

func (r *ConversationRepository) WithTx(tx any) conversationrepo.ConversationRepository {
	return &ConversationRepository{
		db: tx.(*gorm.DB),
	}
}

// 创建会话
func (r *ConversationRepository) CreateConversation(ctx context.Context, conv *conversationentity.Conversation) error {
	m := toConversationModel(conv)
	return r.db.WithContext(ctx).Create(&m).Error
}

// 根据 ID 查询
func (r *ConversationRepository) GetByID(ctx context.Context, id string) (*conversationentity.Conversation, error) {
	var m model.Conversation

	err := r.db.WithContext(ctx).
		Where("conversation_id = ?", id).
		First(&m).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		return nil, err
	}

	return toConversationDomain(&m), nil
}

func (r *ConversationRepository) FindPrivateConversation(ctx context.Context, userID1, userID2 string) (*conversationentity.Conversation, error) {
	var row model.Conversation
	err := r.db.WithContext(ctx).Where("convtype = ? AND ((user_id1 = ? AND user_id2 = ?) OR (user_id1 = ? AND user_id2 = ?))", conversationvo.PrivateChat, userID1, userID2, userID2, userID1).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toConversationDomain(&row), nil
}

func (r *ConversationRepository) FindRoomConversation(ctx context.Context, roomID string) (*conversationentity.Conversation, error) {
	var row model.Conversation
	err := r.db.WithContext(ctx).Where("convtype = ? AND room_id = ?", conversationvo.RoomChat, roomID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toConversationDomain(&row), nil
}

// LockPrivateParticipants 在创建事务开始时按固定顺序锁用户行，避免同一用户对并发创建不同会话。
func (r *ConversationRepository) LockPrivateParticipants(ctx context.Context, userID1, userID2 string) error {
	if userID1 == "" || userID2 == "" || userID1 == userID2 {
		return errors.New("私聊参与者无效")
	}
	for _, id := range []string{min(userID1, userID2), max(userID1, userID2)} {
		var row model.User
		if err := r.db.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Select("user_id").Where("user_id = ?", id).Take(&row).Error; err != nil {
			return err
		}
	}
	return nil
}

func (r *ConversationRepository) ReserveSequenceRange(
	ctx context.Context,
	conversationID string,
	count int,
	lastMessageID string,
) (int64, error) {
	if count <= 0 {
		return 0, errors.New("消息批次不能为空")
	}
	var row model.Conversation
	err := r.db.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Model(&model.Conversation{}).
		Where("conversation_id = ?", conversationID).
		First(&row).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, conversationentity.ErrConversationNotCreated
		}
		return 0, err
	}

	firstSeq := row.LatestSeq + 1
	lastSeq := row.LatestSeq + int64(count)

	err = r.db.WithContext(ctx).
		Model(&model.Conversation{}).
		Where("conversation_id = ?", conversationID).
		Updates(map[string]any{
			"latest_seq":        lastSeq,
			"latest_message_id": lastMessageID,
		}).Error

	if err != nil {
		return 0, err
	}

	return firstSeq, nil
}

func (r *ConversationRepository) UpdateLatestSequence(
	ctx context.Context,
	conversationID string,
	messageID string,
) (int64, error) {
	return r.ReserveSequenceRange(ctx, conversationID, 1, messageID)
}

func (r *ConversationRepository) GetConversationSeq(
	ctx context.Context,
	conversationID string,
) (int64, error) {
	var conv model.Conversation

	err := r.db.WithContext(ctx).
		Select("latest_seq").
		Where("conversation_id = ?", conversationID).
		Take(&conv).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, conversationentity.ErrConversationNotCreated
	}
	if err != nil {
		return 0, err
	}

	return conv.LatestSeq, nil
}

func (r *ConversationRepository) ListByIDs(
	ctx context.Context,
	ids []string,
) ([]*conversationentity.Conversation, error) {

	if len(ids) == 0 {
		return []*conversationentity.Conversation{}, nil
	}

	var models []model.Conversation

	err := r.db.WithContext(ctx).
		Where("conversation_id IN ?", ids).
		Find(&models).Error
	if err != nil {
		return nil, err
	}

	// 转换为 domain
	result := make([]*conversationentity.Conversation, 0, len(models))
	for _, m := range models {
		result = append(result, toConversationDomain(&m))
	}

	return result, nil
}
