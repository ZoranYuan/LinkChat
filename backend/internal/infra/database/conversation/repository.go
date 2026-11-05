package infraconversation

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	domainconversation "IM_backend/internal/domain/conversation"
	infrauser "IM_backend/internal/infra/database/user"
)

type ConversationRepository struct {
	db *gorm.DB
}

func NewConversationRepository(db *gorm.DB) *ConversationRepository {
	return &ConversationRepository{db: db}
}

func (r *ConversationRepository) WithTx(tx any) domainconversation.ConversationRepository {
	return &ConversationRepository{
		db: tx.(*gorm.DB),
	}
}

// 创建会话
func (r *ConversationRepository) CreateConversation(ctx context.Context, conv *domainconversation.Conversation) error {
	m := toConversationModel(conv)
	return r.db.WithContext(ctx).Create(&m).Error
}

// 根据 ID 查询
func (r *ConversationRepository) FindConversationByID(ctx context.Context, id string) (*domainconversation.Conversation, error) {
	var m ConversationModel

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

func (r *ConversationRepository) FindConversationByUserIDs(ctx context.Context, userID1, userID2 string) (*domainconversation.Conversation, error) {
	var row ConversationModel
	err := r.db.WithContext(ctx).Where("user_id1 = ? AND user_id2 = ?", min(userID1, userID2), max(userID1, userID2)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toConversationDomain(&row), nil
}

func (r *ConversationRepository) FindConversationByRoomID(ctx context.Context, roomID string) (*domainconversation.Conversation, error) {
	var row ConversationModel
	err := r.db.WithContext(ctx).Where("convtype = ? AND room_id = ?", domainconversation.RoomChat, roomID).Take(&row).Error
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
		var row infrauser.UserModel
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
	var row ConversationModel
	err := r.db.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Model(&ConversationModel{}).
		Where("conversation_id = ?", conversationID).
		First(&row).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, domainconversation.ErrConversationNotCreated
		}
		return 0, err
	}

	firstSeq := row.LatestSeq + 1
	lastSeq := row.LatestSeq + int64(count)

	err = r.db.WithContext(ctx).
		Model(&ConversationModel{}).
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

func (r *ConversationRepository) GetConversationSeq(
	ctx context.Context,
	conversationID string,
) (int64, error) {
	var conv ConversationModel

	err := r.db.WithContext(ctx).
		Select("latest_seq").
		Where("conversation_id = ?", conversationID).
		Take(&conv).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, domainconversation.ErrConversationNotCreated
	}
	if err != nil {
		return 0, err
	}

	return conv.LatestSeq, nil
}

func (r *ConversationRepository) ListConversationsByIDs(
	ctx context.Context,
	ids []string,
) ([]*domainconversation.Conversation, error) {

	if len(ids) == 0 {
		return []*domainconversation.Conversation{}, nil
	}

	var models []ConversationModel

	err := r.db.WithContext(ctx).
		Where("conversation_id IN ?", ids).
		Find(&models).Error
	if err != nil {
		return nil, err
	}

	// 转换为 domain
	result := make([]*domainconversation.Conversation, 0, len(models))
	for _, m := range models {
		result = append(result, toConversationDomain(&m))
	}

	return result, nil
}
