package infraconversation

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	domainconversation "IM_backend/core/domain/conversation"
	infrauser "IM_backend/core/infra/database/user"
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

func (r *ConversationRepository) FindConversationByGroupID(ctx context.Context, groupID string) (*domainconversation.Conversation, error) {
	var row ConversationModel
	err := r.db.WithContext(ctx).Where("convtype = ? AND group_id = ?", domainconversation.GroupChat, groupID).Take(&row).Error
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
	// 原子累加：UPDATE 自身对目标行加排他锁，天然避免同一会话并发撞号，
	// 不再需要额外的 SELECT ... FOR UPDATE 往返。
	// LAST_INSERT_ID(expr) 把累加后的水位记入当前连接会话变量，紧随其后的
	// SELECT LAST_INSERT_ID() 在同一事务连接上读回，避免两次 DB 往返。
	// 注意：LAST_INSERT_ID() 会被同连接的其他 AUTO_INCREMENT 写入覆盖，
	// 因此必须在同一事务内、紧跟 UPDATE 立即读取，中间不能穿插其他 INSERT。
	result := r.db.WithContext(ctx).
		Model(&ConversationModel{}).
		Where("conversation_id = ?", conversationID).
		Updates(map[string]any{
			"latest_seq":        gorm.Expr("LAST_INSERT_ID(latest_seq + ?)", count),
			"latest_message_id": lastMessageID,
		})

	if result.Error != nil {
		return 0, result.Error
	}
	if result.RowsAffected == 0 {
		return 0, domainconversation.ErrConversationNotCreated
	}

	var lastSeq int64
	if err := r.db.WithContext(ctx).Raw("SELECT LAST_INSERT_ID()").Scan(&lastSeq).Error; err != nil {
		return 0, err
	}

	firstSeq := lastSeq - int64(count) + 1
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
