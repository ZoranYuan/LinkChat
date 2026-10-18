package message

import (
	"context"

	messagerepo "IM_backend/internal/application/ports/persistence/repository/message"
	messageentity "IM_backend/internal/domain/message/entity"
	"IM_backend/internal/infrastructure/persistence/mysql/model"

	mysqlDriver "github.com/go-sql-driver/mysql"

	"errors"
	"gorm.io/gorm"
)

type MessageRepository struct {
	db *gorm.DB
}

func NewMessageRepository(db *gorm.DB) *MessageRepository {
	return &MessageRepository{db: db}
}

func (r *MessageRepository) WithTx(tx any) messagerepo.MessageRepository {
	return &MessageRepository{
		db: tx.(*gorm.DB),
	}
}

// 保存消息
func (r *MessageRepository) CreateNewMessage(ctx context.Context, msg *messageentity.Message) error {
	m := toMessageModel(msg)
	err := r.db.WithContext(ctx).Create(m).Error
	if err != nil {
		var mysqlErr *mysqlDriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 && m.ClientMsgId != nil {
			return messageentity.ErrDuplicateClientMessage
		}
	}
	return err
}

func (r *MessageRepository) CreateNewMessages(ctx context.Context, messages []*messageentity.Message) error {
	if len(messages) == 0 {
		return nil
	}
	rows := make([]*model.Message, 0, len(messages))
	for _, msg := range messages {
		if msg != nil {
			rows = append(rows, toMessageModel(msg))
		}
	}
	if len(rows) == 0 {
		return nil
	}
	err := r.db.WithContext(ctx).Create(&rows).Error
	if err != nil {
		var mysqlErr *mysqlDriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return messageentity.ErrDuplicateClientMessage
		}
	}
	return err
}

func (r *MessageRepository) FindByClientMsgID(
	ctx context.Context,
	senderID, clientMsgID string,
) (*messageentity.Message, error) {
	if senderID == "" || clientMsgID == "" {
		return nil, nil
	}
	var m model.Message
	err := r.db.WithContext(ctx).
		Where("sender_id = ? AND client_msg_id = ?", senderID, clientMsgID).
		First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toMessageDomain(&m), nil
}

func (r *MessageRepository) FindByMessageID(
	ctx context.Context,
	messageID string,
) (*messageentity.Message, error) {
	if messageID == "" {
		return nil, nil
	}
	var m model.Message
	err := r.db.WithContext(ctx).Where("message_id = ?", messageID).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toMessageDomain(&m), nil
}

func (r *MessageRepository) GetHistoryMessage(
	ctx context.Context,
	conversationId string,
	maxSeq int64,
	limit int,
) ([]*messageentity.Message, error) {

	var models []*model.Message

	// 只找出聊天的历史消息
	err := r.db.WithContext(ctx).
		Where("conversation_id = ? AND seq < ?", conversationId, maxSeq).
		Order("seq DESC").
		Limit(limit).
		Find(&models).Error

	if err != nil {
		return nil, err
	}

	result := make([]*messageentity.Message, 0, len(models))
	for _, m := range models {
		result = append(result, toMessageDomain(m))
	}

	return result, nil
}

func (r *MessageRepository) ListAfterSeq(
	ctx context.Context,
	conversationId string,
	afterSeq int64,
) ([]*messageentity.Message, error) {
	var models []*model.Message

	query := r.db.WithContext(ctx).
		Where("conversation_id = ? AND seq > ?", conversationId, afterSeq).
		Order("seq ASC")
	err := query.Find(&models).Error
	if err != nil {
		return nil, err
	}

	result := make([]*messageentity.Message, 0, len(models))
	for _, m := range models {
		result = append(result, toMessageDomain(m))
	}

	return result, nil
}

func (r *MessageRepository) ListAfterSeqUntil(
	ctx context.Context,
	conversationId string,
	afterSeq int64,
	untilSeq int64,
	limit int,
) ([]*messageentity.Message, error) {
	if conversationId == "" || untilSeq <= afterSeq || limit <= 0 {
		return []*messageentity.Message{}, nil
	}

	var models []*model.Message
	err := r.db.WithContext(ctx).
		Where(
			"conversation_id = ? AND seq > ? AND seq <= ?",
			conversationId,
			afterSeq,
			untilSeq,
		).
		Order("seq ASC").
		Limit(limit).
		Find(&models).Error
	if err != nil {
		return nil, err
	}

	result := make([]*messageentity.Message, 0, len(models))
	for _, item := range models {
		result = append(result, toMessageDomain(item))
	}
	return result, nil
}

func (r *MessageRepository) ListBySeqRange(
	ctx context.Context,
	conversationId string,
	fromSeq int64,
	toSeq int64,
) ([]*messageentity.Message, error) {
	if conversationId == "" || toSeq <= fromSeq {
		return []*messageentity.Message{}, nil
	}
	var models []*model.Message
	err := r.db.WithContext(ctx).
		Where("conversation_id = ? AND seq >= ? AND seq <= ?", conversationId, fromSeq, toSeq).
		Order("seq ASC").
		Find(&models).Error
	if err != nil {
		return nil, err
	}
	result := make([]*messageentity.Message, 0, len(models))
	for _, m := range models {
		result = append(result, toMessageDomain(m))
	}
	return result, nil
}

func (r *MessageRepository) ListBySeqs(
	ctx context.Context,
	conversationId string,
	seqs []int64,
) ([]*messageentity.Message, error) {
	if conversationId == "" || len(seqs) == 0 {
		return []*messageentity.Message{}, nil
	}

	var models []*model.Message
	err := r.db.WithContext(ctx).
		Where("conversation_id = ? AND seq IN ?", conversationId, seqs).
		Order("seq ASC").
		Find(&models).Error
	if err != nil {
		return nil, err
	}

	result := make([]*messageentity.Message, 0, len(models))
	for _, m := range models {
		result = append(result, toMessageDomain(m))
	}

	return result, nil
}

func (r *MessageRepository) GetLatestMessagesByConversationIDs(
	ctx context.Context,
	conversationIDs []string,
) ([]*messageentity.Message, error) {

	var msgs []*model.Message

	err := r.db.WithContext(ctx).
		Raw(`
			select m.* 
			from messages m
			join conversations c
			on m.message_id = c.latest_message_id
			where c.conversation_id in (?)
        `, conversationIDs).
		Find(&msgs).Error

	var domains []*messageentity.Message

	for _, m := range msgs {
		domains = append(domains, toMessageDomain(m))
	}

	return domains, err
}
