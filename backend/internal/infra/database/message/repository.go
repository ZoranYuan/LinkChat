package inframessage

import (
	"context"
	"errors"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	domainmessage "IM_backend/internal/domain/message"
)

type MessageRepository struct {
	db *gorm.DB
}

func NewMessageRepository(db *gorm.DB) *MessageRepository {
	return &MessageRepository{db: db}
}

func (r *MessageRepository) WithTx(tx any) domainmessage.MessageRepository {
	return &MessageRepository{
		db: tx.(*gorm.DB),
	}
}

// 保存消息
func (r *MessageRepository) CreateNewMessage(ctx context.Context, msg *domainmessage.Message) error {
	m := toMessageModel(msg)
	err := r.db.WithContext(ctx).Create(m).Error
	if err != nil {
		var mysqlErr *mysqlDriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 && m.ClientMsgID != nil {
			return domainmessage.ErrDuplicateClientMessage
		}
	}
	return err
}

func (r *MessageRepository) CreateNewMessages(ctx context.Context, messages []*domainmessage.Message) error {
	if len(messages) == 0 {
		return nil
	}
	rows := make([]*MessageModel, 0, len(messages))
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
			return domainmessage.ErrDuplicateClientMessage
		}
	}
	return err
}

func (r *MessageRepository) FindMessageBySenderIDAndClientMsgID(
	ctx context.Context,
	senderID, clientMsgID string,
) (*domainmessage.Message, error) {
	if senderID == "" || clientMsgID == "" {
		return nil, nil
	}
	var m MessageModel
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

func (r *MessageRepository) FindMessageByMessageID(
	ctx context.Context,
	messageID string,
) (*domainmessage.Message, error) {
	if messageID == "" {
		return nil, nil
	}
	var m MessageModel
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
	conversationID string,
	maxSeq int64,
	limit int,
) ([]*domainmessage.Message, error) {

	var models []*MessageModel

	// 只找出聊天的历史消息
	err := r.db.WithContext(ctx).
		Where("conversation_id = ? AND seq < ?", conversationID, maxSeq).
		Order("seq DESC").
		Limit(limit).
		Find(&models).Error

	if err != nil {
		return nil, err
	}

	result := make([]*domainmessage.Message, 0, len(models))
	for _, m := range models {
		result = append(result, toMessageDomain(m))
	}

	return result, nil
}

func (r *MessageRepository) ListAfterSeq(
	ctx context.Context,
	conversationID string,
	afterSeq int64,
) ([]*domainmessage.Message, error) {
	var models []*MessageModel

	query := r.db.WithContext(ctx).
		Where("conversation_id = ? AND seq > ?", conversationID, afterSeq).
		Order("seq ASC")
	err := query.Find(&models).Error
	if err != nil {
		return nil, err
	}

	result := make([]*domainmessage.Message, 0, len(models))
	for _, m := range models {
		result = append(result, toMessageDomain(m))
	}

	return result, nil
}

func (r *MessageRepository) ListAfterSeqUntil(
	ctx context.Context,
	conversationID string,
	afterSeq int64,
	untilSeq int64,
	limit int,
) ([]*domainmessage.Message, error) {
	if conversationID == "" || untilSeq <= afterSeq || limit <= 0 {
		return []*domainmessage.Message{}, nil
	}

	var models []*MessageModel
	err := r.db.WithContext(ctx).
		Where(
			"conversation_id = ? AND seq > ? AND seq <= ?",
			conversationID,
			afterSeq,
			untilSeq,
		).
		Order("seq ASC").
		Limit(limit).
		Find(&models).Error
	if err != nil {
		return nil, err
	}

	result := make([]*domainmessage.Message, 0, len(models))
	for _, item := range models {
		result = append(result, toMessageDomain(item))
	}
	return result, nil
}

func (r *MessageRepository) ListBySeqRange(
	ctx context.Context,
	conversationID string,
	fromSeq int64,
	toSeq int64,
) ([]*domainmessage.Message, error) {
	if conversationID == "" || toSeq <= fromSeq {
		return []*domainmessage.Message{}, nil
	}
	var models []*MessageModel
	err := r.db.WithContext(ctx).
		Where("conversation_id = ? AND seq >= ? AND seq <= ?", conversationID, fromSeq, toSeq).
		Order("seq ASC").
		Find(&models).Error
	if err != nil {
		return nil, err
	}
	result := make([]*domainmessage.Message, 0, len(models))
	for _, m := range models {
		result = append(result, toMessageDomain(m))
	}
	return result, nil
}

func (r *MessageRepository) ListBySeqs(
	ctx context.Context,
	conversationID string,
	seqs []int64,
) ([]*domainmessage.Message, error) {
	if conversationID == "" || len(seqs) == 0 {
		return []*domainmessage.Message{}, nil
	}

	var models []*MessageModel
	err := r.db.WithContext(ctx).
		Where("conversation_id = ? AND seq IN ?", conversationID, seqs).
		Order("seq ASC").
		Find(&models).Error
	if err != nil {
		return nil, err
	}

	result := make([]*domainmessage.Message, 0, len(models))
	for _, m := range models {
		result = append(result, toMessageDomain(m))
	}

	return result, nil
}

func (r *MessageRepository) GetLatestMessagesByConversationIDs(
	ctx context.Context,
	conversationIDs []string,
) ([]*domainmessage.Message, error) {

	var msgs []*MessageModel

	err := r.db.WithContext(ctx).
		Raw(`
			select m.* 
			from messages m
			join conversations c
			on m.message_id = c.latest_message_id
			where c.conversation_id in (?)
        `, conversationIDs).
		Find(&msgs).Error

	var domains []*domainmessage.Message

	for _, m := range msgs {
		domains = append(domains, toMessageDomain(m))
	}

	return domains, err
}
