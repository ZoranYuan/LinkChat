package inframessage

import (
	"context"
	"time"

	"gorm.io/gorm"

	domainmessage "IM_backend/internal/domain/message"
)

type MessageAttachmentRepository struct {
	db *gorm.DB
}

func NewMessageAttachmentRepository(db *gorm.DB) *MessageAttachmentRepository {
	return &MessageAttachmentRepository{
		db: db,
	}
}

func (m *MessageAttachmentRepository) WithTx(tx any) domainmessage.MessageAttachmentRepository {
	return &MessageAttachmentRepository{db: tx.(*gorm.DB)}
}

func toMessageAttachmentModel(e *domainmessage.MessageAttachment) *MessageAttachmentModel {
	return &MessageAttachmentModel{
		AttachmentID:   e.AttachmentID,
		MessageID:      e.MessageID,
		ConversationID: e.ConversationID,
		FileID:         e.FileID,
		Kind:           e.Kind,
		ExpireAt:       e.ExpireAt,
		CreatedAt:      e.CreatedAt,
	}
}

func toMessageAttachmentDomain(m *MessageAttachmentModel) *domainmessage.MessageAttachment {
	return &domainmessage.MessageAttachment{
		AttachmentID:   m.AttachmentID,
		MessageID:      m.MessageID,
		ConversationID: m.ConversationID,
		FileID:         m.FileID,
		Kind:           m.Kind,
		ExpireAt:       m.ExpireAt,
		CreatedAt:      m.CreatedAt,
	}
}

func (m *MessageAttachmentRepository) Create(ctx context.Context, item *domainmessage.MessageAttachment) error {
	return m.db.WithContext(ctx).Create(toMessageAttachmentModel(item)).Error
}

func (m *MessageAttachmentRepository) CreateBatch(ctx context.Context, items []*domainmessage.MessageAttachment) error {
	if len(items) == 0 {
		return nil
	}
	rows := make([]*MessageAttachmentModel, len(items))
	for i, item := range items {
		rows[i] = toMessageAttachmentModel(item)
	}
	return m.db.WithContext(ctx).Create(&rows).Error
}

func (m *MessageAttachmentRepository) BatchGetByMessageIDs(ctx context.Context, messageIDs []string, isActivate bool) (map[string]*domainmessage.MessageAttachment, error) {
	if len(messageIDs) == 0 {
		return map[string]*domainmessage.MessageAttachment{}, nil
	}

	query := m.db.WithContext(ctx).Where("message_id IN (?)", messageIDs)
	if isActivate {
		query = query.Where("expire_at > ?", time.Now().UnixMilli())
	}

	var rows []MessageAttachmentModel
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}

	result := make(map[string]*domainmessage.MessageAttachment, len(rows))
	for i := range rows {
		result[rows[i].MessageID] = toMessageAttachmentDomain(&rows[i])
	}
	return result, nil
}

func (m *MessageAttachmentRepository) ListAccessibleAttachmentsByUserIDAndAttachmentIDs(ctx context.Context, userID string, attachmentIDs []string) (map[string]*domainmessage.MessageAttachment, error) {
	result := make(map[string]*domainmessage.MessageAttachment, len(attachmentIDs))
	if userID == "" || len(attachmentIDs) == 0 {
		return result, nil
	}

	var rows []MessageAttachmentModel
	err := m.db.WithContext(ctx).
		Table("message_attachments AS ma").
		Select("ma.*").
		Joins("JOIN user_conversations AS uc ON uc.conversation_id = ma.conversation_id").
		Where("uc.user_id = ? AND ma.attachment_id IN (?) AND ma.expire_at > ?", userID, attachmentIDs, time.Now().UnixMilli()).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}

	for i := range rows {
		attachment := toMessageAttachmentDomain(&rows[i])
		result[attachment.AttachmentID] = attachment
	}
	return result, nil
}
