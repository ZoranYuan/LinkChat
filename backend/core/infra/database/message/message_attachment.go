package inframessage

import (
	"context"
	"time"

	"gorm.io/gorm"

	domainfile "IM_backend/core/domain/file"
	domainmessage "IM_backend/core/domain/message"
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
		ExpireAt:       m.ExpireAt,
		CreatedAt:      m.CreatedAt,
	}
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

type accessibleAttachmentRow struct {
	AttachmentID string `gorm:"column:attachment_id"`
	FileID       string `gorm:"column:file_id"`
	ObjectKey    string `gorm:"column:object_key"`
	FileName     string `gorm:"column:file_name"`
	ContentType  string `gorm:"column:content_type"`
	Size         int64  `gorm:"column:size"`
}

func (m *MessageAttachmentRepository) ListAccessibleAttachmentsByUserIDAndAttachmentIDs(ctx context.Context, userID string, attachmentIDs []string) (map[string]*domainmessage.AccessibleAttachment, error) {
	result := make(map[string]*domainmessage.AccessibleAttachment, len(attachmentIDs))
	if userID == "" || len(attachmentIDs) == 0 {
		return result, nil
	}

	var rows []accessibleAttachmentRow
	err := m.db.WithContext(ctx).
		Table("message_attachments AS ma").
		Select("ma.attachment_id, ma.file_id, f.object_key, f.file_name, f.content_type, f.size").
		Joins("JOIN user_conversations AS uc ON uc.conversation_id = ma.conversation_id").
		Joins("JOIN files AS f ON f.file_id = ma.file_id AND f.status = ?", domainfile.FileStatusUploaded).
		Where("uc.user_id = ? AND ma.attachment_id IN (?) AND ma.expire_at > ?", userID, attachmentIDs, time.Now().UnixMilli()).
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}

	for i := range rows {
		row := rows[i]
		result[row.AttachmentID] = &domainmessage.AccessibleAttachment{
			AttachmentID: row.AttachmentID,
			FileID:       row.FileID,
			ObjectKey:    row.ObjectKey,
			FileName:     row.FileName,
			ContentType:  row.ContentType,
			Size:         row.Size,
		}
	}

	return result, nil
}
