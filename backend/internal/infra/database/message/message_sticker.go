package inframessage

import (
	"context"

	"gorm.io/gorm"

	domainmessage "IM_backend/internal/domain/message"
)

type MessageStickerRepository struct{ db *gorm.DB }

func NewMessageStickerRepository(db *gorm.DB) *MessageStickerRepository {
	return &MessageStickerRepository{db: db}
}
func (r *MessageStickerRepository) WithTx(tx any) domainmessage.MessageStickerRepository {
	return &MessageStickerRepository{db: tx.(*gorm.DB)}
}
func toMessageStickerModel(e *domainmessage.MessageSticker) *MessageStickerModel {
	if e == nil {
		return nil
	}
	return &MessageStickerModel{MessageID: e.MessageID, StickerID: e.StickerID, PackID: e.PackID, Width: e.Width, Height: e.Height}
}
func toMessageStickerDomain(m *MessageStickerModel) *domainmessage.MessageSticker {
	if m == nil {
		return nil
	}
	return &domainmessage.MessageSticker{MessageID: m.MessageID, StickerID: m.StickerID, PackID: m.PackID, Width: m.Width, Height: m.Height}
}
func (r *MessageStickerRepository) Create(ctx context.Context, item *domainmessage.MessageSticker) error {
	return r.db.WithContext(ctx).Create(toMessageStickerModel(item)).Error
}

func (r *MessageStickerRepository) CreateBatch(ctx context.Context, items []*domainmessage.MessageSticker) error {
	if len(items) == 0 {
		return nil
	}
	rows := make([]*MessageStickerModel, len(items))
	for i, item := range items {
		rows[i] = toMessageStickerModel(item)
	}
	return r.db.WithContext(ctx).Create(&rows).Error
}

func (r *MessageStickerRepository) BatchGetByMessageIDs(ctx context.Context, messageIDs []string) (map[string]*domainmessage.MessageSticker, error) {
	if len(messageIDs) == 0 {
		return map[string]*domainmessage.MessageSticker{}, nil
	}
	var models []MessageStickerModel
	if err := r.db.WithContext(ctx).Where("message_id IN (?)", messageIDs).Find(&models).Error; err != nil {
		return nil, err
	}
	result := make(map[string]*domainmessage.MessageSticker, len(models))
	for i := range models {
		result[models[i].MessageID] = toMessageStickerDomain(&models[i])
	}
	return result, nil
}
