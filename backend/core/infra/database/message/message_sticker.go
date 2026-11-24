package inframessage

import (
	"context"

	"gorm.io/gorm"

	domainmessage "IM_backend/core/domain/message"
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
