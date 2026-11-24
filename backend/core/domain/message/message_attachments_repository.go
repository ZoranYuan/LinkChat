package domainmessage

import (
	"context"
)

type MessageAttachmentRepository interface {
	CreateBatch(ctx context.Context, items []*MessageAttachment) error
	BatchGetByMessageIDs(ctx context.Context, messageIDs []string, isActivate bool) (map[string]*MessageAttachment, error)
	ListAccessibleAttachmentsByUserIDAndAttachmentIDs(ctx context.Context, userID string, attachmentIDs []string) (map[string]*AccessibleAttachment, error)
	WithTx(tx any) MessageAttachmentRepository
}
