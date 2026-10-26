package domainmessage

import (
	"context"
)

type MessageAttachmentRepository interface {
	Create(ctx context.Context, item *MessageAttachment) error
	BatchGetByMessageIDs(ctx context.Context, messageIDs []string, isActivate bool) (map[string]*MessageAttachment, error)
	ListAccessibleAttachmentsByUserIDAndAttachmentIDs(ctx context.Context, userID string, attachmentIDs []string) (map[string]*MessageAttachment, error)
	WithTx(tx any) MessageAttachmentRepository
}
