package file

import (
	"context"
	"time"
)

type AttachmentURL struct {
	AttachmentID string `json:"attachmentId"`
	MediaURL     string `json:"mediaUrl"`
	ThumbURL     string `json:"thumbUrl,omitempty"`
	ExpiresAt    int64  `json:"expiresAt"`
}

type FileCache interface {
	GetAttachmentURLBatch(ctx context.Context, attachmentIDs []string) (map[string]*AttachmentURL, error)
	SetAttachmentURLBatch(ctx context.Context, values []*AttachmentURL, ttl time.Duration) error
}
