package domainconversation

import (
	"context"
)

type ReadSeqUpdate struct {
	UserID         string
	ConversationID string
	LastReadSeq    int64
}

type UserConversationRepository interface {
	CreateUserConversation(ctx context.Context, conversation *UserConversation) error
	FindUserConversationByUserIDAndConversationID(ctx context.Context, userID, conversationID string) (*UserConversation, error)
	CaptureRoomUnreadSnapshot(ctx context.Context, userID, conversationID string, latestSeq int64) (*UserConversation, error)
	UpdateReadSeq(ctx context.Context, conversation *UserConversation) error
	UpdateReadSeqBatch(ctx context.Context, updates []ReadSeqUpdate) error
	AdvanceReadSeq(ctx context.Context, conversation *UserConversation) (bool, error)
	ListUserConversationsByUserID(ctx context.Context, userID string) ([]*UserConversation, error)
	DelUserConversation(ctx context.Context, userID string, conversationID string) error
	WithTx(tx any) UserConversationRepository
}
