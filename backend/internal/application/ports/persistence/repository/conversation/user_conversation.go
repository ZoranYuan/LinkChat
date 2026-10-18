package conversation

import (
	conversationentity "IM_backend/internal/domain/conversation/entity"
	"context"
)

type ReadSeqUpdate struct {
	UserID         string
	ConversationID string
	LastReadSeq    int64
}

type UserConversationRepository interface {
	GetUserConversationsByUserId(ctx context.Context, userId string) ([]*conversationentity.UserConversation, error)
	CreateUserConversation(ctx context.Context, conversation *conversationentity.UserConversation) error
	GetUsersByConversationID(ctx context.Context, conversationID string) ([]string, error)
	GetUserConversation(ctx context.Context, userId, conversationId string) (*conversationentity.UserConversation, error)
	UpdateReadSeq(ctx context.Context, conversation *conversationentity.UserConversation) error
	UpdateReadSeqBatch(ctx context.Context, updates []ReadSeqUpdate) error
	AdvanceReadSeq(ctx context.Context, conversation *conversationentity.UserConversation) (bool, error)
	ListByUser(ctx context.Context, userId string) ([]*conversationentity.UserConversation, error)
	DelUserConversation(ctx context.Context, userId string, conversationId string) error
	WithTx(tx any) UserConversationRepository
}
