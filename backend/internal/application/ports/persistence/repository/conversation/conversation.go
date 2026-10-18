package conversation

import (
	conversationentity "IM_backend/internal/domain/conversation/entity"
	"context"
)

type ConversationRepository interface {
	CreateConversation(ctx context.Context, conversation *conversationentity.Conversation) error
	GetConversationSeq(ctx context.Context, conversationID string) (int64, error)
	GetByID(ctx context.Context, conversationID string) (*conversationentity.Conversation, error)
	FindPrivateConversation(ctx context.Context, userID1, userID2 string) (*conversationentity.Conversation, error)
	FindRoomConversation(ctx context.Context, roomID string) (*conversationentity.Conversation, error)
	LockPrivateParticipants(ctx context.Context, userID1, userID2 string) error
	ReserveSequenceRange(
		ctx context.Context,
		conversationID string,
		count int,
		lastMessageID string,
	) (int64, error)
	UpdateLatestSequence(ctx context.Context, conversationID, messageID string) (int64, error)
	ListByIDs(ctx context.Context, ids []string) ([]*conversationentity.Conversation, error)
	WithTx(tx any) ConversationRepository
}
