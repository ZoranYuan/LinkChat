package domainconversation

import (
	"context"
)

type ConversationRepository interface {
	CreateConversation(ctx context.Context, conversation *Conversation) error
	GetConversationSeq(ctx context.Context, conversationID string) (int64, error)
	FindConversationByID(ctx context.Context, conversationID string) (*Conversation, error)
	FindConversationByUserIDs(ctx context.Context, userID1, userID2 string) (*Conversation, error)
	FindConversationByRoomID(ctx context.Context, roomID string) (*Conversation, error)
	LockPrivateParticipants(ctx context.Context, userID1, userID2 string) error
	ReserveSequenceRange(
		ctx context.Context,
		conversationID string,
		count int,
		lastMessageID string,
	) (int64, error)
	ListConversationsByIDs(ctx context.Context, ids []string) ([]*Conversation, error)
	WithTx(tx any) ConversationRepository
}
