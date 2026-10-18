package message

import (
	messageentity "IM_backend/internal/domain/message/entity"
	"context"
)

type MessageRepository interface {
	CreateNewMessage(ctx context.Context, msg *messageentity.Message) error
	CreateNewMessages(ctx context.Context, messages []*messageentity.Message) error
	FindByMessageID(ctx context.Context, messageID string) (*messageentity.Message, error)
	FindByClientMsgID(ctx context.Context, senderID, clientMsgID string) (*messageentity.Message, error)

	GetHistoryMessage(
		ctx context.Context,
		conversationId string,
		minSeq int64,
		limit int,
	) ([]*messageentity.Message, error)

	ListAfterSeq(
		ctx context.Context,
		conversationId string,
		afterSeq int64,
	) ([]*messageentity.Message, error)

	ListAfterSeqUntil(
		ctx context.Context,
		conversationId string,
		afterSeq int64,
		untilSeq int64,
		limit int,
	) ([]*messageentity.Message, error)

	ListBySeqRange(
		ctx context.Context,
		conversationId string,
		fromSeq int64,
		toSeq int64,
	) ([]*messageentity.Message, error)

	ListBySeqs(
		ctx context.Context,
		conversationId string,
		seqs []int64,
	) ([]*messageentity.Message, error)

	GetLatestMessagesByConversationIDs(
		ctx context.Context,
		conversationIDs []string,
	) ([]*messageentity.Message, error)

	WithTx(tx any) MessageRepository
}

type MessageStickerRepository interface {
	Create(ctx context.Context, item *messageentity.MessageSticker) error
	GetByMessageID(ctx context.Context, messageId string) (*messageentity.MessageSticker, error)
	BatchGetByMessageIDs(ctx context.Context, messageIds []string) (map[string]*messageentity.MessageSticker, error)
	WithTx(tx any) MessageStickerRepository
}
