package domainmessage

import (
	"context"
)

type MessageRepository interface {
	CreateNewMessage(ctx context.Context, msg *Message) error
	CreateNewMessages(ctx context.Context, messages []*Message) error
	FindMessageByMessageID(ctx context.Context, messageID string) (*Message, error)
	FindMessageBySenderIDAndClientMsgID(ctx context.Context, senderID, clientMsgID string) (*Message, error)

	GetHistoryMessage(
		ctx context.Context,
		conversationID string,
		minSeq int64,
		limit int,
	) ([]*Message, error)

	ListAfterSeq(
		ctx context.Context,
		conversationID string,
		afterSeq int64,
	) ([]*Message, error)

	ListAfterSeqUntil(
		ctx context.Context,
		conversationID string,
		afterSeq int64,
		untilSeq int64,
		limit int,
	) ([]*Message, error)

	ListBySeqRange(
		ctx context.Context,
		conversationID string,
		fromSeq int64,
		toSeq int64,
	) ([]*Message, error)

	ListBySeqs(
		ctx context.Context,
		conversationID string,
		seqs []int64,
	) ([]*Message, error)

	GetLatestMessagesByConversationIDs(
		ctx context.Context,
		conversationIDs []string,
	) ([]*Message, error)

	WithTx(tx any) MessageRepository
}

type MessageStickerRepository interface {
	Create(ctx context.Context, item *MessageSticker) error
	CreateBatch(ctx context.Context, items []*MessageSticker) error
	BatchGetByMessageIDs(ctx context.Context, messageIDs []string) (map[string]*MessageSticker, error)
	WithTx(tx any) MessageStickerRepository
}
