package entity

type UserConversation struct {
	UserId                string
	ConversationId        string
	LastReadSeq           int64
	SummaryUnreadBeginSeq int64
	SummaryUnreadEndSeq   int64
	IsMuted               bool
}

func BuildUserConversation(
	userId string,
	conversationId string,
	lastReadSeq int64,
) *UserConversation {
	return &UserConversation{
		UserId:         userId,
		ConversationId: conversationId,
		LastReadSeq:    lastReadSeq,
	}
}

func (uc *UserConversation) UpdateReadSeq(lastReadSeq int64) {
	uc.LastReadSeq = lastReadSeq
}
