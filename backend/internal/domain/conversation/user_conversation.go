package domainconversation

type UserConversation struct {
	UserID                string `json:"UserId"`
	ConversationID        string `json:"ConversationId"`
	LastReadSeq           int64
	SummaryUnreadBeginSeq int64
	SummaryUnreadEndSeq   int64
	IsMuted               bool
}

func BuildUserConversation(
	userID string,
	conversationID string,
	lastReadSeq int64,
) *UserConversation {
	return &UserConversation{
		UserID:         userID,
		ConversationID: conversationID,
		LastReadSeq:    lastReadSeq,
	}
}

func (uc *UserConversation) UpdateReadSeq(lastReadSeq int64) {
	uc.LastReadSeq = lastReadSeq
}
