package domainconversation

type Conversation struct {
	ConversationID  string   `json:"ConversationId"`
	ConvType        ConvType `json:"Convtype"`
	UserID1         string   `json:"UserId1"`
	UserID2         string   `json:"UserId2"`
	GroupID          string   `json:"GroupId"`
	LatestMessageID string   `json:"LatestMessageId"`
	LatestSeq       int64
}

func NewGroupConversation(conversationID, groupID string) *Conversation {
	return &Conversation{
		ConversationID: conversationID,
		ConvType:       GroupChat,
		GroupID:         groupID,
	}
}

func NewPrivateConversation(conversationID, userID1, userID2 string) *Conversation {
	return &Conversation{
		ConversationID: conversationID,
		ConvType:       PrivateChat,
		UserID1:        min(userID1, userID2),
		UserID2:        max(userID1, userID2),
	}
}
