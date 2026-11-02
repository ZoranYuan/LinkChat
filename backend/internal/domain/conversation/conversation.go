package domainconversation

type Conversation struct {
	ConversationID  string   `json:"ConversationId"`
	ConvType        ConvType `json:"Convtype"`
	UserID1         string   `json:"UserId1"`
	UserID2         string   `json:"UserId2"`
	RoomID          string   `json:"RoomId"`
	LatestMessageID string   `json:"LatestMessageId"`
	LatestSeq       int64
}

func NewRoomConversation(conversationID, roomID string) *Conversation {
	return &Conversation{
		ConversationID: conversationID,
		ConvType:       Room,
		RoomID:         roomID,
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
