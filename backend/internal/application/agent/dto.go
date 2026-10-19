package agent

type RoomAgentContext struct {
	UserID             string
	ThreadID           string
	UnreadMessageCount uint32
	UnreadBeginSeq     int64
	UnreadEndSeq       int64
	Messages           []RoomAgentContextMessage
}

type RoomAgentContextMessage struct {
	MessageID      string
	ConversationID string
	SenderID       string
	ClientMsgID    *string
	Seq            int64
	Type           int32
	Content        string
	Status         int32
	SendTime       int64
}
