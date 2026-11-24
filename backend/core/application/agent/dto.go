package agent

type ChatMessageDTO struct {
	MessageID      string
	ConversationID string
	SenderID       string
	SenderName     string
	Seq            int64
	Type           int32
	Content        string
	Status         int32
	SendTime       int64
}
