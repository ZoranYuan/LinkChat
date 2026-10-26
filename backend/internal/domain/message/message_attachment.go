package domainmessage

type MessageAttachment struct {
	AttachmentID   string `json:"AttachmentId"`
	MessageID      string `json:"MessageId"`
	ConversationID string `json:"ConversationId"`
	FileID         string `json:"FileId"`
	Kind           int8
	ExpireAt       int64
	CreatedAt      int64
}

func NewMessageAttachment(
	attachmentID string,
	messageID string,
	conversationID string,
	fileID string,
	kind int8,
	expireAt int64,
	createdAt int64,
) *MessageAttachment {
	return &MessageAttachment{
		AttachmentID:   attachmentID,
		MessageID:      messageID,
		ConversationID: conversationID,
		FileID:         fileID,
		Kind:           kind,
		ExpireAt:       expireAt,
		CreatedAt:      createdAt,
	}
}
