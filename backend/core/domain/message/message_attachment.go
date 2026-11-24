package domainmessage

// AccessibleAttachment 是「用户有权访问的附件 + 其文件元数据」的只读视图。
// 由鉴权查询一次 JOIN 拿全，供 AccessURLs 直接组装 DTO，避免再回源 files 表。
type AccessibleAttachment struct {
	AttachmentID string
	FileID       string

	ObjectKey   string
	FileName    string
	ContentType string
	Size        int64
}

type MessageAttachment struct {
	AttachmentID   string `json:"AttachmentId"`
	MessageID      string `json:"MessageId"`
	ConversationID string `json:"ConversationId"`
	FileID         string `json:"FileId"`
	ExpireAt       int64
	CreatedAt      int64
}

func NewMessageAttachment(
	attachmentID string,
	messageID string,
	conversationID string,
	fileID string,
	expireAt int64,
	createdAt int64,
) *MessageAttachment {
	return &MessageAttachment{
		AttachmentID:   attachmentID,
		MessageID:      messageID,
		ConversationID: conversationID,
		FileID:         fileID,
		ExpireAt:       expireAt,
		CreatedAt:      createdAt,
	}
}
