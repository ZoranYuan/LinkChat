package inframessage

type MessageAttachmentModel struct {
	AttachmentID   string `gorm:"size:32;primaryKey" json:"attachmentId"`
	MessageID      string `gorm:"size:32;not null;index:idx_attachment_message" json:"messageId"`
	ConversationID string `gorm:"size:64;not null;index:idx_attachment_conversation" json:"conversationId"`
	FileID         string `gorm:"size:32;not null;index:idx_attachment_file" json:"fileId"`
	Kind           int8   `gorm:"not null;index:idx_attachment_kind" json:"kind"`
	ExpireAt       int64  `gorm:"not null;index:idx_attachment_expire_at" json:"expireAt"`
	CreatedAt      int64  `gorm:"not null;index:idx_attachment_created_at" json:"createdAt"`
}

func (MessageAttachmentModel) TableName() string { return "message_attachments" }

type MessageModel struct {
	MessageID      string  `gorm:"size:32;primaryKey" json:"messageId"`
	ConversationID string  `gorm:"size:64;not null;uniqueIndex:uk_message_conv_seq,priority:1;uniqueIndex:uq_sender_conversation_client_msg,priority:2" json:"conversationId"`
	SenderID       string  `gorm:"size:32;not null;uniqueIndex:uq_sender_conversation_client_msg,priority:1" json:"senderId"`
	ClientMsgID    *string `gorm:"size:64;uniqueIndex:uq_sender_conversation_client_msg,priority:3" json:"clientMsgId,omitempty"`
	RequestHash    string  `gorm:"size:64;default:''" json:"-"`

	Seq      int64  `gorm:"not null;uniqueIndex:uk_message_conv_seq,priority:2" json:"seq"`
	Type     int8   `gorm:"not null;comment:1=text 2=image 3=video 4=sticker 5=file" json:"type"`
	Content  string `gorm:"type:text" json:"content"`
	Status   int8   `gorm:"default:1;comment:1=normal 2=recall" json:"status"`
	SendTime int64  `gorm:"not null;index:idx_send_time" json:"sendTime"`
}

func (MessageModel) TableName() string { return "messages" }

type MessageStickerModel struct {
	MessageID string `gorm:"size:32;primaryKey" json:"messageId"`
	StickerID string `gorm:"size:32;not null;index" json:"stickerId"`
	PackID    string `gorm:"size:32;index" json:"packId,omitempty"`
	Width     int    `gorm:"not null" json:"width"`
	Height    int    `gorm:"not null" json:"height"`
}

func (MessageStickerModel) TableName() string { return "message_stickers" }
