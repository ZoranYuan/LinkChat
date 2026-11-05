package inframessage

type MessageAttachmentModel struct {
	AttachmentID   string `gorm:"type:bigint;primaryKey" json:"attachmentId"`
	MessageID      string `gorm:"type:bigint;not null;uniqueIndex:uk_attachment_message" json:"messageId"`
	ConversationID string `gorm:"type:bigint;not null" json:"conversationId"`
	FileID         string `gorm:"type:bigint;not null;index:idx_attachment_file_expire,priority:1" json:"fileId"`
	Kind           int8   `gorm:"not null" json:"kind"`
	ExpireAt       int64  `gorm:"not null;index:idx_attachment_file_expire,priority:2" json:"expireAt"`
	CreatedAt      int64  `gorm:"not null" json:"createdAt"`
}

func (MessageAttachmentModel) TableName() string { return "message_attachments" }

type MessageModel struct {
	MessageID      string  `gorm:"type:bigint;primaryKey" json:"messageId"`
	ConversationID string  `gorm:"type:bigint;not null;uniqueIndex:uk_message_conv_seq,priority:1" json:"conversationId"`
	SenderID       string  `gorm:"type:bigint;not null;uniqueIndex:uk_message_sender_client,priority:1" json:"senderId"`
	ClientMsgID    *string `gorm:"size:64;uniqueIndex:uk_message_sender_client,priority:2" json:"clientMsgId,omitempty"`
	RequestHash    string  `gorm:"size:64;default:''" json:"-"`

	Seq      int64  `gorm:"not null;uniqueIndex:uk_message_conv_seq,priority:2" json:"seq"`
	Type     int8   `gorm:"not null;comment:1=text 2=image 3=video 4=sticker 5=file" json:"type"`
	Content  string `gorm:"type:text" json:"content"`
	Status   int8   `gorm:"default:1;comment:1=normal 2=recall" json:"status"`
	SendTime int64  `gorm:"not null" json:"sendTime"`
}

func (MessageModel) TableName() string { return "messages" }

type MessageStickerModel struct {
	MessageID string `gorm:"type:bigint;primaryKey" json:"messageId"`
	StickerID string `gorm:"size:32;not null" json:"stickerId"`
	PackID    string `gorm:"size:32" json:"packId,omitempty"`
	Width     int    `gorm:"not null" json:"width"`
	Height    int    `gorm:"not null" json:"height"`
}

func (MessageStickerModel) TableName() string { return "message_stickers" }
