package message

import messageentity "IM_backend/internal/domain/message/entity"

type SendMessageDTO struct {
	ClientMessageID  string
	SenderID         string
	ReceiverID       string
	ConversationID   string
	ConversationType int
	Type             int
	Content          string
	AttachmentID     string
	FileID           string
	FileName         string
	FileSize         int64
	MimeType         string
	Width            int
	Height           int
	DurationMs       *int64
	StickerID        string
	PackID           string
}

type MessageAckDTO struct {
	ClientMessageID string
	ConversationID  string
	MessageID       string
	Seq             int64
	AttachmentID    string
	SendTime        int64
	Status          string
	Error           string
}

type MessageDTO struct {
	MessageID       string
	ConversationID  string
	SenderID        string
	ClientMessageID *string
	Seq             int64
	Type            int
	Content         string
	Status          int8
	SendTime        int64

	AttachmentID string
	Width        int
	Height       int
	DurationMs   *int64
	StickerID    string
	PackID       string
}

func toMessageDTOs(ms []*messageentity.Message) []MessageDTO {
	if len(ms) == 0 {
		return nil
	}

	res := make([]MessageDTO, 0, len(ms))
	for _, m := range ms {
		if m == nil {
			continue
		}

		item := MessageDTO{
			MessageID:      m.MessageId,
			ConversationID: m.ConversationId,
			SenderID:       m.SenderId,
			Seq:            m.Seq,
			Type:           int(m.Type),
			Content:        m.Content,
			Status:         int8(m.Status),
			SendTime:       m.SendTime,
		}
		if m.ClientMsgId != nil {
			clientMsgID := *m.ClientMsgId
			item.ClientMessageID = &clientMsgID
		}
		res = append(res, item)
	}

	return res
}
