package message

import (
	domainmessage "IM_backend/internal/domain/message"
)

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

func toMessageDTOs(ms []*domainmessage.Message) []MessageDTO {
	if len(ms) == 0 {
		return nil
	}

	res := make([]MessageDTO, 0, len(ms))
	for _, m := range ms {
		if m == nil {
			continue
		}

		item := MessageDTO{
			MessageID:      m.MessageID,
			ConversationID: m.ConversationID,
			SenderID:       m.SenderID,
			Seq:            m.Seq,
			Type:           int(m.Type),
			Content:        m.Content,
			Status:         int8(m.Status),
			SendTime:       m.SendTime,
		}
		if m.ClientMsgID != nil {
			clientMsgID := *m.ClientMsgID
			item.ClientMessageID = &clientMsgID
		}
		res = append(res, item)
	}

	return res
}
