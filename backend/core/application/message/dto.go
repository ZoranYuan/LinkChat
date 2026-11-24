package message

import (
	domainmessage "IM_backend/core/domain/message"
)

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
	StickerID    string
	PackID       string
}

type SendMessageDTO struct {
	ClientMessageID string
	SenderID        string // 服务端注入（connection.UserID()），不在客户端 proto 中
	Type            int
	Content         string
	FileID          string
	StickerID       string
	PackID          string
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
