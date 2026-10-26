package inframessage

import (
	domainmessage "IM_backend/internal/domain/message"
)

func toMessageDomain(m *MessageModel) *domainmessage.Message {
	if m == nil {
		return nil
	}

	return &domainmessage.Message{
		MessageID:      m.MessageID,
		ConversationID: m.ConversationID,
		SenderID:       m.SenderID,
		ClientMsgID:    m.ClientMsgID,
		RequestHash:    m.RequestHash,
		Seq:            m.Seq,
		Type:           domainmessage.MessageType(m.Type),
		Content:        m.Content,
		Status:         domainmessage.Status(m.Status),
		SendTime:       m.SendTime,
	}
}

func toMessageModel(d *domainmessage.Message) *MessageModel {
	if d == nil {
		return nil
	}

	return &MessageModel{
		MessageID:      d.MessageID,
		ConversationID: d.ConversationID,
		SenderID:       d.SenderID,
		ClientMsgID:    d.ClientMsgID,
		RequestHash:    d.RequestHash,
		Seq:            d.Seq,
		Type:           int8(d.Type),
		Content:        d.Content,
		Status:         int8(d.Status),
		SendTime:       d.SendTime,
	}
}
