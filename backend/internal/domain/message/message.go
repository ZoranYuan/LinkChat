package domainmessage

import (
	"errors"
	"time"
)

var ErrDuplicateClientMessage = errors.New("客户端消息已存在")
var ErrClientMessageConflict = errors.New("客户端消息标识对应的内容不一致")

type Message struct {
	MessageID      string      `json:"MessageId"`
	ConversationID string      `json:"ConversationId"`
	SenderID       string      `json:"SenderId"`
	ClientMsgID    *string     `json:"ClientMsgId"`
	RequestHash    string      `json:"RequestHash"`
	Seq            int64       `json:"Seq"`
	Type           MessageType `json:"Type"`
	Content        string      `json:"Content"`
	Status         Status      `json:"Status"`
	SendTime       int64       `json:"SendTime"`
}

func NewMessage(
	messageID,
	conversationID,
	senderID string,
	seq int64,
	cType MessageType,
	content string,
) *Message {
	return &Message{
		MessageID:      messageID,
		ConversationID: conversationID,
		Seq:            seq,
		Type:           cType,
		Content:        content,
		Status:         Normal,
		SenderID:       senderID,
		SendTime:       time.Now().UnixMilli(),
	}
}
