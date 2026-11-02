package messaging

import (
	"context"
	"errors"
)

type IntegrationEvent struct {
	EventID      string
	Name         string
	PartitionKey string
	Payload      []byte
}

type Publisher interface {
	Publish(ctx context.Context, event IntegrationEvent) error
}

type IncomingEvent struct {
	EventID string
	Name    string
	Key     []byte
	Payload []byte
}

type Handler interface {
	Handle(ctx context.Context, event IncomingEvent) error
}

type NonRetryableError struct {
	err error
}

func (e *NonRetryableError) Error() string { return e.err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.err }

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &NonRetryableError{err: err}
}

func IsNonRetryable(err error) bool {
	var target *NonRetryableError
	return errors.As(err, &target)
}

type AckStatus string

const (
	AckStatusSent   AckStatus = "sent"   // 消息已成功投递到 Kafka
	AckStatusFailed AckStatus = "failed" // 消息未成功投递到 Kafka
)

type EventType string

type ConvType int

const (
	PrivateChat ConvType = 1
	RoomChat    ConvType = 2
)

const (
	TypeFriendRequestCreated = "friend_request_created" // 好友申请提醒
	TypeReadAck              = "msg_read_ack"           // 前端 -> 后端
	TypeRoomEntry            = "room_entry"             // 前端进入房间时请求固定个人未读快照
	TypeConnectionVisibility = "connection_visibility"  // 前端上报当前页面可见状态
	TypeReadNotify           = "msg_read_notify"        // 发送给前端，表示已经有用户读取消息
	TypeMessageAck           = "msg_ack"                // 发送给前端，表示已经接收到消息
	TypeMessage              = "msg"                    // 后端发送给前端，表示新的消息
	TypeRoomMessageNotice    = "room_msg_notice"        // 大型房间轻量新消息提醒，客户端按 seq 拉取详情
	TypeMessageBatch         = "msg_batch"              // 服务端一次性发送多个数据
	TypeWebSocketError       = "ws_error"
)

type MessageReadAckEvent struct {
	UserID         string   `json:"userId"`
	ConversationID string   `json:"conversationId"`
	LastReadSeq    int64    `json:"lastReadSeq"`
	ConvType       ConvType `json:"convType"`
	Avatar         string   `json:"avatar,omitempty"`
}

type FriendRequestCreatedEvent struct {
	RequestID       string `json:"requestId"`
	ApplicantUserID string `json:"applicantUserId"`
	PeerUserID      string `json:"peerUserId"`
	Message         string `json:"message,omitempty"`
	ApplyTime       int64  `json:"applyTime"`
}

type MessageAckEvent struct {
	ClientMsgID    string    `json:"clientMsgId"`
	MessageID      string    `json:"messageId"`
	ConversationID string    `json:"conversationId,omitempty"`
	Seq            int64     `json:"seq,omitempty"`
	AttachmentID   string    `json:"attachmentId,omitempty"`
	Status         AckStatus `json:"status"`
	Extra          string    `json:"extra"`
	SendTime       int64     `json:"sendTime"`
}

type MessageNotifyEvent struct {
	ConversationID string `json:"conversationId"`
	MessageID      string `json:"messageId"`
	Seq            int64  `json:"seq"`
}

type MessageEvent struct {
	MessageID      string   `json:"messageId"`
	ConversationID string   `json:"conversationId"`
	SenderID       string   `json:"senderId"`
	RecvID         string   `json:"recvId"`
	Seq            int64    `json:"seq"`
	ConvType       ConvType `json:"convType"`
	MessageType    int      `json:"cType"`
	Content        string   `json:"content"`
	SendTime       int64    `json:"sendTime"`
	ClientMsgID    string   `json:"clientMsgId"`
	Status         int8     `json:"status"`
	AttachmentID   string   `json:"attachmentId,omitempty"`
	Width          int      `json:"width,omitempty"`
	Height         int      `json:"height,omitempty"`
	DurationMs     *int64   `json:"durationMs,omitempty"`
	StickerID      string   `json:"stickerId,omitempty"`
	PackID         string   `json:"packId,omitempty"`
}

type WSErrorEvent struct {
	RequestOp string `json:"requestOp"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

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
	Width            int
	Height           int
	DurationMs       *int64
	StickerID        string
	PackID           string
}
