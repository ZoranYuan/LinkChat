package events

import (
	"context"
	"errors"

	domainconversation "IM_backend/core/domain/conversation"
)

type PublishMessage struct {
	Topic        string
	EventType    string
	EventID      string
	PartitionKey string
	Payload      []byte
}

type Publisher interface {
	Publish(ctx context.Context, topic string, eventType, eventID, partitionKey string, payload []byte) error
	PublishBatch(ctx context.Context, msgs []PublishMessage) error
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

const (
	TypeFriendRequestNotice  = "ws:friend_request_notice" // 好友申请提醒
	TypeMessageRead          = "ws:message_read"          // 前端 -> 后端
	TypeGroupEnter           = "ws:group_enter"           // 前端进入群聊时请求固定个人未读快照
	TypeConnectionVisibility = "ws:connection_visibility" // 前端上报当前页面可见状态
	TypeMessageReadNotice    = "ws:message_read_notice"   // 发送给前端，表示已经有用户读取消息
	TypeMessageSendAck       = "ws:message_send_ack"      // 消息发送状态 ACK，客户端据此开始轮询
	TypeMessageSend          = "ws:message_send"          // 客户端请求发送消息
	TypeMessagePush          = "ws:message_push"          // 向客户端推送已持久化消息
	TypeEventBatchAck        = "ws:event_batch_ack"       // 客户端确认整个事件批次
	TypeGroupMessageNotice   = "ws:group_message_notice"  // 大型群聊轻量新消息提醒，客户端按 seq 拉取详情
	TypeEventBatch           = "ws:event_batch"           // 服务端一次性发送多个数据
	TypeErrorNotice          = "ws:error_notice"
)

const (
	KafkaEventMessageSend = "kafka:message_send"
	KafkaEventReadReceipt = "kafka:read_receipt"
)

type MessageReadNoticeEvent struct {
	UserID         string                      `json:"userId"`
	ConversationID string                      `json:"conversationId"`
	LastReadSeq    int64                       `json:"lastReadSeq"`
	ConvType       domainconversation.ConvType `json:"convType"`
	Avatar         string                      `json:"avatar,omitempty"`
}

type FriendRequestNoticeEvent struct {
	RequestID       string `json:"requestId"`
	ApplicantUserID string `json:"applicantUserId"`
	PeerUserID      string `json:"peerUserId"`
	Message         string `json:"message,omitempty"`
	ApplyTime       int64  `json:"applyTime"`
}

type GroupMessageNoticeEvent struct {
	ConversationID string `json:"conversationId"`
	Seq            int64  `json:"seq"`
}

type MessagePushEvent struct {
	MessageID      string                      `json:"messageId"`
	ConversationID string                      `json:"conversationId"`
	SenderID       string                      `json:"senderId"`
	RecvID         string                      `json:"recvId"`
	Seq            int64                       `json:"seq"`
	ConvType       domainconversation.ConvType `json:"convType"`
	MessageType    int                         `json:"cType"`
	Content        string                      `json:"content"`
	SendTime       int64                       `json:"sendTime"`
	ClientMsgID    string                      `json:"clientMsgId"`
	Status         int8                        `json:"status"`
	AttachmentID   string                      `json:"attachmentId,omitempty"`
	StickerID      string                      `json:"stickerId,omitempty"`
	PackID         string                      `json:"packId,omitempty"`
}

type ReadReceiptEvent struct {
	UserID    string `json:"userId"`    // 读者，来自 WS connection
	MessageID string `json:"messageId"` // 连续已读边界那条消息
}
