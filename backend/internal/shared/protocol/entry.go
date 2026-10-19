package protocol

type AckStatus string

const (
	AckStatusSent      AckStatus = "sent"      // 服务端事务已提交
	AckStatusDelivered AckStatus = "delivered" // 至少一个目标在线 Connection 已接收
	AckStatusRead      AckStatus = "read"      // 对方读水位已越过该消息
	AckStatusFailed    AckStatus = "failed"    // 服务端未提交
)

type EventType string

type ConvType int

const (
	PrivateChat ConvType = 1
	RoomChat    ConvType = 2
)

const (
	EventFriendRequestCreated = "friend_request_created" // 好友申请提醒
	EventReadMessageAck       = "msg_read_ack"           // 前端 -> 后端
	EventRoomEntry            = "room_entry"              // 前端进入房间时请求固定个人未读快照
	EventConnectionVisibility = "connection_visibility"  // 前端上报当前页面可见状态
	EventReadMessageCommitted = "msg_read_committed"     // 后端内部 outbox/MQ
	EventReadMessageNotify    = "msg_read_notify"        // 发送给前端，表示已经有用户读取消息
	EventTypeMsgAck           = "msg_ack"                // 发送给前端，表示已经接收到消息
	EventTypeSendMessage      = "msg"                    // 后端发送给前端，表示新的消息
	EventRoomMessageNotice    = "room_msg_notice"        // 大群轻量新消息提醒，客户端按 seq 拉取详情
	EventRoomMemberChanged    = "room_member_changed"
	EventMessageBatch         = "msg_batch" // 服务端一次性发送多个数据
	EventTypeWSError          = "ws_error"
)

type MessageReadAckEvent struct {
	UserId         string   `json:"userId"`
	ConversationId string   `json:"conversationId"`
	LastReadSeq    int64    `json:"lastReadSeq"`
	ConvType       ConvType `json:"convType"`
	Avatar         string   `json:"avatar,omitempty"`
}

type MessageReadCommittedEvent struct {
	ReaderId        string   `json:"readerId"`
	RecipientUserId string   `json:"recipientUserId"`
	ConversationId  string   `json:"conversationId"`
	LastReadSeq     int64    `json:"lastReadSeq"`
	ConvType        ConvType `json:"convType"`
	Avatar          string   `json:"avatar,omitempty"`
}

type FriendRequestCreatedEvent struct {
	RequestId       string `json:"requestId"`
	ApplicantUserId string `json:"applicantUserId"`
	PeerUserId      string `json:"peerUserId"`
	Message         string `json:"message,omitempty"`
	ApplyTime       int64  `json:"applyTime"`
}

type MessageAckEvent struct {
	ClientMsgId    string    `json:"clientMsgId"`
	MessageId      string    `json:"messageId"`
	ConversationId string    `json:"conversationId,omitempty"`
	Seq            int64     `json:"seq,omitempty"`
	AttachmentId   string    `json:"attachmentId,omitempty"`
	Status         AckStatus `json:"status"`
	Extra          string    `json:"extra"`
	SendTime       int64     `json:"sendTime"`
}

type MessageNotifyEvent struct {
	ConversationId string `json:"conversationId"`
	MessageId      string `json:"messageId"`
	Seq            int64  `json:"seq"`
}

type RoomMemberChangedEvent struct {
	RoomID    string `json:"roomId"`
	UserID    string `json:"userId"`
	Status    int    `json:"status"`
	Role      int    `json:"role"`
	MuteUntil *int64 `json:"muteUntil,omitempty"`
	Version   int64  `json:"version"`
}

type MessageEvent struct {
	MessageId      string   `json:"messageId"`
	ConversationId string   `json:"conversationId"`
	SenderId       string   `json:"senderId"`
	SenderUsername string   `json:"senderUsername"`
	RecvId         string   `json:"recvId"`
	Seq            int64    `json:"seq"`
	ConvType       ConvType `json:"convType"`
	CType          int      `json:"cType"`
	Content        string   `json:"content"`
	SendTime       int64    `json:"sendTime"`
	ClientMsgId    string   `json:"clientMsgId"`
	Status         int8     `json:"status"`
	AttachmentId   string   `json:"attachmentId,omitempty"`
	Width          int      `json:"width,omitempty"`
	Height         int      `json:"height,omitempty"`
	DurationMs     *int64   `json:"durationMs,omitempty"`
	StickerId      string   `json:"stickerId,omitempty"`
	PackId         string   `json:"packId,omitempty"`
}

type WSErrorEvent struct {
	RequestOp string `json:"requestOp"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}
