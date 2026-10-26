package conversation

import (
	domainmessage "IM_backend/internal/domain/message"
)

type PeerUserDTO struct {
	UserID    string `json:"userId"`
	Username  string `json:"userName"`
	Nickname  string `json:"nickName"`
	Remark    string `json:"remark"`
	AvatarURL string `json:"avatar"`
}

type RoomDTO struct {
	RoomID      string `json:"roomId"`
	RoomName    string `json:"roomName"`
	AvatarURL   string `json:"avatar"`
	Description string `json:"description,omitempty"`
	MemberCount int    `json:"memberCount,omitempty"`
}

type LatestMessageDTO struct {
	MessageID      string                    `json:"messageId"`
	ConversationID string                    `json:"conversationId"`
	SenderID       string                    `json:"senderId"`
	Seq            int64                     `json:"seq"`
	Type           domainmessage.MessageType `json:"cType"`
	Content        string                    `json:"content"`
	SendTime       int64                     `json:"sendTime"`
}

type ConversationItemDTO struct {
	ConversationID   string            `json:"conversationId"`
	ConversationType int8              `json:"convType"`
	TargetID         string            `json:"targetId"`
	DisplayName      string            `json:"displayName"`
	AvatarURL        string            `json:"avatar"`
	Unread           int64             `json:"unread"`
	LatestSeq        int64             `json:"latestSeq"`
	LastMessage      *LatestMessageDTO `json:"lastMessage,omitempty"`

	PeerUser *PeerUserDTO `json:"peerUser,omitempty"`
	Room     *RoomDTO     `json:"room,omitempty"`

	IsMuted bool `json:"isMuted"`
}
