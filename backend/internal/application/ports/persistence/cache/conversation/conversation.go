package conversation

import (
	"context"
	"time"

	conversationentity "IM_backend/internal/domain/conversation/entity"
	conversationvo "IM_backend/internal/domain/conversation/value_object"
)

type ConversationInfo struct {
	ConversationID   string `json:"conversationId"`
	ConversationType int    `json:"convType"`
	UserID1          string `json:"userId1,omitempty"`
	UserID2          string `json:"userId2,omitempty"`
	RoomID           string `json:"roomId,omitempty"`
}

func FromConversation(record *conversationentity.Conversation) *ConversationInfo {
	info := &ConversationInfo{ConversationID: record.ConversationId, ConversationType: int(record.Convtype)}
	if record.Convtype == conversationvo.PrivateChat {
		info.UserID1, info.UserID2 = record.UserId1, record.UserId2
	} else {
		info.RoomID = record.RoomId
	}
	return info
}

// Conversation 仅转换会话身份关系，最新序号仍需从数据库读取。
func (info *ConversationInfo) Conversation() *conversationentity.Conversation {
	return &conversationentity.Conversation{ConversationId: info.ConversationID, Convtype: conversationvo.ConvType(info.ConversationType), UserId1: info.UserID1, UserId2: info.UserID2, RoomId: info.RoomID}
}

type ConversationCache interface {
	GetConversationInfo(ctx context.Context, conversationID string) (*ConversationInfo, bool, error)
	SetConversationInfo(ctx context.Context, info *ConversationInfo, ttl time.Duration) error
	SetConversationNotFound(ctx context.Context, conversationID string) error
	DeleteConversationInfo(ctx context.Context, conversationID string) error
}
