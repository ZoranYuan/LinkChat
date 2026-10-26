package conversation

import (
	"context"
	"time"

	domainconversation "IM_backend/internal/domain/conversation"
)

type ConversationInfo struct {
	ConversationID   string `json:"conversationId"`
	ConversationType int    `json:"convType"`
	UserID1          string `json:"userId1,omitempty"`
	UserID2          string `json:"userId2,omitempty"`
	RoomID           string `json:"roomId,omitempty"`
}

func FromConversation(record *domainconversation.Conversation) *ConversationInfo {
	info := &ConversationInfo{ConversationID: record.ConversationID, ConversationType: int(record.ConvType)}
	if record.ConvType == domainconversation.PrivateChat {
		info.UserID1, info.UserID2 = record.UserID1, record.UserID2
	} else {
		info.RoomID = record.RoomID
	}
	return info
}

type ConversationCache interface {
	GetConversationInfo(ctx context.Context, conversationID string) (*ConversationInfo, bool, error)
	SetConversationInfo(ctx context.Context, info *ConversationInfo, ttl time.Duration) error
	SetConversationNotFound(ctx context.Context, conversationID string) error
}
