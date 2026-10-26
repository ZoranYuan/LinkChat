package infraconversation

import (
	"time"
)

type ConversationModel struct {
	ConversationID  string    `json:"conversationId" gorm:"size:64;not null;primaryKey"`
	ConvType        int8      `json:"convType" gorm:"column:convtype;tinyInt;not null;comment:(私聊 1/房间 2)"`
	UserID1         string    `json:"userId1" gorm:"size:32;not null"`
	UserID2         string    `json:"userId2" gorm:"size:32;not null"`
	RoomID          string    `json:"roomId" gorm:"size:32;not null"`
	LatestMessageID string    `json:"latestMessageId" gorm:"size:32;index"`
	LatestSeq       int64     `json:"latestSeq" gorm:"not null;default:0"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

func (ConversationModel) TableName() string { return "conversations" }

type UserConversationModel struct {
	UserID                string `json:"userId" gorm:"size:32;primaryKey"`
	ConversationID        string `json:"conversationId" gorm:"size:64;primaryKey"`
	LastReadSeq           int64  `json:"lastReadSeq" gorm:"not null;default:0"`
	SummaryUnreadBeginSeq int64  `json:"summaryUnreadBeginSeq" gorm:"not null;default:0"`
	SummaryUnreadEndSeq   int64  `json:"summaryUnreadEndSeq" gorm:"not null;default:0"`

	IsMuted   bool      `json:"isMuted" gorm:"not null;default:false"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func (UserConversationModel) TableName() string { return "user_conversations" }
