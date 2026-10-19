package model

import "time"

type UserConversation struct {
	UserId                string `json:"userId" gorm:"size:32;primaryKey"`
	ConversationId        string `json:"conversationId" gorm:"size:64;primaryKey"`
	LastReadSeq           int64  `json:"lastReadSeq" gorm:"not null;default:0"`
	SummaryUnreadBeginSeq int64  `json:"summaryUnreadBeginSeq" gorm:"not null;default:0"`
	SummaryUnreadEndSeq   int64  `json:"summaryUnreadEndSeq" gorm:"not null;default:0"`

	IsMuted   bool      `json:"isMuted" gorm:"not null;default:false"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
