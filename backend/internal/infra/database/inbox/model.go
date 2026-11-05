package infrainbox

import (
	"time"
)

type InboxModel struct {
	EventID      string `gorm:"type:bigint;primaryKey"`
	EventType    string `gorm:"size:64;not null"`
	Status       string `gorm:"size:16;not null;default:'processing'"`
	LockedAt     *time.Time
	LockToken    string `gorm:"size:64;not null;default:''"`
	AttemptCount int    `gorm:"not null;default:0"`
	LastError    string `gorm:"type:text"`
	FinishedAt   *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (InboxModel) TableName() string {
	return "inboxes"
}
