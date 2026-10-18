package model

import "time"

type OutboxRecord struct {
	EventID      string     `json:"eventId" gorm:"size:32;primaryKey"`
	EventType    string     `json:"eventType" gorm:"size:64;index"`
	PartitionKey string     `json:"partitionKey" gorm:"size:128"`
	Payload      []byte     `json:"payload" gorm:"type:longblob"`
	Status       string     `json:"status" gorm:"size:16;not null;default:'pending';index:idx_outbox_pending,priority:1;index:idx_outbox_processing,priority:1"`
	RetryCount   int        `json:"retryCount" gorm:"not null;default:0"`
	NextRetryAt  time.Time  `json:"nextRetryAt" gorm:"not null;default:'1970-01-01 00:00:00';index:idx_outbox_pending,priority:2"`
	LockedAt     *time.Time `json:"lockedAt" gorm:"index:idx_outbox_processing,priority:2"`
	LockToken    string     `json:"-" gorm:"size:64;not null;default:'';index:idx_outbox_processing,priority:3"`
	LastError    string     `json:"lastError" gorm:"type:text"`
	SentAt       *time.Time `json:"sentAt" gorm:"index"`
	CreatedAt    time.Time  `json:"createdAt" gorm:"index:idx_outbox_pending,priority:3"`
	UpdatedAt    time.Time  `json:"updatedAt"`
}

func (OutboxRecord) TableName() string {
	return "outboxes"
}
