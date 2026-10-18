package outbox

import (
	"errors"
	"time"
)

const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusSent       = "sent"
	StatusDead       = "dead"
)

var ErrLeaseLost = errors.New("outbox lease lost")

type Entry struct {
	EventID      string
	EventType    string
	PartitionKey string
	Payload      []byte
	Status       string
	RetryCount   int
	NextRetryAt  time.Time
	LockedAt     *time.Time
	LockToken    string
	LastError    string
	SentAt       *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
