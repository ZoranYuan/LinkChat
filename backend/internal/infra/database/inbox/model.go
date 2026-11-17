package infrainbox

import "time"

type InboxModel struct {
	ID          uint64 `gorm:"primaryKey;autoIncrement"`
	SenderID    string `gorm:"type:bigint;not null;uniqueIndex:uq_inboxes_sender_client,priority:1;index:idx_inboxes_lock_token_key,priority:2"`
	ClientMsgID string `gorm:"size:64;not null;uniqueIndex:uq_inboxes_sender_client,priority:2;index:idx_inboxes_lock_token_key,priority:3"`
	RequestHash string `gorm:"size:64;not null"`
	Status      string `gorm:"size:16;not null;default:'processing'"`
	LockToken   string `gorm:"size:64;not null;default:'';index:idx_inboxes_lock_token_key,priority:1"`
	LockedAt    *time.Time
	LastError   string `gorm:"type:text"`
}

func (InboxModel) TableName() string {
	return "inboxes"
}
