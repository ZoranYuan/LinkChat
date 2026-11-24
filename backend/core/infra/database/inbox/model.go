package infrainbox

type InboxModel struct {
	ID          uint64 `gorm:"primaryKey;autoIncrement"`
	EventType   string `gorm:"size:64;not null;uniqueIndex:uq_inboxes_event,priority:1"`
	EventID     string `gorm:"size:255;not null;uniqueIndex:uq_inboxes_event,priority:2"`
	RequestHash string `gorm:"size:64;not null"`
}

func (InboxModel) TableName() string {
	return "inboxes"
}
