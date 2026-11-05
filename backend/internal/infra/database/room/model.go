package infraroom

import (
	"time"
)

type RoomModel struct {
	RoomID      string `json:"userId" gorm:"type:bigint;primaryKey;not null"`
	OwnerUserID string `json:"ownerUserId" gorm:"type:bigint;not null"`
	Description string `json:"description" gorm:"comment:房间介绍"`
	RoomName    string `json:"roomName" gorm:"size:32;comment:房间名称"`
	Status      int    `json:"status" gorm:"tinyInt;comment:1为正常，0为暂时不可用"`
	Avatar      string `json:"avatar" gorm:"size:255;comment:房间头像URL"`
	MemberCount int    `json:"memberCount" gorm:"type:int;default:0;comment:房间人数"`
	MaxMembers  int    `json:"maxUsers" gorm:"not null;comment:房间最大人数"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (u *RoomModel) TableName() string {
	return "rooms"
}

type RoomMemberModel struct {
	UserID    string `json:"userId" gorm:"type:bigint;primaryKey;not null;comment:用户ID"`
	RoomID    string `json:"roomId" gorm:"type:bigint;index:idx_room_members_room_id;primaryKey;not null"`
	Role      int    `gorm:"tinyInt;default:0;comment:1普通用户,2管理员,3房主"`
	Status    int    `json:"status" gorm:"tinyInt;comment: 成员状态"`
	MuteUntil *int64 `gorm:"column:mute_util"` // 禁言到什么时候（时间戳）
	JoinTime  int64
	LeaveTime *int64
	Version   int64 `gorm:"not null;default:1"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (u *RoomMemberModel) TableName() string {
	return "room_members"
}
