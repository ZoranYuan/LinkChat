package infragroup

import (
	"time"
)

type GroupModel struct {
	GroupID      string `json:"userId" gorm:"type:bigint;primaryKey;not null"`
	OwnerUserID string `json:"ownerUserId" gorm:"type:bigint;not null"`
	Description string `json:"description" gorm:"comment:群聊介绍"`
	GroupName    string `json:"groupName" gorm:"size:32;comment:群聊名称"`
	Status      int    `json:"status" gorm:"tinyInt;comment:1为正常，0为暂时不可用"`
	Avatar      string `json:"avatar" gorm:"size:255;comment:群聊头像URL"`
	MemberCount int    `json:"memberCount" gorm:"type:int;default:0;comment:群聊人数"`
	MaxMembers  int    `json:"maxUsers" gorm:"not null;comment:群聊最大人数"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (u *GroupModel) TableName() string {
	return "groups"
}

type GroupMemberModel struct {
	UserID    string `json:"userId" gorm:"type:bigint;primaryKey;not null;comment:用户ID"`
	GroupID    string `json:"groupId" gorm:"type:bigint;index:idx_group_members_group_id;primaryKey;not null"`
	Role      int    `gorm:"tinyInt;default:0;comment:1普通用户,2管理员,3群主"`
	Status    int    `json:"status" gorm:"tinyInt;comment: 成员状态"`
	MuteUntil *int64 `gorm:"column:mute_util"` // 禁言到什么时候（时间戳）
	JoinTime  int64
	LeaveTime *int64
	Version   int64 `gorm:"not null;default:1"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (u *GroupMemberModel) TableName() string {
	return "group_members"
}
