package infrafriend

import (
	"time"
)

type FriendModel struct {
	UserID       string `json:"userId" gorm:"type:bigint;not null;primaryKey;comment:用户ID"`
	FriendUserID string `json:"friendUserId" gorm:"type:bigint;not null;primaryKey;comment:好友ID"`
	Remarks      string `json:"remarks" gorm:"size:64;comment:好友备注"`
	Status       int    `json:"status" gorm:"tinyInt;comment:好友状态(0-好友，1-拉黑对方，2-被对方拉黑，3-删除, 4-待同意，5-已申请)"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (u *FriendModel) TableName() string {
	return "friendships"
}

type FriendRequestModel struct {
	RequestID       string `json:"requestId" gorm:"type:bigint;not null;uniqueIndex:uk_friend_request_id"`
	ApplicantUserID string `json:"applicantUserId" gorm:"type:bigint;not null;primaryKey"`
	PeerUserID      string `json:"peerUserId" gorm:"type:bigint;not null;primaryKey;index:idx_friend_request_peer_updated,priority:1"`
	Status          int    `json:"status" gorm:"tinyInt;default:0;comment:1-待处理，2-已同意，3-已拒绝"`
	Message         string `gorm:"size:128"`
	ApplyTime       int64
	CreatedAt       time.Time
	UpdatedAt       time.Time `gorm:"index:idx_friend_request_peer_updated,priority:2"`
}

func (FriendRequestModel) TableName() string {
	return "friend_requests"
}
