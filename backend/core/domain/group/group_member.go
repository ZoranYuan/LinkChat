package domaingroup

import (
	"time"
)

type GroupMember struct {
	UserID    string `json:"UserId"`
	GroupID    string `json:"GroupId"`
	Role      Role
	Status    GroupMemberStatus
	MuteUntil *int64 `json:"MuteUtil"`
	JoinTime  int64
	LeaveTime *int64

	// 标识用户在某个群聊中的成员状态版本
	Version int64
}

func NewGroupMember(userID, groupID string, role Role) *GroupMember {
	return &GroupMember{
		UserID:    userID,
		GroupID:    groupID,
		Role:      role,
		Status:    Active,
		MuteUntil: nil,
		JoinTime:  time.Now().UnixMilli(),
		Version:   1,
	}
}

func (member *GroupMember) Invite() error {
	if member.Status != Active {
		return ErrPermissionDenied
	}

	return nil
}

func (member *GroupMember) Join() {
	member.Status = Active
	member.JoinTime = time.Now().UnixMilli()
	member.MuteUntil = nil
	member.LeaveTime = nil
}

func (member *GroupMember) ReJoin() error {
	if member.Status != Left {
		return ErrDuplicateJoin
	}

	member.Status = Active
	member.JoinTime = time.Now().UnixMilli()
	member.MuteUntil = nil
	member.LeaveTime = nil
	return nil
}

func (member *GroupMember) Leave() error {
	if member.Status != Muted && member.Status != Active {
		return ErrDuplicateLeave
	}
	member.Status = Left
	now := time.Now().UnixMilli()
	member.LeaveTime = &now

	return nil
}
