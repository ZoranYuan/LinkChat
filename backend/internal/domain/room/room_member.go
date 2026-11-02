package domainroom

import (
	"time"
)

type RoomMember struct {
	UserID    string `json:"UserId"`
	RoomID    string `json:"RoomId"`
	Role      Role
	Status    RoomMemberStatus
	MuteUntil *int64 `json:"MuteUtil"`
	JoinTime  int64
	LeaveTime *int64

	// 标识用户在某个房间中的成员状态版本
	Version int64
}

func NewRoomMember(userID, roomID string, role Role) *RoomMember {
	return &RoomMember{
		UserID:    userID,
		RoomID:    roomID,
		Role:      role,
		Status:    Active,
		MuteUntil: nil,
		JoinTime:  time.Now().UnixMilli(),
		Version:   1,
	}
}

func (member *RoomMember) Invite() error {
	if member.Status != Active {
		return ErrPermissionDenied
	}

	return nil
}

func (member *RoomMember) Join() {
	member.Status = Active
	member.JoinTime = time.Now().UnixMilli()
	member.MuteUntil = nil
	member.LeaveTime = nil
}

func (member *RoomMember) ReJoin() error {
	if member.Status != Left {
		return ErrDuplicateJoin
	}

	member.Status = Active
	member.JoinTime = time.Now().UnixMilli()
	member.MuteUntil = nil
	member.LeaveTime = nil
	return nil
}

func (member *RoomMember) Leave() error {
	if member.Status != Muted && member.Status != Active {
		return ErrDuplicateLeave
	}
	member.Status = Left
	now := time.Now().UnixMilli()
	member.LeaveTime = &now

	return nil
}
