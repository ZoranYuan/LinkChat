package room

import (
	domainroom "IM_backend/internal/domain/room"
)

type RoomAppDTO struct {
	RoomID      string
	Description string
	RoomName    string
	AvatarURL   string
	MemberCount int
	InviteCode  string
}

type RoomMemberDTO struct {
	Role     int
	JoinTime int64
}

func toRoomAppDTO(r *domainroom.Room, inviteCode string) *RoomAppDTO {
	return &RoomAppDTO{
		RoomID:      r.RoomID,
		Description: r.Description,
		RoomName:    r.RoomName,
		AvatarURL:   r.Avatar,
		MemberCount: r.MemberCount,
		InviteCode:  inviteCode,
	}
}

func toRoomMemberDTO(r *domainroom.RoomMember) *RoomMemberDTO {
	return &RoomMemberDTO{
		Role:     int(r.Role),
		JoinTime: r.JoinTime,
	}
}
