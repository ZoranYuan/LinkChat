package infraroom

import (
	domainroom "IM_backend/internal/domain/room"
)

func memberToDomain(m RoomMemberModel) *domainroom.RoomMember {
	return &domainroom.RoomMember{
		UserID:    m.UserID,
		RoomID:    m.RoomID,
		Role:      domainroom.Role(m.Role),
		Status:    domainroom.RoomMemberStatus(m.Status),
		MuteUntil: m.MuteUntil,
		JoinTime:  m.JoinTime,
		Version:   m.Version,
	}
}

func memberToModel(e *domainroom.RoomMember) RoomMemberModel {
	return RoomMemberModel{
		UserID:    e.UserID,
		RoomID:    e.RoomID,
		Role:      int(e.Role),
		Status:    int(e.Status),
		MuteUntil: e.MuteUntil,
		JoinTime:  e.JoinTime,
		LeaveTime: e.LeaveTime,
		Version:   e.Version,
	}
}
