package infraroom

import (
	domainroom "IM_backend/internal/domain/room"
)

func toDomain(m RoomModel) *domainroom.Room {
	return &domainroom.Room{
		RoomID:      m.RoomID,
		OwnerUserID: m.OwnerUserID,
		Description: m.Description,
		RoomName:    m.RoomName,
		Status:      domainroom.RoomStatus(m.Status),
		Avatar:      m.Avatar,
		MemberCount: m.MemberCount,
		MaxMembers:  m.MaxMembers,
	}
}

func toModel(e *domainroom.Room) RoomModel {
	return RoomModel{
		RoomID:      e.RoomID,
		OwnerUserID: e.OwnerUserID,
		Description: e.Description,
		RoomName:    e.RoomName,
		Status:      int(e.Status),
		Avatar:      e.Avatar,
		MemberCount: e.MemberCount,
		MaxMembers:  e.MaxMembers,
	}
}
