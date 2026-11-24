package infragroup

import (
	domaingroup "IM_backend/core/domain/group"
)

func memberToDomain(m GroupMemberModel) *domaingroup.GroupMember {
	return &domaingroup.GroupMember{
		UserID:    m.UserID,
		GroupID:    m.GroupID,
		Role:      domaingroup.Role(m.Role),
		Status:    domaingroup.GroupMemberStatus(m.Status),
		MuteUntil: m.MuteUntil,
		JoinTime:  m.JoinTime,
		Version:   m.Version,
	}
}

func memberToModel(e *domaingroup.GroupMember) GroupMemberModel {
	return GroupMemberModel{
		UserID:    e.UserID,
		GroupID:    e.GroupID,
		Role:      int(e.Role),
		Status:    int(e.Status),
		MuteUntil: e.MuteUntil,
		JoinTime:  e.JoinTime,
		LeaveTime: e.LeaveTime,
		Version:   e.Version,
	}
}
