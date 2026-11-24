package infragroup

import (
	domaingroup "IM_backend/core/domain/group"
)

func toDomain(m GroupModel) *domaingroup.Group {
	return &domaingroup.Group{
		GroupID:      m.GroupID,
		OwnerUserID: m.OwnerUserID,
		Description: m.Description,
		GroupName:    m.GroupName,
		Status:      domaingroup.GroupStatus(m.Status),
		Avatar:      m.Avatar,
		MemberCount: m.MemberCount,
		MaxMembers:  m.MaxMembers,
	}
}

func toModel(e *domaingroup.Group) GroupModel {
	return GroupModel{
		GroupID:      e.GroupID,
		OwnerUserID: e.OwnerUserID,
		Description: e.Description,
		GroupName:    e.GroupName,
		Status:      int(e.Status),
		Avatar:      e.Avatar,
		MemberCount: e.MemberCount,
		MaxMembers:  e.MaxMembers,
	}
}
