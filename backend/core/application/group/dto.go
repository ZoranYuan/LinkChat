package group

import (
	domaingroup "IM_backend/core/domain/group"
)

type GroupAppDTO struct {
	GroupID      string
	Description string
	GroupName    string
	AvatarURL   string
	MemberCount int
	InviteCode  string
}

type GroupMemberDTO struct {
	Role     int
	JoinTime int64
}

func toGroupAppDTO(r *domaingroup.Group, inviteCode string) *GroupAppDTO {
	return &GroupAppDTO{
		GroupID:      r.GroupID,
		Description: r.Description,
		GroupName:    r.GroupName,
		AvatarURL:   r.Avatar,
		MemberCount: r.MemberCount,
		InviteCode:  inviteCode,
	}
}

func toGroupMemberDTO(r *domaingroup.GroupMember) *GroupMemberDTO {
	return &GroupMemberDTO{
		Role:     int(r.Role),
		JoinTime: r.JoinTime,
	}
}
