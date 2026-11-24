package domaingroup

type Group struct {
	GroupID      string     `json:"groupId"`
	OwnerUserID string     `json:"ownerUserId"`
	Description string     `json:"description"`
	GroupName    string     `json:"groupName"`
	Status      GroupStatus `json:"status"`
	Avatar      string     `json:"avatar"`
	MemberCount int        `json:"memberCount"`
	MaxMembers  int        `json:"maxMembers"`
}

func NewGroup(
	groupID string,
	ownerUserID string,
	description string,
	groupName string,
	avatar string,
	maxMembers int,
) (*Group, error) {
	if groupName == "" {
		return nil, ErrGroupNameRequired
	}

	return &Group{
		GroupID:      groupID,
		OwnerUserID: ownerUserID,
		Description: description,
		GroupName:    groupName,
		Status:      Normal,
		Avatar:      avatar,
		MemberCount: 1,
		MaxMembers:  maxMembers,
	}, nil
}

func (r *Group) Invite() bool {
	if r.Status != Normal {
		return false
	}

	return true
}
