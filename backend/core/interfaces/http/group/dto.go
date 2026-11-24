package group

type CreateGroupRequest struct {
	Description string `json:"description"`
	GroupName    string `json:"groupName" binding:"required"`
	AvatarURL   string `json:"avatar"`
}

type CreateGroupResponse struct {
	GroupID      string `json:"groupId"`
	Description string `json:"description"`
	GroupName    string `json:"groupName"`
	AvatarURL   string `json:"avatar"`
	MemberCount int    `json:"memberCount"`
	InviteCode  string `json:"inviteCode"`
}

type JoinGroupRequest struct {
	InviteCode string `json:"inviteCode" binding:"required"`
}

type JoinGroupResponse struct {
	GroupID         string `json:"groupId"`
	Description    string `json:"description"`
	GroupName       string `json:"groupName"`
	AvatarURL      string `json:"avatar"`
	MemberCount    int    `json:"memberCount"`
	Role           int    `json:"role"`
	JoinTime       int64  `json:"joinTime"`
	ConversationID string `json:"conversationId"`
	LatestSeq      int64  `json:"latestSeq"`
}
