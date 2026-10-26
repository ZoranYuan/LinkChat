package room

type CreateRoomRequest struct {
	Description string `json:"description"`
	RoomName    string `json:"roomName" binding:"required"`
	AvatarURL   string `json:"avatar"`
}

type CreateRoomResponse struct {
	RoomID      string `json:"roomId"`
	Description string `json:"description"`
	RoomName    string `json:"roomName"`
	AvatarURL   string `json:"avatar"`
	MemberCount int    `json:"memberCount"`
	InviteCode  string `json:"inviteCode"`
}

type JoinRoomRequest struct {
	InviteCode string `json:"inviteCode" binding:"required"`
}

type JoinRoomResponse struct {
	RoomID         string `json:"roomId"`
	Description    string `json:"description"`
	RoomName       string `json:"roomName"`
	AvatarURL      string `json:"avatar"`
	MemberCount    int    `json:"memberCount"`
	Role           int    `json:"role"`
	JoinTime       int64  `json:"joinTime"`
	ConversationID string `json:"conversationId"`
	LatestSeq      int64  `json:"latestSeq"`
}
