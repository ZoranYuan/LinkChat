package domainroom

type Room struct {
	RoomID      string     `json:"roomId"`
	OwnerUserID string     `json:"ownerUserId"`
	Description string     `json:"description"`
	RoomName    string     `json:"roomName"`
	Status      RoomStatus `json:"status"`
	Avatar      string     `json:"avatar"`
	MemberCount int        `json:"memberCount"`
	MaxMembers  int        `json:"maxMembers"`
}

func NewRoom(
	roomID string,
	ownerUserID string,
	description string,
	roomName string,
	avatar string,
	maxMembers int,
) (*Room, error) {
	if roomName == "" {
		return nil, ErrRoomNameRequired
	}

	return &Room{
		RoomID:      roomID,
		OwnerUserID: ownerUserID,
		Description: description,
		RoomName:    roomName,
		Status:      Normal,
		Avatar:      avatar,
		MemberCount: 1,
		MaxMembers:  maxMembers,
	}, nil
}

func (r *Room) Invite() bool {
	if r.Status != Normal {
		return false
	}

	return true
}
