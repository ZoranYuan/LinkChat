package friend

import (
	friendapp "IM_backend/internal/application/friend"
)

type FriendItemResponse struct {
	FriendUserID    string `json:"friendUserId"`
	FriendUsername  string `json:"friendUsername"`
	FriendAvatarURL string `json:"friendAvatar"`
	Status          int    `json:"status"`
	DisplayName     string `json:"displayName"`
}

func toFriendListResponse(f []friendapp.FriendAppDTO) []FriendItemResponse {
	var res = make([]FriendItemResponse, 0, len(f))

	for _, i := range f {
		var displayName string
		if i.Remarks != "" {
			displayName = i.Remarks
		} else if i.FriendNickname != "" {
			displayName = i.FriendNickname
		} else {
			displayName = i.FriendUsername
		}

		res = append(res, FriendItemResponse{
			FriendUserID:    i.FriendUserID,
			FriendUsername:  i.FriendUsername,
			FriendAvatarURL: i.FriendAvatarURL,
			DisplayName:     displayName,
			Status:          i.Status,
		})
	}

	return res
}
