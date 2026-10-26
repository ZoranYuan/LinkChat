package friend

import (
	domainfriend "IM_backend/internal/domain/friend"
)

type FriendAppDTO struct {
	FriendUserID    string
	FriendAvatarURL string
	FriendUsername  string
	FriendNickname  string
	Status          int
	// LastMessage    string
	Remarks string
}

type FriendRequestDTO struct {
	RequestID         string
	ApplicantUserID   string
	ApplicantNickName string
	Message           string
	Status            int
	ApplyTime         int64
}

func toFriendRequestDTO(fr *domainfriend.FriendRequest) FriendRequestDTO {
	return FriendRequestDTO{
		RequestID:       fr.RequestID,
		ApplicantUserID: fr.ApplicantUserID,
		Message:         fr.Message,
		Status:          int(fr.Status),
		ApplyTime:       fr.ApplyTime,
	}
}
