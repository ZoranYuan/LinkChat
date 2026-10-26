package infrafriend

import (
	domainfriend "IM_backend/internal/domain/friend"
)

func requestToDomain(m FriendRequestModel) *domainfriend.FriendRequest {
	return &domainfriend.FriendRequest{
		ApplicantUserID: m.ApplicantUserID,
		PeerUserID:      m.PeerUserID,
		RequestID:       m.RequestID,
		Message:         m.Message,
		Status:          domainfriend.RequestStatus(m.Status),
		ApplyTime:       m.ApplyTime,
	}
}

func requestToModel(e *domainfriend.FriendRequest) FriendRequestModel {
	return FriendRequestModel{
		ApplicantUserID: e.ApplicantUserID,
		RequestID:       e.RequestID,
		PeerUserID:      e.PeerUserID,
		Message:         e.Message,
		Status:          int(e.Status),
		ApplyTime:       e.ApplyTime,
	}
}
