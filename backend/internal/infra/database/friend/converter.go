package infrafriend

import (
	domainfriend "IM_backend/internal/domain/friend"
)

func toDomain(m FriendModel) domainfriend.Friend {
	return domainfriend.Friend{
		UserID:       m.UserID,
		FriendUserID: m.FriendUserID,
		Status:       domainfriend.Status(m.Status),
		Remarks:      m.Remarks,
	}
}

func toModel(e domainfriend.Friend) FriendModel {
	return FriendModel{
		UserID:       e.UserID,
		FriendUserID: e.FriendUserID,
		Status:       int(e.Status),
		Remarks:      e.Remarks,
	}
}
