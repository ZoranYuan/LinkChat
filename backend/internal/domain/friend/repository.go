package domainfriend

import (
	"context"
)

type FriendRepository interface {
	Create(domains []Friend) error
	FindFriendByUserIDAndFriendUserID(userID, friendID string) (*Friend, error)
	ListFriendsByUserPairs(context.Context, []RelationPair) ([]*Friend, error)
	ListFriendsByUserIDAndFriendUserIDs(
		ctx context.Context,
		userID string,
		friendIDs []string,
	) ([]*Friend, error)
	WithTx(tx any) FriendRepository
	GetUserFriendList(userID string, status int) ([]Friend, error)
}

type RelationPair struct {
	UserID       string
	FriendUserID string
}
