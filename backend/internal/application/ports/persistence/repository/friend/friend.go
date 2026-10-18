package friend

import (
	friendentity "IM_backend/internal/domain/friend/entity"
	"context"
)

type FriendRepository interface {
	Create(domains []friendentity.Friend) error
	FindRelation(userId, friendId string) (*friendentity.Friend, error)
	FindRelationsByPairs(context.Context, []RelationPair) ([]*friendentity.Friend, error)
	FindRelations(
		ctx context.Context,
		userId string,
		friendIds []string,
	) ([]*friendentity.Friend, error)
	WithTx(tx any) FriendRepository
	GetUserFriendList(userId string, status int) ([]friendentity.Friend, error)
}

type RelationPair struct {
	UserID       string
	FriendUserID string
}
