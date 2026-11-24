package domaingroup

import (
	"context"
)

type GroupRepository interface {
	Create(domain *Group) error
	WithTx(tx any) GroupRepository
	FindGroupByGroupID(ctx context.Context, groupID string) (*Group, error)
	ListGroupsByIDs(ctx context.Context, groupIDs []string) ([]*Group, error)
	FindGroupByGroupIDAndStatus(groupID string, status int) (*Group, error)
	IncrementMemberCount(ctx context.Context, groupID string, status int) (bool, error)
	DecrementMemberCount(ctx context.Context, groupID string, status int) (bool, error)
}
