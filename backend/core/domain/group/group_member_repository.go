package domaingroup

import (
	"context"
)

type GroupMemberRepository interface {
	JoinGroup(*GroupMember) (*GroupMember, error)
	WithTx(any) GroupMemberRepository
	RejoinGroup(
		member *GroupMember,
	) error
	FindGroupMemberByUserIDAndGroupID(string, string) (*GroupMember, error)
	ListGroupMembersByGroupIDAndUserIDs(context.Context, string, []string) ([]*GroupMember, error)
	ListActiveGroupIDsByUserID(userID string) ([]string, error)
	LeaveGroup(*GroupMember, []int) error
}
