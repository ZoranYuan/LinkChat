package domainroom

import (
	"context"
)

type RoomMemberRepository interface {
	JoinRoom(*RoomMember) (*RoomMember, error)
	WithTx(any) RoomMemberRepository
	RejoinRoom(
		member *RoomMember,
	) error
	FindRoomMemberByUserIDAndRoomID(string, string) (*RoomMember, error)
	ListRoomMembersByRoomIDAndUserIDs(context.Context, string, []string) ([]*RoomMember, error)
	ListActiveRoomIDsByUserID(userID string) ([]string, error)
	LeaveRoom(*RoomMember, []int) error
}
