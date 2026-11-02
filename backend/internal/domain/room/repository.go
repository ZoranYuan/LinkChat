package domainroom

import (
	"context"
)

type RoomRepository interface {
	Create(domain *Room) error
	WithTx(tx any) RoomRepository
	FindRoomByRoomID(ctx context.Context, roomID string) (*Room, error)
	ListRoomsByIDs(ctx context.Context, roomIDs []string) ([]*Room, error)
	FindRoomByRoomIDAndStatus(roomID string, status int) (*Room, error)
	IncrementMemberCount(ctx context.Context, roomID string, status int) (bool, error)
	DecrementMemberCount(ctx context.Context, roomID string, status int) (bool, error)
}
