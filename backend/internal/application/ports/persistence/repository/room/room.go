package room

import (
	roomentity "IM_backend/internal/domain/room/entity"
	"context"
)

type RoomRepository interface {
	Create(domain *roomentity.Room) error
	WithTx(tx any) RoomRepository
	FindByID(ctx context.Context, roomID string) (*roomentity.Room, error)
	FindActiveRoom(roomId string, status int) (*roomentity.Room, error)
	IncrementMemberCount(ctx context.Context, roomId string, status int) (bool, error)
	DecrementMemberCount(ctx context.Context, roomId string, status int) (bool, error)
}
