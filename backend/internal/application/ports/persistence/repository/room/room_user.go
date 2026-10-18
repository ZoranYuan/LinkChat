package room

import (
	"context"

	roomentity "IM_backend/internal/domain/room/entity"
)

type RoomUserRepository interface {
	JoinRoom(*roomentity.RoomUser) (*roomentity.RoomUser, error)
	WithTx(any) RoomUserRepository
	RejoinRoom(
		member *roomentity.RoomUser,
	) error
	GetRelationByIDs(string, string) (*roomentity.RoomUser, error)
	ListRelationsByRoomAndUserIDs(context.Context, string, []string) ([]*roomentity.RoomUser, error)
	ListActiveUserIDs(roomId string) ([]string, error)
	ListActiveRoomIDs(userId string) ([]string, error)
	LeaveRoom(*roomentity.RoomUser, []int) error
}
