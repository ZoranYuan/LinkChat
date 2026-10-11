package room

import (
	roomrepo "IM_backend/internal/application/ports/persistence/repository/room"
	roomentity "IM_backend/internal/domain/room/entity"
	"IM_backend/internal/infrastructure/persistence/mysql/model"
	"context"
	"errors"

	"gorm.io/gorm"
)

type RoomRepository struct {
	db *gorm.DB
}

func NewRoomRepository(db *gorm.DB) *RoomRepository {
	return &RoomRepository{
		db: db,
	}
}

func (rr *RoomRepository) Create(domain *roomentity.Room) error {
	model := toModel(domain)

	return rr.db.Create(&model).Error
}

func (rr *RoomRepository) FindActiveRoom(roomId string, status int) (*roomentity.Room, error) {
	var m model.Room

	if err := rr.db.Where("room_id = ? AND status = ?", roomId, status).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, roomentity.ErrRoomNotFound
		}

		return nil, err
	}

	return toDomain(m), nil
}

func (rr *RoomRepository) FindByID(ctx context.Context, roomID string) (*roomentity.Room, error) {
	var m model.Room
	if err := rr.db.WithContext(ctx).Where("room_id = ?", roomID).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, roomentity.ErrRoomNotFound
		}
		return nil, err
	}
	return toDomain(m), nil
}

func (rr *RoomRepository) IncrementMemberCount(ctx context.Context, roomId string, status int) (bool, error) {
	result := rr.db.WithContext(ctx).
		Model(&model.Room{}).
		Where("room_id = ? AND status = ? AND (max_members <= 0 OR member_count < max_members)", roomId, status).
		UpdateColumn("member_count", gorm.Expr("member_count + 1"))
	return result.RowsAffected == 1, result.Error
}

func (rr *RoomRepository) DecrementMemberCount(ctx context.Context, roomId string, status int) (bool, error) {
	result := rr.db.WithContext(ctx).
		Model(&model.Room{}).
		Where("room_id = ? AND status = ? AND member_count > 0", roomId, status).
		UpdateColumn("member_count", gorm.Expr("member_count - 1"))
	return result.RowsAffected == 1, result.Error
}

func (rr *RoomRepository) WithTx(tx any) roomrepo.RoomRepository {
	return &RoomRepository{
		db: tx.(*gorm.DB),
	}
}
