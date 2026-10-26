package infraroom

import (
	"context"
	"errors"

	"gorm.io/gorm"

	domainroom "IM_backend/internal/domain/room"
)

type RoomRepository struct {
	db *gorm.DB
}

func NewRoomRepository(db *gorm.DB) *RoomRepository {
	return &RoomRepository{
		db: db,
	}
}

func (rr *RoomRepository) Create(domain *domainroom.Room) error {
	model := toModel(domain)

	return rr.db.Create(&model).Error
}

func (rr *RoomRepository) FindRoomByRoomIDAndStatus(roomID string, status int) (*domainroom.Room, error) {
	var m RoomModel

	if err := rr.db.Where("room_id = ? AND status = ?", roomID, status).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domainroom.ErrRoomNotFound
		}

		return nil, err
	}

	return toDomain(m), nil
}

func (rr *RoomRepository) FindRoomByRoomID(ctx context.Context, roomID string) (*domainroom.Room, error) {
	var m RoomModel
	if err := rr.db.WithContext(ctx).Where("room_id = ?", roomID).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domainroom.ErrRoomNotFound
		}
		return nil, err
	}
	return toDomain(m), nil
}

func (rr *RoomRepository) IncrementMemberCount(ctx context.Context, roomID string, status int) (bool, error) {
	result := rr.db.WithContext(ctx).
		Model(&RoomModel{}).
		Where("room_id = ? AND status = ? AND (max_members <= 0 OR member_count < max_members)", roomID, status).
		UpdateColumn("member_count", gorm.Expr("member_count + 1"))
	return result.RowsAffected == 1, result.Error
}

func (rr *RoomRepository) DecrementMemberCount(ctx context.Context, roomID string, status int) (bool, error) {
	result := rr.db.WithContext(ctx).
		Model(&RoomModel{}).
		Where("room_id = ? AND status = ? AND member_count > 0", roomID, status).
		UpdateColumn("member_count", gorm.Expr("member_count - 1"))
	return result.RowsAffected == 1, result.Error
}

func (rr *RoomRepository) WithTx(tx any) domainroom.RoomRepository {
	return &RoomRepository{
		db: tx.(*gorm.DB),
	}
}
