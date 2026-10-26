package infraroom

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	domainroom "IM_backend/internal/domain/room"
)

type RoomMemberRepository struct {
	db *gorm.DB
}

func NewRoomMemberRepository(db *gorm.DB) *RoomMemberRepository {
	return &RoomMemberRepository{
		db: db,
	}
}

func (r *RoomMemberRepository) WithTx(tx any) domainroom.RoomMemberRepository {
	return &RoomMemberRepository{
		db: tx.(*gorm.DB),
	}
}

func (r *RoomMemberRepository) FindRoomMemberByUserIDAndRoomID(userID, roomID string) (*domainroom.RoomMember, error) {
	var m RoomMemberModel
	if err := r.db.Where("user_id = ? AND room_id = ?", userID, roomID).
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domainroom.ErrMemberNotFound
		}

		return nil, err
	}

	return memberToDomain(m), nil
}

func (r *RoomMemberRepository) ListRoomMembersByRoomIDAndUserIDs(ctx context.Context, roomID string, userIDs []string) ([]*domainroom.RoomMember, error) {
	if len(userIDs) == 0 {
		return []*domainroom.RoomMember{}, nil
	}
	var rows []RoomMemberModel
	if err := r.db.WithContext(ctx).
		Where("room_id = ? AND user_id IN ?", roomID, userIDs).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	members := make([]*domainroom.RoomMember, 0, len(rows))
	for _, row := range rows {
		members = append(members, memberToDomain(row))
	}
	return members, nil
}

func (r *RoomMemberRepository) ListActiveRoomIDsByUserID(userID string) ([]string, error) {
	var roomIDs []string
	err := r.db.Model(&RoomMemberModel{}).
		Where("user_id = ? AND status IN ?", userID, []int{int(domainroom.Active), int(domainroom.Muted)}).
		Pluck("room_id", &roomIDs).Error
	return roomIDs, err
}

func (r *RoomMemberRepository) RejoinRoom(
	member *domainroom.RoomMember,
) error {
	result := r.db.Model(&RoomMemberModel{}).
		Where("room_id = ? AND user_id = ?", member.RoomID, member.UserID).
		Where("status = ?", domainroom.Left).
		Where("version = ?", member.Version).
		Updates(map[string]any{
			"status":     domainroom.Active,
			"join_time":  member.JoinTime,
			"leave_time": nil,
			"mute_util":  nil,
			"version":    gorm.Expr("version + 1"),
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return domainroom.ErrVersionConflict
	}

	member.Version++
	return nil
}

func (r *RoomMemberRepository) JoinRoom(domain *domainroom.RoomMember) (*domainroom.RoomMember, error) {
	model := memberToModel(domain)

	res := r.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{
			Name: "room_id",
		}, {
			Name: "user_id",
		},
		},
		DoNothing: true,
	}).Create(&model)

	if res.Error != nil {
		return nil, res.Error
	}

	if res.RowsAffected == 0 {
		return nil, domainroom.ErrDuplicateCreation
	}

	return memberToDomain(model), nil
}

func (r *RoomMemberRepository) LeaveRoom(domain *domainroom.RoomMember, status []int) error {
	var m = memberToModel(domain)

	updates := map[string]interface{}{
		"status":     m.Status,
		"leave_time": m.LeaveTime,
		"version":    gorm.Expr("version + 1"),
	}
	res := r.db.Model(&RoomMemberModel{}).
		Where("room_id = ? AND user_id = ?", domain.RoomID, domain.UserID).
		Where("status IN ?", status).
		Where("version = ?", m.Version).
		Updates(updates)

	if res.Error != nil {
		return res.Error
	}

	if res.RowsAffected == 0 {
		return domainroom.ErrVersionConflict
	}

	domain.Version++

	return nil
}
