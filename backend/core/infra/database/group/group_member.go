package infragroup

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	domaingroup "IM_backend/core/domain/group"
)

type GroupMemberRepository struct {
	db *gorm.DB
}

func NewGroupMemberRepository(db *gorm.DB) *GroupMemberRepository {
	return &GroupMemberRepository{
		db: db,
	}
}

func (r *GroupMemberRepository) WithTx(tx any) domaingroup.GroupMemberRepository {
	return &GroupMemberRepository{
		db: tx.(*gorm.DB),
	}
}

func (r *GroupMemberRepository) FindGroupMemberByUserIDAndGroupID(userID, groupID string) (*domaingroup.GroupMember, error) {
	var m GroupMemberModel
	if err := r.db.Where("user_id = ? AND group_id = ?", userID, groupID).
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domaingroup.ErrMemberNotFound
		}

		return nil, err
	}

	return memberToDomain(m), nil
}

func (r *GroupMemberRepository) ListGroupMembersByGroupIDAndUserIDs(ctx context.Context, groupID string, userIDs []string) ([]*domaingroup.GroupMember, error) {
	if len(userIDs) == 0 {
		return []*domaingroup.GroupMember{}, nil
	}
	var rows []GroupMemberModel
	if err := r.db.WithContext(ctx).
		Where("group_id = ? AND user_id IN ?", groupID, userIDs).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	members := make([]*domaingroup.GroupMember, 0, len(rows))
	for _, row := range rows {
		members = append(members, memberToDomain(row))
	}
	return members, nil
}

func (r *GroupMemberRepository) ListActiveGroupIDsByUserID(userID string) ([]string, error) {
	var groupIDs []string
	err := r.db.Model(&GroupMemberModel{}).
		Where("user_id = ? AND status IN ?", userID, []int{int(domaingroup.Active), int(domaingroup.Muted)}).
		Pluck("group_id", &groupIDs).Error
	return groupIDs, err
}

func (r *GroupMemberRepository) RejoinGroup(
	member *domaingroup.GroupMember,
) error {
	result := r.db.Model(&GroupMemberModel{}).
		Where("group_id = ? AND user_id = ?", member.GroupID, member.UserID).
		Where("status = ?", domaingroup.Left).
		Where("version = ?", member.Version).
		Updates(map[string]any{
			"status":     domaingroup.Active,
			"join_time":  member.JoinTime,
			"leave_time": nil,
			"mute_util":  nil,
			"version":    gorm.Expr("version + 1"),
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return domaingroup.ErrVersionConflict
	}

	member.Version++
	return nil
}

func (r *GroupMemberRepository) JoinGroup(domain *domaingroup.GroupMember) (*domaingroup.GroupMember, error) {
	model := memberToModel(domain)

	res := r.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{
			Name: "group_id",
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
		return nil, domaingroup.ErrDuplicateCreation
	}

	return memberToDomain(model), nil
}

func (r *GroupMemberRepository) LeaveGroup(domain *domaingroup.GroupMember, status []int) error {
	var m = memberToModel(domain)

	updates := map[string]interface{}{
		"status":     m.Status,
		"leave_time": m.LeaveTime,
		"version":    gorm.Expr("version + 1"),
	}
	res := r.db.Model(&GroupMemberModel{}).
		Where("group_id = ? AND user_id = ?", domain.GroupID, domain.UserID).
		Where("status IN ?", status).
		Where("version = ?", m.Version).
		Updates(updates)

	if res.Error != nil {
		return res.Error
	}

	if res.RowsAffected == 0 {
		return domaingroup.ErrVersionConflict
	}

	domain.Version++

	return nil
}
