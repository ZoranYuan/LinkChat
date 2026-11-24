package infragroup

import (
	"context"
	"errors"

	"gorm.io/gorm"

	domaingroup "IM_backend/core/domain/group"
)

type GroupRepository struct {
	db *gorm.DB
}

func NewGroupRepository(db *gorm.DB) *GroupRepository {
	return &GroupRepository{
		db: db,
	}
}

func (rr *GroupRepository) Create(domain *domaingroup.Group) error {
	model := toModel(domain)

	return rr.db.Create(&model).Error
}

func (rr *GroupRepository) FindGroupByGroupIDAndStatus(groupID string, status int) (*domaingroup.Group, error) {
	var m GroupModel

	if err := rr.db.Where("group_id = ? AND status = ?", groupID, status).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domaingroup.ErrGroupNotFound
		}

		return nil, err
	}

	return toDomain(m), nil
}

func (rr *GroupRepository) FindGroupByGroupID(ctx context.Context, groupID string) (*domaingroup.Group, error) {
	var m GroupModel
	if err := rr.db.WithContext(ctx).Where("group_id = ?", groupID).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domaingroup.ErrGroupNotFound
		}
		return nil, err
	}
	return toDomain(m), nil
}

func (rr *GroupRepository) ListGroupsByIDs(ctx context.Context, groupIDs []string) ([]*domaingroup.Group, error) {
	if len(groupIDs) == 0 {
		return []*domaingroup.Group{}, nil
	}
	var models []GroupModel
	if err := rr.db.WithContext(ctx).Where("group_id IN ?", groupIDs).Find(&models).Error; err != nil {
		return nil, err
	}
	groups := make([]*domaingroup.Group, 0, len(models))
	for _, model := range models {
		groups = append(groups, toDomain(model))
	}
	return groups, nil
}

func (rr *GroupRepository) IncrementMemberCount(ctx context.Context, groupID string, status int) (bool, error) {
	result := rr.db.WithContext(ctx).
		Model(&GroupModel{}).
		Where("group_id = ? AND status = ? AND (max_members <= 0 OR member_count < max_members)", groupID, status).
		UpdateColumn("member_count", gorm.Expr("member_count + 1"))
	return result.RowsAffected == 1, result.Error
}

func (rr *GroupRepository) DecrementMemberCount(ctx context.Context, groupID string, status int) (bool, error) {
	result := rr.db.WithContext(ctx).
		Model(&GroupModel{}).
		Where("group_id = ? AND status = ? AND member_count > 0", groupID, status).
		UpdateColumn("member_count", gorm.Expr("member_count - 1"))
	return result.RowsAffected == 1, result.Error
}

func (rr *GroupRepository) WithTx(tx any) domaingroup.GroupRepository {
	return &GroupRepository{
		db: tx.(*gorm.DB),
	}
}
