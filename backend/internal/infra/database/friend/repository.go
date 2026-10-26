package infrafriend

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	domainfriend "IM_backend/internal/domain/friend"
)

type FriendRepository struct {
	db *gorm.DB
}

func NewFriendRepository(db *gorm.DB) *FriendRepository {
	return &FriendRepository{
		db: db,
	}
}

func (r *FriendRepository) WithTx(tx any) domainfriend.FriendRepository {
	return &FriendRepository{db: tx.(*gorm.DB)}
}

func (fr *FriendRepository) Create(domains []domainfriend.Friend) error {
	return fr.db.Transaction(func(tx *gorm.DB) error {
		for _, d := range domains {
			model := toModel(d)

			if err := tx.Clauses(clause.OnConflict{
				DoNothing: true,
			}).Create(&model).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (fr *FriendRepository) GetUserFriendList(userID string, status int) ([]domainfriend.Friend, error) {
	var m []FriendModel

	if err := fr.db.
		Where("user_id = ? AND status = ?", userID, status).
		Find(&m).
		Error; err != nil {
		return nil, err
	}

	domains := make([]domainfriend.Friend, 0, len(m))

	for _, r := range m {
		domains = append(domains, toDomain(r))
	}

	return domains, nil
}

func (fr *FriendRepository) FindFriendByUserIDAndFriendUserID(userID, friendID string) (*domainfriend.Friend, error) {
	var m FriendModel

	if err := fr.db.
		Where("user_id = ? AND friend_user_id = ?", userID, friendID).
		First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		} else {
			return nil, err
		}
	}

	d := toDomain(m)
	return &d, nil
}

func (fr *FriendRepository) ListFriendsByUserPairs(ctx context.Context, pairs []domainfriend.RelationPair) ([]*domainfriend.Friend, error) {
	if len(pairs) == 0 {
		return []*domainfriend.Friend{}, nil
	}
	query := fr.db.WithContext(ctx).Model(&FriendModel{})
	for i, pair := range pairs {
		condition := "(user_id = ? AND friend_user_id = ?)"
		if i == 0 {
			query = query.Where(condition, pair.UserID, pair.FriendUserID)
		} else {
			query = query.Or(condition, pair.UserID, pair.FriendUserID)
		}
	}
	var rows []FriendModel
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}
	relations := make([]*domainfriend.Friend, 0, len(rows))
	for _, row := range rows {
		domain := toDomain(row)
		relations = append(relations, &domain)
	}
	return relations, nil
}

func (fr *FriendRepository) ListFriendsByUserIDAndFriendUserIDs(
	ctx context.Context,
	userID string,
	friendIDs []string,
) ([]*domainfriend.Friend, error) {
	if len(friendIDs) == 0 {
		return []*domainfriend.Friend{}, nil
	}

	var models []FriendModel

	err := fr.db.
		WithContext(ctx).
		Where(
			"user_id = ? AND friend_user_id IN ?",
			userID,
			friendIDs,
		).
		Find(&models).
		Error
	if err != nil {
		return nil, err
	}

	relations := make(
		[]*domainfriend.Friend,
		0,
		len(models),
	)

	for _, m := range models {
		domain := toDomain(m)
		relations = append(relations, &domain)
	}

	return relations, nil
}
