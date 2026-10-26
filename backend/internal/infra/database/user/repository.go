package infrauser

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

	domainuser "IM_backend/internal/domain/user"
)

type UserRepository struct {
	db *gorm.DB
}

func NewUserRepository(db *gorm.DB) *UserRepository {
	return &UserRepository{
		db: db,
	}
}

func (r *UserRepository) WithTx(tx any) domainuser.UserRepository {
	return &UserRepository{db: tx.(*gorm.DB)}
}

func (ur *UserRepository) FindUserByPhone(phone string) (*domainuser.User, error) {
	var user = UserModel{}
	if err := ur.db.Where("phone = ?", phone).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	domain := toDomain(user)
	return &domain, nil
}

func (ur *UserRepository) Create(user *domainuser.User) error {
	model := toModel(user)
	result := ur.db.Where("phone = ?", model.Phone).FirstOrCreate(model)

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return domainuser.ErrUserAlreadyExists
	}

	return nil
}

func (ur *UserRepository) FindUserByUserID(userID string) (*domainuser.User, error) {
	var user = UserModel{}
	if err := ur.db.Where("user_id = ?", userID).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	domain := toDomain(user)
	return &domain, nil
}

func (ur *UserRepository) UpdateUserProfile(ctx context.Context,
	userID string,
	updates map[string]any) (*domainuser.User, error) {
	result := ur.db.WithContext(ctx).
		Model(&UserModel{}).
		Where("user_id = ?", userID).
		Updates(updates)

	if result.Error != nil {
		return nil, result.Error
	}

	var user UserModel
	if err := ur.db.WithContext(ctx).
		Where("user_id = ?", userID).
		First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domainuser.ErrUserNotFound
		}
		return nil, err
	}

	domain := toDomain(user)
	return &domain, nil
}

func (ur *UserRepository) FindUserByUsernameOrPhone(keyword string) (*domainuser.User, error) {
	var user = UserModel{}
	if err := ur.db.Where("user_name = ? OR phone = ?", keyword, keyword).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	domain := toDomain(user)
	return &domain, nil
}

func (ur *UserRepository) ListUsersByUserIDs(userIDs []string) ([]domainuser.User, error) {
	if len(userIDs) == 0 {
		return []domainuser.User{}, nil
	}

	var models []UserModel

	err := ur.db.
		Where("user_id IN ?", userIDs).
		Find(&models).Error
	if err != nil {
		return nil, err
	}

	users := make([]domainuser.User, 0, len(models))
	for _, m := range models {
		users = append(users, toDomain(m))
	}

	return users, nil
}

func (ur *UserRepository) updateTime(userID, phone, column string, value time.Time) error {
	return ur.db.Model(&UserModel{}).
		Where("user_id = ? AND phone = ?", userID, phone).
		Update(column, value).
		Error
}

func (ur *UserRepository) UpdateOnlineTime(userID, phone string, onlineAt time.Time) error {
	return ur.updateTime(userID, phone, "on_line_time", onlineAt)
}

func (ur *UserRepository) UpdateOfflineTime(userID, phone string, offlineAt time.Time) error {
	return ur.updateTime(userID, phone, "off_line_time", offlineAt)
}

func toModel(u *domainuser.User) *UserModel {
	return &UserModel{
		UserID:      u.UserID,
		Username:    u.Username,
		Nickname:    u.Nickname,
		Password:    string(u.Password), // ValueObject -> string
		Phone:       string(u.Phone),
		Avatar:      u.Avatar,
		Status:      int(u.Status),
		OnlineTime:  u.OnlineTime,
		OfflineTime: u.OfflineTime,
		LoginType:   int(u.LoginType),
		WxOpenID:    u.WxOpenID,
		WxUnionID:   u.WxUnionID,
	}
}
