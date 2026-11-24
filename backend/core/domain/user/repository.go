package domainuser

import (
	"context"
	"time"
)

type UserRepository interface {
	FindUserByPhone(string) (*User, error)
	Create(*User) error
	FindUserByUserID(userID string) (*User, error)
	FindUserByUsernameOrPhone(keyword string) (*User, error)
	ListUsersByUserIDs(userIDs []string) ([]User, error)
	UpdateUserProfile(ctx context.Context, userID string, updates map[string]any) (*User, error)
	UpdateOnlineTime(userID, phone string, onlineAt time.Time) error
	UpdateOfflineTime(userID, phone string, offlineAt time.Time) error
	WithTx(tx any) UserRepository
}
