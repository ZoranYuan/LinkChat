package user

import (
	"context"
	"time"
)

type Profile struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
	Nickname string `json:"nickname"`
	Avatar   string `json:"avatar"`
}

type ProfileCache interface {
	GetByUserIDs(ctx context.Context, userIDs []string) (map[string]*Profile, error)
	SetByUserID(ctx context.Context, userID string, profile *Profile, ttl time.Duration) error
	DeleteByUserID(ctx context.Context, userID string) error
	GetByPhone(ctx context.Context, phone string) (*Profile, bool, error)
	GetByUsername(ctx context.Context, username string) (*Profile, bool, error)
	SetByPhone(ctx context.Context, phone string, profile *Profile, ttl time.Duration) error
	SetByUsername(ctx context.Context, username string, profile *Profile, ttl time.Duration) error
	GetNotFound(ctx context.Context, keyword string) (bool, error)
	SetNotFound(ctx context.Context, keyword string, ttl time.Duration) error
	DeleteByPhone(ctx context.Context, phone string) error
	DeleteByUsername(ctx context.Context, username string) error
	DeleteNotFound(ctx context.Context, keyword string) error
}
