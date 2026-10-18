package auth

import (
	"context"
	"time"
)

type AuthCache interface {
	CreateSession(ctx context.Context, refreshToken string, session RefreshSession, expire time.Duration) error
	GetRefreshSession(ctx context.Context, token string) (RefreshSession, bool, error)
	RotateRefreshSession(
		ctx context.Context,
		oldToken string,
		newToken string,
		session RefreshSession,
		expire time.Duration,
	) (bool, error)
	DeleteSession(ctx context.Context, userID, sessionID string) error
	IsSessionActive(ctx context.Context, userID, sessionID string) (bool, error)
}

type RefreshSession struct {
	UserID    string `json:"userId"`
	SessionID string `json:"sessionId"`
}
