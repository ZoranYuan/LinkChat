package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"

	"github.com/google/uuid"

	"IM_backend/configs"
	"IM_backend/internal/infra/security/jwt"
)

type TokenService struct {
	jwtConfig configs.JWT
}

func NewTokenService(jwtConfig configs.JWT) *TokenService {
	return &TokenService{jwtConfig: jwtConfig}
}

func (s *TokenService) IssueToken(userID string) (string, string, string, error) {
	sessionID := uuid.NewString()
	accessToken, refreshToken, err := s.IssueTokensForSession(userID, sessionID)
	if err != nil {
		return "", "", "", err
	}
	return accessToken, refreshToken, sessionID, nil
}

func (s *TokenService) IssueTokensForSession(userID, sessionID string) (string, string, error) {
	if userID == "" || sessionID == "" {
		return "", "", errors.New("用户或认证会话 ID 为空")
	}
	accessToken, err := jwt.GenerateAccessToken(
		userID,
		sessionID,
		s.jwtConfig,
	)
	if err != nil {
		return "", "", err
	}

	refreshBytes := make([]byte, 32)
	if _, err := rand.Read(refreshBytes); err != nil {
		return "", "", err
	}

	refreshToken := base64.RawURLEncoding.EncodeToString(refreshBytes)
	return accessToken, refreshToken, nil
}

func (s *TokenService) ValidateAccessToken(token string) (string, string, error) {
	claims, err := jwt.ValidateToken(token, s.jwtConfig)
	if err != nil {
		return "", "", err
	}
	return claims.UserID, claims.SessionID, nil
}
