package auth

import (
	"crypto/sha256"
	"encoding/hex"

	keycache "IM_backend/internal/infra/cache/key"
)

func RefreshTokenKey(token string) string {
	return RefreshTokenKeyFromHash(RefreshTokenHash(token))
}

func RefreshTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func RefreshTokenKeyFromHash(hash string) string {
	return keycache.AuthRefreshToken(hash)
}
