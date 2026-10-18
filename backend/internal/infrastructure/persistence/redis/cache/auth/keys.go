package auth

import (
	cachekey "IM_backend/internal/infrastructure/persistence/redis/cache/key"
	"crypto/sha256"
	"encoding/hex"
)

func RefreshTokenKey(token string) string {
	return RefreshTokenKeyFromHash(RefreshTokenHash(token))
}

func RefreshTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func RefreshTokenKeyFromHash(hash string) string {
	return cachekey.AuthRefreshToken(hash)
}
