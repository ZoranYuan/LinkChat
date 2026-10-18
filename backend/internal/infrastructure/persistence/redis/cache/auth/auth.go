package auth

import (
	authport "IM_backend/internal/application/ports/persistence/cache/auth"
	cachekey "IM_backend/internal/infrastructure/persistence/redis/cache/key"
	"IM_backend/internal/infrastructure/persistence/redis/cache/shared"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	errSessionAlreadyExists = errors.New("认证会话已存在")
	errSessionOwnerMismatch = errors.New("认证会话用户不匹配")
)

const createSessionScript = `
	if redis.call("EXISTS", KEYS[1]) == 1 or redis.call("EXISTS", KEYS[2]) == 1 then
		return 0
	end
	redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[3])
	redis.call("SET", KEYS[2], ARGV[2], "PX", ARGV[3])
	return 1
`

const rotateRefreshSessionScript = `
	if redis.call("GET", KEYS[1]) ~= ARGV[1] then
		return 0
	end
	if redis.call("EXISTS", KEYS[2]) == 1 then
		return 0
	end
	local current = redis.call("GET", KEYS[3])
	if not current then
		return 0
	end
	local current_session = cjson.decode(current)
	if current_session.userId ~= ARGV[2] or current_session.refreshTokenHash ~= ARGV[3] then
		return 0
	end
	redis.call("SET", KEYS[3], ARGV[4], "PX", ARGV[5])
	redis.call("SET", KEYS[2], ARGV[1], "PX", ARGV[5])
	redis.call("DEL", KEYS[1])
	return 1
`

const deleteSessionScript = `
	local current = redis.call("GET", KEYS[1])
	if not current then
		return 0
	end
	local session = cjson.decode(current)
	if session.userId ~= ARGV[1] then
		return -1
	end
	if session.refreshTokenHash and session.refreshTokenHash ~= "" then
		local refresh_key = ARGV[2] .. session.refreshTokenHash
		if redis.call("GET", refresh_key) == ARGV[3] then
			redis.call("DEL", refresh_key)
		end
	end
	redis.call("DEL", KEYS[1])
	return 1
`

const isSessionActiveScript = `
	local current = redis.call("GET", KEYS[1])
	if not current then
		return 0
	end
	local session = cjson.decode(current)
	if session.userId ~= ARGV[1] or not session.refreshTokenHash or session.refreshTokenHash == "" then
		return 0
	end
	local refresh_key = ARGV[2] .. session.refreshTokenHash
	if redis.call("GET", refresh_key) ~= ARGV[3] then
		return 0
	end
	return 1
`

type sessionRecord struct {
	UserID           string `json:"userId"`
	RefreshTokenHash string `json:"refreshTokenHash"`
}

type authCache struct {
	store *shared.Store
}

func NewAuthCache(client *redis.Client) *authCache {
	return &authCache{store: shared.NewStore(client)}
}

func (ac *authCache) CreateSession(
	ctx context.Context,
	refreshToken string,
	session authport.RefreshSession,
	expire time.Duration,
) error {
	if refreshToken == "" || session.UserID == "" || session.SessionID == "" || expire.Milliseconds() <= 0 {
		return errors.New("创建认证会话参数无效")
	}

	refreshHash := RefreshTokenHash(refreshToken)
	data, err := json.Marshal(sessionRecord{UserID: session.UserID, RefreshTokenHash: refreshHash})
	if err != nil {
		return err
	}
	result, err := ac.store.Client().Eval(
		ctx,
		createSessionScript,
		[]string{SessionKey(session.SessionID), RefreshTokenKeyFromHash(refreshHash)},
		string(data),
		session.SessionID,
		expire.Milliseconds(),
	).Int()
	if err != nil {
		return err
	}
	if result != 1 {
		return errSessionAlreadyExists
	}
	return nil
}

func (ac *authCache) GetRefreshSession(
	ctx context.Context,
	refreshToken string,
) (authport.RefreshSession, bool, error) {
	if refreshToken == "" {
		return authport.RefreshSession{}, false, nil
	}

	sessionID, err := ac.store.GetString(ctx, RefreshTokenKey(refreshToken))
	if err != nil {
		return authport.RefreshSession{}, false, err
	}
	if sessionID == "" {
		return authport.RefreshSession{}, false, nil
	}

	var record sessionRecord
	found, err := ac.store.GetJSON(ctx, SessionKey(sessionID), &record)
	if err != nil {
		return authport.RefreshSession{}, false, err
	}
	if !found || record.UserID == "" || record.RefreshTokenHash != RefreshTokenHash(refreshToken) {
		return authport.RefreshSession{}, false, nil
	}
	return authport.RefreshSession{UserID: record.UserID, SessionID: sessionID}, true, nil
}

func (ac *authCache) RotateRefreshSession(
	ctx context.Context,
	oldToken string,
	newToken string,
	session authport.RefreshSession,
	expire time.Duration,
) (bool, error) {
	if oldToken == "" || newToken == "" || session.UserID == "" || session.SessionID == "" || expire.Milliseconds() <= 0 {
		return false, errors.New("轮换认证会话参数无效")
	}
	oldHash := RefreshTokenHash(oldToken)
	newHash := RefreshTokenHash(newToken)
	if oldHash == newHash {
		return false, errors.New("新旧 refresh token 不能相同")
	}

	data, err := json.Marshal(sessionRecord{UserID: session.UserID, RefreshTokenHash: newHash})
	if err != nil {
		return false, err
	}
	result, err := ac.store.Client().Eval(
		ctx,
		rotateRefreshSessionScript,
		[]string{
			RefreshTokenKeyFromHash(oldHash),
			RefreshTokenKeyFromHash(newHash),
			SessionKey(session.SessionID),
		},
		session.SessionID,
		session.UserID,
		oldHash,
		string(data),
		expire.Milliseconds(),
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (ac *authCache) DeleteSession(ctx context.Context, userID, sessionID string) error {
	if userID == "" || sessionID == "" {
		return errors.New("删除认证会话参数无效")
	}
	result, err := ac.store.Client().Eval(
		ctx,
		deleteSessionScript,
		[]string{SessionKey(sessionID)},
		userID,
		refreshTokenKeyPrefix,
		sessionID,
	).Int()
	if err != nil {
		return err
	}
	if result == -1 {
		return fmt.Errorf("%w: sessionId=%s", errSessionOwnerMismatch, sessionID)
	}
	return nil
}

func (ac *authCache) IsSessionActive(ctx context.Context, userID, sessionID string) (bool, error) {
	if userID == "" || sessionID == "" {
		return false, nil
	}
	result, err := ac.store.Client().Eval(
		ctx,
		isSessionActiveScript,
		[]string{SessionKey(sessionID)},
		userID,
		refreshTokenKeyPrefix,
		sessionID,
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func SessionKey(sessionID string) string {
	return cachekey.AuthSession(sessionID)
}

var refreshTokenKeyPrefix = cachekey.AuthRefreshToken("") + ":"
