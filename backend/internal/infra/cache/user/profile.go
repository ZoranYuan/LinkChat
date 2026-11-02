package user

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	usercache "IM_backend/internal/application/ports/cache/user"
)

type ProfileCache struct {
	client *redis.Client
}

var _ usercache.ProfileCache = (*ProfileCache)(nil)

func NewProfileCache(client *redis.Client) *ProfileCache { return &ProfileCache{client: client} }

func phoneKey(phone string) string   { return "im:user:phone:" + strings.TrimSpace(phone) }
func userIDKey(userID string) string { return "im:user:profile:" + userID }
func usernameKey(username string) string {
	return "im:user:username:" + strings.ToLower(strings.TrimSpace(username))
}
func missingKey(keyword string) string {
	return "im:user:missing:" + strings.ToLower(strings.TrimSpace(keyword))
}

func (c *ProfileCache) get(ctx context.Context, key string) (*usercache.Profile, bool, error) {
	data, err := c.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var profile usercache.Profile
	if err := json.Unmarshal(data, &profile); err != nil {
		return nil, false, err
	}
	return &profile, true, nil
}

func (c *ProfileCache) set(ctx context.Context, key string, profile *usercache.Profile, ttl time.Duration) error {
	data, err := json.Marshal(profile)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key, data, ttl).Err()
}

func (c *ProfileCache) GetByPhone(ctx context.Context, phone string) (*usercache.Profile, bool, error) {
	return c.get(ctx, phoneKey(phone))
}

func (c *ProfileCache) GetByUsername(ctx context.Context, username string) (*usercache.Profile, bool, error) {
	return c.get(ctx, usernameKey(username))
}

func (c *ProfileCache) SetByPhone(ctx context.Context, phone string, profile *usercache.Profile, ttl time.Duration) error {
	return c.set(ctx, phoneKey(phone), profile, ttl)
}

func (c *ProfileCache) GetByUserIDs(ctx context.Context, userIDs []string) (map[string]*usercache.Profile, error) {
	profiles := make(map[string]*usercache.Profile, len(userIDs))
	if len(userIDs) == 0 {
		return profiles, nil
	}
	keys := make([]string, len(userIDs))
	for i, userID := range userIDs {
		keys[i] = userIDKey(userID)
	}
	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		if value == nil {
			continue
		}
		data, ok := value.(string)
		if !ok {
			continue
		}
		var profile usercache.Profile
		if err := json.Unmarshal([]byte(data), &profile); err != nil {
			return nil, err
		}
		profiles[userIDs[i]] = &profile
	}
	return profiles, nil
}

func (c *ProfileCache) SetByUserID(ctx context.Context, userID string, profile *usercache.Profile, ttl time.Duration) error {
	return c.set(ctx, userIDKey(userID), profile, ttl)
}

func (c *ProfileCache) DeleteByUserID(ctx context.Context, userID string) error {
	return c.client.Del(ctx, userIDKey(userID)).Err()
}

func (c *ProfileCache) SetByUsername(ctx context.Context, username string, profile *usercache.Profile, ttl time.Duration) error {
	return c.set(ctx, usernameKey(username), profile, ttl)
}

func (c *ProfileCache) GetNotFound(ctx context.Context, keyword string) (bool, error) {
	_, err := c.client.Get(ctx, missingKey(keyword)).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	return err == nil, err
}

func (c *ProfileCache) SetNotFound(ctx context.Context, keyword string, ttl time.Duration) error {
	return c.client.Set(ctx, missingKey(keyword), "1", ttl).Err()
}

func (c *ProfileCache) DeleteByPhone(ctx context.Context, phone string) error {
	return c.client.Del(ctx, phoneKey(phone)).Err()
}

func (c *ProfileCache) DeleteByUsername(ctx context.Context, username string) error {
	return c.client.Del(ctx, usernameKey(username)).Err()
}

func (c *ProfileCache) DeleteNotFound(ctx context.Context, keyword string) error {
	return c.client.Del(ctx, missingKey(keyword)).Err()
}
