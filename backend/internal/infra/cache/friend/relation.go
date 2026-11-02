package friend

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	friendcache "IM_backend/internal/application/ports/cache/friend"
)

type RelationCache struct {
	client *redis.Client
}

var _ friendcache.RelationCache = (*RelationCache)(nil)

func NewRelationCache(client *redis.Client) *RelationCache {
	return &RelationCache{client: client}
}

func relationKey(userID, friendUserID string) string {
	return "im:friend:relation:" + userID + ":" + friendUserID
}

func (c *RelationCache) Get(ctx context.Context, userID, friendUserID string) (*friendcache.Relation, bool, error) {
	data, err := c.client.Get(ctx, relationKey(userID, friendUserID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var relation friendcache.Relation
	if err := json.Unmarshal(data, &relation); err != nil {
		return nil, false, err
	}
	return &relation, true, nil
}

func (c *RelationCache) Set(ctx context.Context, userID, friendUserID string, relation *friendcache.Relation, ttl time.Duration) error {
	data, err := json.Marshal(relation)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, relationKey(userID, friendUserID), data, ttl).Err()
}

func (c *RelationCache) Delete(ctx context.Context, userID, friendUserID string) error {
	return c.client.Del(ctx, relationKey(userID, friendUserID)).Err()
}
