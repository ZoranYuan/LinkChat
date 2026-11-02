package friend

import (
	"context"
	"time"
)

type Relation struct {
	Exists  bool   `json:"exists"`
	Status  int    `json:"status"`
	Remarks string `json:"remarks"`
}

type RelationCache interface {
	Get(ctx context.Context, userID, friendUserID string) (*Relation, bool, error)
	Set(ctx context.Context, userID, friendUserID string, relation *Relation, ttl time.Duration) error
	Delete(ctx context.Context, userID, friendUserID string) error
}
