package transaction

import (
	"context"
)

type Manager interface {
	WithinTransaction(ctx context.Context, fn func(tx any) error) error
}
