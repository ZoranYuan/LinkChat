package ratelimit

import (
	"context"
	"time"
)

// 将限流器单独抽离，为了方便以后切换限流方式

type Policy struct {
	// 令牌生成的速率
	Rate float64

	// 桶的最大数量，系统允许的最大突发
	Burst int64
}

type Decision struct {
	Allowed bool

	// 最早可以重试的时间
	RetryAfter time.Duration
}

type Limiter interface {
	Allow(
		ctx context.Context,
		key string,
		policy Policy,
	) (Decision, error)
}
