package ratelimit

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	ratelimitport "IM_backend/internal/application/ports/ratelimit"
)

//go:embed limit.lua
var limitScript string

// 使用 redis 实现分布式限流
type RedisLimiter struct {
	client *redis.Client

	prefix string
}

func NewRedisLimiter(client *redis.Client, prefix string) *RedisLimiter {

	return &RedisLimiter{
		client,
		prefix,
	}
}

func (r *RedisLimiter) Allow(ctx context.Context, key string, policy ratelimitport.Policy) (ratelimitport.Decision, error) {
	if r.client == nil {
		return ratelimitport.Decision{}, errors.New("限流器缓存客户端不能为空")
	}

	if key == "" {
		return ratelimitport.Decision{}, errors.New("限流键不能为空")
	}

	if policy.Rate <= 0 || policy.Burst <= 0 {
		return ratelimitport.Decision{}, errors.New("限流策略参数无效")
	}

	// 执行 lua 脚本
	result, err := r.client.Eval(
		ctx,
		limitScript,
		[]string{r.prefix + key},
		strconv.FormatFloat(
			policy.Rate,
			'f',
			-1,
			64,
		),
		policy.Burst,
	).Result()

	if err != nil {
		return ratelimitport.Decision{}, err
	}

	values, ok := result.([]interface{})
	if !ok || len(values) != 3 {
		return ratelimitport.Decision{},
			fmt.Errorf(
				"限流结果格式异常：%T",
				result,
			)
	}

	allowed, err := redisInt64(values[0])
	if err != nil {
		return ratelimitport.Decision{}, err
	}

	retryAfterMs, err := redisInt64(values[2])
	if err != nil {
		return ratelimitport.Decision{}, err
	}

	return ratelimitport.Decision{
		Allowed: allowed == 1,
		RetryAfter: time.Duration(retryAfterMs) *
			time.Millisecond,
	}, nil
}

func redisInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil

	case string:
		return strconv.ParseInt(v, 10, 64)

	case []byte:
		return strconv.ParseInt(
			string(v),
			10,
			64,
		)

	default:
		return 0, fmt.Errorf(
			"缓存整数类型异常：%T",
			value,
		)
	}
}
