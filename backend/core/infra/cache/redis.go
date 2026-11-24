package cache

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

func InitRedis(dsn string) *redis.Client {
	opt, err := redis.ParseURL(dsn)
	if err != nil {
		log.Fatal("初始化缓存服务失败：", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := redis.NewClient(opt)

	if err = client.Ping(ctx).Err(); err != nil {
		log.Fatal("缓存服务连通性检查失败：", err)
	}

	return client
}
