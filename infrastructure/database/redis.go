package database

import (
	"context"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func NewRedis() (*redis.Client, error) {
	poolSize := 100

	if poolSizeStr := os.Getenv("REDIS_POOL_SIZE"); poolSizeStr != "" {
		if val, err := strconv.Atoi(poolSizeStr); err == nil && val > 0 {
			poolSize = val
		}
	}

	client := redis.NewClient(&redis.Options{
		Addr:         os.Getenv("REDIS_HOST"),
		DB:           0,
		PoolSize:     poolSize,
		MinIdleConns: 20,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return client, nil
}
