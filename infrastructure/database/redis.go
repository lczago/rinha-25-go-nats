package database

import (
	"context"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"os"
)

func NewRedis() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_HOST"),
		DB:   0,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}
	return client, nil
}
