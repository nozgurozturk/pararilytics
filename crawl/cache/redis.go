package cache

import (
	"cloud.google.com/go/logging"
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/nozgurozturk/pararilytics/crawl/logger"
	"os"
)

var (
	Client *redis.Client
)

func NewClient() error {
	redisURL := os.Getenv("REDIS_URL")

	if redisURL == "" {
		err := errors.New("REDIS_URL must set")
		logger.NewEntry(logging.Critical, "can not connected to redis instance", err.Error())
		return err
	}
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		logger.NewEntry(logging.Critical, "can not connected to redis instance", err.Error())
		return err
	}

	Client = redis.NewClient(opt)
	if _, err := Client.Ping(context.Background()).Result(); err != nil {
		logger.NewEntry(logging.Critical, "can not connected to redis instance", err.Error())
		return err
	}

	logger.NewEntry(logging.Notice, "connected to redis instance", "")

	return nil
}
