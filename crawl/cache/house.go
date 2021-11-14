package cache

import (
	"cloud.google.com/go/logging"
	"context"
	"github.com/nozgurozturk/pararilytics/crawl/house"
	"github.com/nozgurozturk/pararilytics/crawl/logger"
	"time"
)

const (
	HOUSES_KEY = "HOUSES"
)

func isExist(ctx context.Context) bool {

	i, err := Client.Exists(ctx, HOUSES_KEY).Result()
	if err != nil {
		logger.NewEntry(logging.Error, "can not get houses from redis instance", err.Error())
		return false
	}
	return i == 1
}

func GetHouses(ctx context.Context) map[string]int {

	members, err := Client.SMembers(ctx, HOUSES_KEY).Result()
	if err != nil {
		logger.NewEntry(logging.Error, "can not get houses from redis instance", err.Error())
		return nil
	}
	houses := make(map[string]int, len(members))
	for _, member := range members {
		houses[member] = 1
	}
	return houses
}

func SetHouses(ctx context.Context, houses []house.House) {

	ids := make([]interface{}, len(houses))
	for i, h := range houses {
		ids[i] = h.ID
	}
	isExist := isExist(ctx)
	Client.SAdd(ctx, HOUSES_KEY, ids...)

	if !isExist {
		now := time.Now()
		Client.ExpireAt(ctx, HOUSES_KEY, now.AddDate(0, 0, 5))
	}

}
