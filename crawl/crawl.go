package crawl

import (
	"cloud.google.com/go/logging"
	"context"
	"github.com/nozgurozturk/pararilytics/crawl/cache"
	"github.com/nozgurozturk/pararilytics/crawl/house"
	"github.com/nozgurozturk/pararilytics/crawl/logger"
	"github.com/nozgurozturk/pararilytics/crawl/publisher"
	"github.com/nozgurozturk/pararilytics/crawl/scraper"
	"os"
)

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func Crawl(ctx context.Context, m PubSubMessage) error {
	// Setup
	projectID := os.Getenv("PROJECT_ID")
	if err := logger.New(ctx, projectID); err != nil {
		return err
	}
	if err := publisher.New(ctx, projectID); err != nil {
		return err
	}
	if err := cache.NewClient(); err != nil {
		return err
	}

	// TearDown
	defer func() {
		if err := publisher.Client.Close(); err != nil {
			logger.NewEntry(logging.Error, "can not disconnect from pub/sub client", err.Error())
		}
		logger.NewEntry(logging.Notice, "disconnected from pub/sub", "")

		if err := cache.Client.Close(); err != nil {
			logger.NewEntry(logging.Error, "can not disconnect from redis instance", err.Error())
		}

		logger.NewEntry(logging.Notice, "disconnected from redis instance", "")
	}()

	houses := make([]house.House, 0)

	houseIDMap := cache.GetHouses(ctx)
	scrappedHouses := scraper.ScrapHouses(0)

	for _, sh := range scrappedHouses {

		if _, ok := houseIDMap[sh.ID]; !ok {
			houses = append(houses, sh)
		}

	}

	cache.SetHouses(ctx, houses)

	publisher.Publish(ctx, houses)

	return nil
}
