package crawl

import (
	"cloud.google.com/go/logging"
	"context"
	"github.com/nozgurozturk/pararilytics/crawl/logger"
	"github.com/nozgurozturk/pararilytics/crawl/publisher"
	"github.com/nozgurozturk/pararilytics/crawl/scraper"
	"github.com/pkg/errors"
	"os"
)

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func CollectHousesOf(ctx context.Context, m PubSubMessage) error {
	// Setup
	projectID := os.Getenv("PROJECT_ID")
	if err := publisher.New(ctx, projectID); err != nil {
		return err
	}
	logger.NewEntry(logging.Notice, "connected to pub/sub", "")

	// TearDown
	defer func() {
		if err := publisher.Client.Close(); err != nil {
			logger.NewEntry(logging.Error, "can not disconnect from pub/sub client", err.Error())
		}
		logger.NewEntry(logging.Notice, "disconnected from pub/sub", "")
	}()

	city := string(m.Data)

	if city == "" {
		err := errors.New("can not found city in message data")
		logger.NewEntry(logging.Error, "Unprocessable entity", err.Error())
		return err
	}

	houses := scraper.ScrapHousesOf(city)

	publisher.Publish(ctx, houses)

	return nil
}
