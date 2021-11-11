package publisher

import (
	"cloud.google.com/go/logging"
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/nozgurozturk/pararilytics/crawl/logger"
)

var (
	Client *pubsub.Client
)

func New(ctx context.Context, projectID string) error {
	var err error
	Client, err = pubsub.NewClient(ctx, projectID)
	if err != nil {
		logger.NewEntry(logging.Error, "can not connected to pub/sub client", err.Error())
		return err
	}

	return nil
}
