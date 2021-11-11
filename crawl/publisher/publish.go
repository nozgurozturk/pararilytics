package publisher

import (
	"cloud.google.com/go/logging"
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"github.com/nozgurozturk/pararilytics/crawl/logger"
	"os"
)

const (
	PUBLISHED_TOPIC_ID_KEY = "PUBLISHED_TOPIC_ID"
)

func Publish(ctx context.Context, payload interface{}) {

	data, err := json.Marshal(payload)
	if err != nil {
		logger.NewEntry(logging.Error, "can not parse payload for publishing message", err.Error())
		return
	}

	topic := Client.Topic(os.Getenv(PUBLISHED_TOPIC_ID_KEY))

	result := topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	id, err := result.Get(ctx)
	if err != nil {
		logger.NewEntry(logging.Error, "can not get message", err.Error())
		return
	}

	logger.NewEntry(logging.Info, "Message: " + id, "")
}
