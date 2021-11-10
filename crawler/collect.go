package crawler

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/pkg/errors"
	"log"
	"os"
)

type MessagePublishedData struct {
	Message PubSubMessage
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}


func CollectHousesOf(ctx context.Context, e event.Event) error {

	projectID := os.Getenv("PROJECT_ID")
	topicID := os.Getenv("TOPIC_ID")

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("event.DataAs: %v", err)
	}

	city := string(msg.Message.Data)
	if city == "" {
		log.Fatal(errors.New("City name is required value"))
	}

	houses := crawlHouses(city)
	payload, err := json.Marshal(houses)
	if err != nil {
		log.Fatal(err)
	}

	topic := client.Topic(topicID)
	result := topic.Publish(ctx, &pubsub.Message{
		Data: payload,
	})

	if _, err := result.Get(ctx); err != nil {
		log.Fatal(err)
	}

	return nil
}
