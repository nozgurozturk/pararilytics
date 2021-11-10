package collector

import (
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"log"
	"os"
	"time"
)

type MessagePublishedData struct {
	Message PubSubMessage
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func StoreHouseFromCity(ctx context.Context, e event.Event) error {
	projectID := os.Getenv("PROJECT_ID")

	fireStoreClient, err := firestore.NewClient(ctx, projectID)

	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("event.DataAs: %v", err)
	}

	var houses []House

	if err := json.Unmarshal(msg.Message.Data, &houses); err != nil {
		return fmt.Errorf("json.Unmarshal: %v", err)
	}

	var fireStoreHouses map[string]House

	for _, house := range houses {
		fireStoreHouses[house.ID] = house
	}

	_, err = fireStoreClient.Collection("cities").Doc(houses[0].Address.City).Set(ctx, fireStoreHouses, firestore.MergeAll)

	if err != nil {
		// Handle any errors in an appropriate way, such as returning them.
		log.Printf("An error has occurred: %s", err)
	}

	return nil
}

type Address struct {
	City    string `json:"city,omitempty"`
	ZipCode string `json:"zip_code,omitempty"`
}

type House struct {
	ID        string    `json:"id,omitempty"`
	URL       string    `json:"url,omitempty"`
	Price     uint      `json:"price,omitempty"`
	Area      uint      `json:"area,omitempty"`
	Address   Address   `json:"address"`
	Interior  string    `json:"interior,omitempty"`
	OfferedAt time.Time `json:"offered_at"`
	CrawledAt time.Time `json:"crawled_at"`
}
