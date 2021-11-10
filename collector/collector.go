package collector

import (
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func StoreHouseFromCity(ctx context.Context, msg PubSubMessage) error {
	projectID := os.Getenv("PROJECT_ID")

	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
		return err
	}
	var houses []House

	if err := json.Unmarshal(msg.Data, &houses); err != nil {
		return fmt.Errorf("json.Unmarshal: %v", err)
	}

	if len(houses) < 1 {
		log.Printf("house length is 0")
		return nil
	}

	batch := client.Batch()

	houseCollection := client.Collection("cities").Doc(houses[0].Address.City).Collection("houses")

	for _, house := range houses {
		if house.ID != "" {
			doc := houseCollection.Doc(house.ID)
			batch.Set(doc, house)
		}
	}

	if _, err := batch.Commit(ctx); err != nil {
		log.Printf("An error occured %v \n", err)
		return err
	}

	return nil
}

type Address struct {
	City     string `json:"city,omitempty" firestore:"city,omitempty"`
	District string `json:"district,omitempty" firestore:"district,omitempty"`
	ZipCode  string `json:"zip_code,omitempty" firestore:"zip_code,omitempty"`
}

type House struct {
	ID        string    `json:"id,omitempty" firestore:"id,omitempty"`
	URL       string    `json:"url,omitempty" firestore:"url,omitempty"`
	Price     int       `json:"price,omitempty" firestore:"price,omitempty"`
	Area      int       `json:"area,omitempty" firestore:"area,omitempty"`
	Address   Address   `json:"address" firestore:"address"`
	Interior  string    `json:"interior,omitempty" firestore:"interior,omitempty"`
	OfferedAt time.Time `json:"offered_at" firestore:"offered_at,omitempty"`
	CrawledAt time.Time `json:"crawled_at" firestore:"crawled_at,omitempty"`
}
