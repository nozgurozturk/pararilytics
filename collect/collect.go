package collect

import (
	"cloud.google.com/go/logging"
	"context"
	"fmt"
	"github.com/nozgurozturk/pararilytics/collect/house"
	"github.com/nozgurozturk/pararilytics/collect/logger"
	"github.com/nozgurozturk/pararilytics/collect/store"
	"log"
	"os"
)

type PubSubMessage struct {
	Data []byte `json:"data"`
}

const (
	PROJECT_ID_KEY              = "PROJECT_ID"
	FIRESTORE_COLLECTION_KEY    = "FIRESTORE_COLLECTION"
	FIRESTORE_SUBCOLLECTION_KEY = "FIRESTORE_SUBCOLLECTION"
)

func Collect(ctx context.Context, msg PubSubMessage) error {
	// Setup
	projectID := os.Getenv(PROJECT_ID_KEY)
	if projectID == "" {
		log.Printf("%s can not found in env variables", PROJECT_ID_KEY)
		return nil
	}

	if err := logger.New(ctx, projectID); err != nil {
		return err
	}
	logger.NewEntry(logging.Notice, "logger initialized", "")

	if err := store.New(ctx, projectID); err != nil {
		return err
	}
	logger.NewEntry(logging.Notice, "connected to the FireStore", "")

	// TearDown
	defer func() {
		if err := store.Client.Close(); err != nil {
			logger.NewEntry(logging.Error, "can not disconnected from the FireStore", err.Error())
		}
		logger.NewEntry(logging.Notice, "disconnected from the FireStore", "")
	}()

	// Houses
	houses, err := house.FromMessage(msg)

	if err != nil {
		logger.NewEntry(logging.Error, "can not parse houses from message", err.Error())
		return err
	}

	city := houses[0].Address.City

	// Store
	batch := store.Client.Batch()
	houseCollection := store.Client.
		Collection(os.Getenv(FIRESTORE_COLLECTION_KEY)).
		Doc(city).
		Collection(os.Getenv(FIRESTORE_SUBCOLLECTION_KEY))

	for _, house := range houses {
		if house.ID != "" {
			batch.Set(houseCollection.Doc(house.ID), house)
		}
	}

	if _, err := batch.Commit(ctx); err != nil {
		logger.NewEntry(logging.Error, "can not store houses into collection", err.Error())
		return err
	}

	logger.NewEntry(logging.Info, fmt.Sprintf("total %d houses of %s are stored into collection", len(houses), city), "")

	return nil
}