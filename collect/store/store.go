package store

import (
	"cloud.google.com/go/firestore"
	"context"
	"log"
)

var (
	Client *firestore.Client
)

func New(ctx context.Context, projectID string) error {
	var err error
	Client, err = firestore.NewClient(ctx, projectID)
	if err != nil {
		log.Println("fireStore.NewClient: " + err.Error())
		return err
	}
	return nil
}
