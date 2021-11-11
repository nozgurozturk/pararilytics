package logger

import (
	"cloud.google.com/go/logging"
	"context"
	"log"
)

const (
	loggerID = "collect"
)

var (
	Client *logging.Client
	logger *logging.Logger
)

func New(ctx context.Context, projectID string) error {
	var err error
	Client, err = logging.NewClient(ctx, projectID)
	if err != nil {
		log.Println("logging.NewClient: " + err.Error())
		return err
	}

	logger = Client.Logger(loggerID)
	return nil
}

func NewEntry(severity logging.Severity, message, record string) {
	defer func() {
		if err := logger.Flush(); err != nil {
			log.Printf("logger.Flush %s: ", err.Error())
		}
	}()

	logger.Log(logging.Entry{
		Severity: severity,
		Payload: map[string]string{
			"message": message,
			"record":  record,
		},
	})
}
