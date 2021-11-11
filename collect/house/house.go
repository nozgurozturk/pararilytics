package house

import (
	"encoding/json"
	"github.com/nozgurozturk/pararilytics/collect"
	"github.com/pkg/errors"
	"time"
)

type House struct {
	ID      string `json:"id,omitempty" firestore:"id,omitempty"`
	URL     string `json:"url,omitempty" firestore:"url,omitempty"`
	Price   int    `json:"price,omitempty" firestore:"price,omitempty"`
	Area    int    `json:"area,omitempty" firestore:"area,omitempty"`
	Address struct {
		City     string `json:"city,omitempty" firestore:"city,omitempty"`
		District string `json:"district,omitempty" firestore:"district,omitempty"`
		ZipCode  string `json:"zip_code,omitempty" firestore:"zip_code,omitempty"`
	} `json:"address" firestore:"address"`
	Interior  string    `json:"interior,omitempty" firestore:"interior,omitempty"`
	OfferedAt time.Time `json:"offered_at" firestore:"offered_at,omitempty"`
	CrawledAt time.Time `json:"crawled_at" firestore:"crawled_at,omitempty"`
}

func FromMessage(msg collect.PubSubMessage) ([]House, error) {
	var houses []House

	if err := json.Unmarshal(msg.Data, &houses); err != nil {
		err = errors.WithMessage(err, "can not parse houses from pub/sub message")
		return nil, err
	}

	if len(houses) < 1 {
		return nil, errors.New("not found house from pub/sub message")
	}

	return houses, nil
}
