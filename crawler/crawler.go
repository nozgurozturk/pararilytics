package crawler

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gocolly/colly"
	"github.com/pkg/errors"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
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

const (
	BaseUrl            = "https://www.pararius.com/apartments/"
	ListItemSelector   = "h2.listing-search-item__title > a[href]"
	DetailItemSelector = "article.page__row--listing"
)

func crawlHouses(city string) []House {
	houses := make([]House, 0, 30)
	baseCollector := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"),
		colly.AllowedDomains("pararius.com", "www.pararius.com"),
		colly.Async(true),
	)

	if err := baseCollector.Limit(&colly.LimitRule{
		Parallelism: 4,
		DomainGlob:  "*",
	}); err != nil {
		log.Fatal(err)
	}

	listCollector := baseCollector.Clone()
	detailCollector := baseCollector.Clone()

	listCollector.OnHTML(ListItemSelector, func(e *colly.HTMLElement) {
		houseURL := e.Request.AbsoluteURL(e.Attr("href"))
		if err := detailCollector.Visit(houseURL); err != nil {
			log.Fatal(err)
		}
	})

	detailCollector.OnHTML(DetailItemSelector, func(e *colly.HTMLElement) {
		house := House{}.BuildFromElement(e)
		houses = append(houses, house)
	})

	listCollector.OnRequest(func(r *colly.Request) {
		r.Headers.Set("cookie", "_ga=GA1.2.1120509150.1634401499; OptanonAlertBoxClosed=2021-10-16T16:25:01.984Z; _fbp=fb.1.1634401502493.2001439941; __gads=ID=6d84bcf744cc0bee:T=1634401604:S=ALNI_MY259i8swZRhUDNjLkW4ylqN7gwSw; eupubconsent-v2=CPOLeCbPOLeCbAcABBENB0CsAP_AAH_AAChQIJtf_X__b3_j-_59f_t0eY1P9_7_v-0zjhfdt-8N2f_X_L8X42M7vF36pq4KuR4Eu3LBIQdlHOHcTUmw6okVrzPsbk2Mr7NKJ7PEmnMbO2dYGH9_n93TuZKY7__8___z__-v_v____f_r-3_3__5_X---_e_V399zLv9____39nN___9ggoASYal5AF2ZY4Mm0aVQogRhWEh0AoAKKAYWiKwgZXBTsrgI9QQsAEJqAnAiBBiCjBgEAAgkASERASAHggEQBEAgABACpAQgAI2AQWAFgYBAAKAaFiBFAEIEhBkcFRymBAVItFBPZWAJQd7GmEIZb4EUCj-iowEazRAsDISFg5jgCQEvAAA.f_gAD_gAAAAA; _gid=GA1.2.1817022130.1636372619; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Nov+08+2021+14%3A07%3A18+GMT%2B0100+(Central+European+Standard+Time)&version=6.24.0&isIABGlobal=false&hosts=&consentId=6cc084ea-c366-4883-907b-9de968ebe8a2&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CSTACK42%3A1&geolocation=NL%3BZH&AwaitingReconsent=false")
		fmt.Println("Visiting", r.URL.String())
	})

	detailCollector.OnRequest(func(r *colly.Request) {
		r.Headers.Set("cookie", "_ga=GA1.2.1120509150.1634401499; OptanonAlertBoxClosed=2021-10-16T16:25:01.984Z; _fbp=fb.1.1634401502493.2001439941; __gads=ID=6d84bcf744cc0bee:T=1634401604:S=ALNI_MY259i8swZRhUDNjLkW4ylqN7gwSw; eupubconsent-v2=CPOLeCbPOLeCbAcABBENB0CsAP_AAH_AAChQIJtf_X__b3_j-_59f_t0eY1P9_7_v-0zjhfdt-8N2f_X_L8X42M7vF36pq4KuR4Eu3LBIQdlHOHcTUmw6okVrzPsbk2Mr7NKJ7PEmnMbO2dYGH9_n93TuZKY7__8___z__-v_v____f_r-3_3__5_X---_e_V399zLv9____39nN___9ggoASYal5AF2ZY4Mm0aVQogRhWEh0AoAKKAYWiKwgZXBTsrgI9QQsAEJqAnAiBBiCjBgEAAgkASERASAHggEQBEAgABACpAQgAI2AQWAFgYBAAKAaFiBFAEIEhBkcFRymBAVItFBPZWAJQd7GmEIZb4EUCj-iowEazRAsDISFg5jgCQEvAAA.f_gAD_gAAAAA; _gid=GA1.2.1817022130.1636372619; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Nov+08+2021+14%3A07%3A18+GMT%2B0100+(Central+European+Standard+Time)&version=6.24.0&isIABGlobal=false&hosts=&consentId=6cc084ea-c366-4883-907b-9de968ebe8a2&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CSTACK42%3A1&geolocation=NL%3BZH&AwaitingReconsent=false")
		fmt.Println("Visiting", r.URL.String())
	})

	listCollector.Visit(BaseUrl + city)

	listCollector.Wait()
	detailCollector.Wait()

	return houses

}

const (
	PriceSelector     = ".listing-features__description--for_rent_price > span"
	AddressSelector   = ".listing-detail-summary__location"
	AreaSelector      = ".listing-features__description--surface_area > span"
	OfferDateSelector = ".listing-features__description--offered_since > span"
	InteriorSelector  = ".listing-features__description--interior > span"
)

const (
	PriceError     = "price_error:"
	IDError        = "id_error:"
	AddressError   = "address_error:"
	AreaError      = "area_error:"
	OfferDateError = "offerDate_error:"
)

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

func (h House) BuildFromElement(e *colly.HTMLElement) House {

	if err := h.setIDFromURL(e.Request.URL); err != nil {
		log.Fatal(err)
	}

	h.URL = e.Request.URL.String()

	if err := h.setPriceFromText(e.ChildText(PriceSelector)); err != nil {
		log.Fatal(err)
	}

	if err := h.setAddressFromText(e.ChildText(AddressSelector)); err != nil {
		log.Fatal(err)
	}

	if err := h.setAreaFromText(e.ChildText(AreaSelector)); err != nil {
		log.Fatal(err)
	}

	if err := h.setOfferDateFromText(e.ChildText(OfferDateSelector)); err != nil {
		log.Fatal(err)
	}

	h.Interior = e.ChildText(InteriorSelector)
	h.CrawledAt = time.Now()

	return h

}

func (h House) setPriceFromText(priceText string) error {
	priceTextParts := strings.Split(priceText, " ")
	if len(priceTextParts) < 1 {
		err := errors.Errorf("text parts length must be more than one:  %d", len(priceTextParts))
		return errors.WithMessage(err, PriceError)
	}
	priceReplacer := strings.NewReplacer(",", "", "€", "")

	price, err := strconv.Atoi(priceReplacer.Replace(priceTextParts[0]))
	if err != nil {
		return errors.WithMessage(err, PriceError)
	}

	h.Price = uint(price)

	return nil
}

func (h House) setIDFromURL(url *url.URL) error {
	path := strings.Split(url.Path, "/")
	if len(path) < 4 {
		err := errors.Errorf("path length must be more than four:  %d", len(path))
		return errors.WithMessage(err, IDError)
	}

	h.ID = path[3]
	return nil
}

func (h House) setAddressFromText(addressText string) error {
	addressTextParts := strings.Split(addressText, "(")
	if len(addressTextParts) < 2 {
		err := errors.Errorf("addressTextParts length must be more than two:  %d", len(addressTextParts))
		return errors.WithMessage(err, AddressError)
	}

	zipCode := strings.TrimRight(addressTextParts[0], " ")
	city := strings.TrimRight(addressTextParts[1], ")")

	address := Address{
		City:    city,
		ZipCode: zipCode,
	}

	h.Address = address
	return nil
}

func (h House) setAreaFromText(areaText string) error {
	areaTextParts := strings.Split(areaText, " ")
	if len(areaTextParts) < 1 {
		err := errors.Errorf("addressTextParts length must be more than two:  %d", len(areaTextParts))
		return errors.WithMessage(err, AreaError)
	}
	area, err := strconv.Atoi(areaTextParts[0])
	if err != nil {
		return errors.WithMessage(err, AreaError)
	}
	h.Area = uint(area)

	return nil
}


func (h House) setOfferDateFromText(offerText string) error {
	offerDate, err := time.Parse("02-01-2006", offerText)
	if err != nil {
		return errors.WithMessage(err, OfferDateError)
	}

	h.OfferedAt = offerDate

	return nil
}
