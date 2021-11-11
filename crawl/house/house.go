package house

import (
	"cloud.google.com/go/logging"
	"github.com/gocolly/colly"
	"github.com/nozgurozturk/pararilytics/crawl/logger"
	"github.com/pkg/errors"
	"net/url"
	"strconv"
	"strings"
	"time"
)

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
	City     string `json:"city,omitempty"`
	District string `json:"district,omitempty"`
	ZipCode  string `json:"zip_code,omitempty"`
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

func (h *House) BuildFromElement(e *colly.HTMLElement) {
	defer func() {
		if err := recover(); err != nil {
			msg := err.(error).Error()
			logger.NewEntry(logging.Error, "can not build house from element", msg)
		}
	}()

	if err := h.setIDFromURL(e.Request.URL); err != nil {
		panic(err)
	}

	h.URL = e.Request.URL.String()

	if err := h.setPriceFromText(e.ChildText(PriceSelector)); err != nil {
		panic(err)
	}

	if err := h.setAddressFromText(e.ChildText(AddressSelector)); err != nil {
		panic(err)
	}

	if err := h.setAreaFromText(e.ChildText(AreaSelector)); err != nil {
		panic(err)
	}

	if err := h.setOfferDateFromText(e.ChildText(OfferDateSelector)); err != nil {
		panic(err)
	}

	h.Interior = e.ChildText(InteriorSelector)
	h.CrawledAt = time.Now()

}

func (h *House) setPriceFromText(priceText string) error {
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

func (h *House) setIDFromURL(url *url.URL) error {
	path := strings.Split(url.Path, "/")
	if len(path) < 4 {
		err := errors.Errorf("path length must be more than four:  %d", len(path))
		return errors.WithMessage(err, IDError)
	}

	h.ID = path[3]
	return nil
}

func (h *House) setAddressFromText(addressText string) error {
	addressTextParts := strings.Split(addressText, "(")
	if len(addressTextParts) < 2 {
		err := errors.Errorf("addressTextParts length must be more than two:  %d", len(addressTextParts))
		return errors.WithMessage(err, AddressError)
	}

	zipCode := strings.TrimRight(addressTextParts[0], " ")
	district := strings.TrimRight(addressTextParts[1], ")")

	address := Address{
		City:     h.Address.City,
		District: district,
		ZipCode:  zipCode,
	}

	h.Address = address
	return nil
}

func (h *House) setAreaFromText(areaText string) error {
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

func (h *House) setOfferDateFromText(offerText string) error {
	offerDate, err := time.Parse("02-01-2006", offerText)
	if err != nil {
		return errors.WithMessage(err, OfferDateError)
	}

	h.OfferedAt = offerDate

	return nil
}
