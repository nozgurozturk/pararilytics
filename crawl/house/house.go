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
	ZipCodeSelector   = ".listing-detail-summary__location"
	AddressSelector   = ".page__row--breadcrumbs > ol.breadcrumbs > li.breadcrumbs__item > a"
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

type House struct {
	ID      string `json:"id,omitempty" firestore:"id,omitempty"`
	URL     string `json:"url,omitempty" firestore:"url,omitempty"`
	Price   int    `json:"price,omitempty" firestore:"price,omitempty"`
	Area    int    `json:"area,omitempty" firestore:"area,omitempty"`
	Address struct {
		Province string `json:"province,omitempty" firestore:"province,omitempty"`
		City     string `json:"city,omitempty" firestore:"city,omitempty"`
		District string `json:"district,omitempty" firestore:"district,omitempty"`
		Street   string `json:"street,omitempty" firestore:"street,omitempty"`
		ZipCode  string `json:"zip_code,omitempty" firestore:"zip_code,omitempty"`
	} `json:"address" firestore:"address"`
	Interior  string    `json:"interior,omitempty" firestore:"interior,omitempty"`
	OfferedAt time.Time `json:"offered_at" firestore:"offered_at,omitempty"`
	CrawledAt time.Time `json:"crawled_at" firestore:"crawled_at,omitempty"`
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

	e.ForEach(AddressSelector, h.setAddressFromElements())

	if err := h.setZipCodeFromText(e.ChildText(ZipCodeSelector)); err != nil {
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
		err := errors.Errorf("text parts length must be more than one:  %s", priceText)
		return errors.WithMessage(err, PriceError)
	}
	priceReplacer := strings.NewReplacer(",", "", "€", "")

	pt := priceReplacer.Replace(priceTextParts[0])
	price, err := strconv.Atoi(pt)
	if err != nil {
		return errors.WithMessage(err, PriceError+priceText)
	}

	h.Price = price

	return nil
}

func (h *House) setIDFromURL(url *url.URL) error {
	path := strings.Split(url.Path, "/")
	if len(path) < 4 {
		err := errors.Errorf("path length must be more than four:  %s", url.Path)
		return errors.WithMessage(err, IDError)
	}

	h.ID = path[3]
	return nil
}

func (h *House) setAddressFromElements() func(i int, e *colly.HTMLElement) {
	return func(i int, e *colly.HTMLElement) {
		switch i {
		case 1:
			h.Address.Province = e.Text
		case 2:
			h.Address.City = e.Text
		case 3:
			h.Address.District = e.Text
		case 4:
			h.Address.Street = e.Text
		}
	}
}

func (h *House) setZipCodeFromText(zipCodeText string) error {
	addressTextParts := strings.Split(zipCodeText, "(")
	if len(addressTextParts) < 2 {
		err := errors.Errorf("addressTextParts length must be more than two:  %s", zipCodeText)
		return errors.WithMessage(err, AddressError)
	}

	zipCode := strings.TrimRight(addressTextParts[0], " ")

	h.Address.ZipCode = zipCode
	return nil
}

func (h *House) setAreaFromText(areaText string) error {
	areaTextParts := strings.Split(areaText, " ")
	if len(areaTextParts) < 1 {
		err := errors.Errorf("addressTextParts length must be more than two:  %s", areaText)
		return errors.WithMessage(err, AreaError)
	}
	area, err := strconv.Atoi(areaTextParts[0])
	if err != nil {
		return errors.WithMessage(err, AreaError+areaText)
	}
	h.Area = area

	return nil
}

func (h *House) setOfferDateFromText(offerText string) error {

	var offerDate time.Time
	var err error

	now := time.Now()

	if strings.Contains(offerText, "week") {

		offerTextParts := strings.Split(offerText, " ")
		week, e := strconv.Atoi(strings.TrimRight(offerTextParts[0], "+"))
		if e != nil {
			return errors.WithMessage(e, OfferDateError+offerText)
		}

		h.OfferedAt = now.AddDate(0, 0, -7*week)

		return nil
	}

	if strings.Contains(offerText, "month") {

		offerTextParts := strings.Split(offerText, " ")
		month, e := strconv.Atoi(strings.TrimRight(offerTextParts[0], "+"))
		if e != nil {
			return errors.WithMessage(e, OfferDateError+offerText)
		}

		h.OfferedAt = now.AddDate(0, -1*month, 0)

		return nil
	}

	offerDate, err = time.Parse("02-01-2006", offerText)
	if err != nil {
		return errors.WithMessage(err, OfferDateError+offerText)
	}

	h.OfferedAt = offerDate

	return nil
}
