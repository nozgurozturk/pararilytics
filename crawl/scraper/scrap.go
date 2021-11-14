package scraper

import (
	"cloud.google.com/go/logging"
	"github.com/gocolly/colly"
	"github.com/nozgurozturk/pararilytics/crawl/house"
	"github.com/nozgurozturk/pararilytics/crawl/logger"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	URL_KEY             = "URL"
	USER_AGENT_KEY      = "USER_AGENT"
	ALLOWED_DOMAINS_KEY = "ALLOWED_DOMAINS"
	COOKIE_KEY          = "COOKIE"
)

const (
	ListItemSelector = "h2.listing-search-item__title > a[href]"
	DetailItemSelector = "main.page__main"
)

func ScrapHouses(pageNumber int) []house.House {
	url := os.Getenv(URL_KEY)
	cookie := os.Getenv(COOKIE_KEY)
	houses := make([]house.House, 0, 30)

	baseCollector := colly.NewCollector(
		colly.UserAgent(os.Getenv(USER_AGENT_KEY)),
		colly.AllowedDomains(strings.Split(os.Getenv(ALLOWED_DOMAINS_KEY), ",")...),
		colly.Async(true),
	)

	if err := baseCollector.Limit(&colly.LimitRule{
		Parallelism: 4,
		DomainGlob:  "*",
		RandomDelay: 10 * time.Second,
	}); err != nil {
		logger.NewEntry(logging.Error, "can not set limit rule for collector", err.Error())
		return nil
	}

	listCollector := baseCollector.Clone()
	detailCollector := baseCollector.Clone()

	listCollector.OnHTML(ListItemSelector, func(e *colly.HTMLElement) {
		houseURL := e.Request.AbsoluteURL(e.Attr("href"))

		if err := detailCollector.Visit(houseURL); err != nil {
			logger.NewEntry(logging.Error, "can not visit detail page of house", err.Error())
		}
	})

	detailCollector.OnHTML(DetailItemSelector, func(e *colly.HTMLElement) {
		h := &house.House{}
		h.BuildFromElement(e)
		houses = append(houses, *h)
	})

	listCollector.OnRequest(func(r *colly.Request) {
		r.Headers.Set("cookie", cookie)
		logger.NewEntry(logging.Info, "List Visiting: "+r.URL.String(), "")
	})

	detailCollector.OnRequest(func(r *colly.Request) {
		r.Headers.Set("cookie", cookie)
		logger.NewEntry(logging.Info, "Detail Visiting: "+r.URL.String(), "")
	})
	n := ""
	if pageNumber > 0 {
		n = strconv.Itoa(pageNumber)
	}
	pageURL := url
	if n != "" {
		pageURL += "/page-" + n
	}

	listCollector.Visit(pageURL)

	listCollector.Wait()
	detailCollector.Wait()

	return houses

}
