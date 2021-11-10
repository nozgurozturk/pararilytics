package crawler

import (
	"fmt"
	"github.com/gocolly/colly"
	"log"
)

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
