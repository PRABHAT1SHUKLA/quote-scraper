package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

type Quote struct {
	Text   string
	Author string
}

func main() {
	pages := flag.Int("pages", 1, "Maximum no. of pages to parse")
	output := flag.String("output", "quotes.csv", "output file name")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")

	flag.Parse()

	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lshortfile)

	} else {
		log.SetOutput(io.Discard)
	}

	log.Println("Starting logs ...")

	quotesChan := make(chan Quote, 100)

	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

	hasNext := true
	currentPage := 1

	for hasNext && currentPage <= *pages {
		wg.Add(1)
		sem <- struct{}{}

		go func(page int) {
			defer wg.Done()
			defer func() { <-sem }()

			quotes, next, err := scrapePage(page)

			if err != nil {
				log.Printf("Error scraping page %d : %v", page, err)
				return
			}
			for _, q := range quotes {
				quotesChan <- q
			}

			hasNext = next
			time.Sleep(1 * time.Second)

		}(currentPage)
		currentPage++
	}

	go func() {
		wg.Wait()
		close(quotesChan)
	}()

	var allQuotes []Quote

	for q := range quotesChan {
		allQuotes = append(allQuotes, q)
	}

	if err := exportToCSV(*output, allQuotes); err != nil {
		log.Printf("Error exporting to csv %v", err)
		os.Exit(1)
	}

	fmt.Printf("Scraped %d quotes and saved to %s", len(allQuotes), *output)

}

func scrapePage(page int) ([]Quote, bool, error) {
	url := fmt.Sprintf("https://quotes.toscrape.com/page/%d/", page)
	log.Printf("Fetching page %d : %s", page, url)

	var resp *http.Response

	var err error

	for retries := 3; retries > 0; retries-- {
		resp, err = http.Get(url)
		if err == nil {
			break
		}

		log.Printf("Retry %d for page %d: %v", 4-retries, page, err)
		time.Sleep(time.Duration(4-retries) * time.Second)

	}

	if err != nil {
		return nil, false, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("status code %d", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)

	if err != nil {
		return nil, false, err
	}

	var quotes []Quote

	doc.Find(".quote").Each(func(i int, s *goquery.Selection) {
		quotes = append(quotes, Quote{
			Text:   s.Find(".text").Text(),
			Author: s.Find(".author").Text(),
		})
	})

	hasNext := doc.Find(".next").Length() > 1

	return quotes, hasNext, nil

}

func exportToCSV(fileName string, quotes []Quote) error {

	file, err := os.Create(fileName)

	if err != nil {
		return err
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"Quote", "Author"}); err != nil {
		return err
	}

	for _, q := range quotes {
		if err := writer.Write([]string{q.Text, q.Author}); err != nil {
			return err
		}
	}
	return nil
}
