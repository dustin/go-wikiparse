// Load a wikipedia dump into ElasticSearch
package main

import (
	"compress/bzip2"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dustin/go-elasticsearch"
	"github.com/dustin/go-humanize"
	"github.com/dustin/go-wikiparse"
)

var wg = sync.WaitGroup{}

func pageHandler(u string, ch chan *wikiparse.Page) {
	counter := 0
	es := elasticsearch.ElasticSearch{URL: u}
	bulkLoader := es.Bulk()

	for p := range ch {
		counter++
		if counter > 1000 {
			bulkLoader.SendBatch()
			counter = 0
		}
		ui := elasticsearch.UpdateInstruction{
			Id:    p.Title,
			Index: "wikipediax",
			Type:  "article",
			Body: map[string]interface{}{
				"author":    p.Revisions[0].Contributor.Username,
				"text":      p.Revisions[0].Text,
				"timestamp": p.Revisions[0].Timestamp,
			},
		}
		bulkLoader.Update(&ui)
	}
	bulkLoader.Quit()
}

func main() {
	filename, esurl := os.Args[1], os.Args[2]

	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()

	z := bzip2.NewReader(f)

	p, err := wikiparse.NewParser(z)
	if err != nil {
		log.Fatalf("Error setting up new page parser:  %v", err)
	}

	log.Printf("Got site info:  %+v", p.SiteInfo())

	ch := make(chan *wikiparse.Page, 1000)

	for i := 0; i < 4; i++ {
		go pageHandler(esurl, ch)
	}

	pages := int64(0)
	start := time.Now()
	prev := start
	reportfreq := int64(1000)
	for err == nil {
		var page *wikiparse.Page
		page, err = p.Next()
		if err == nil {
			wg.Add(1)
			ch <- page
		}

		pages++
		if pages%reportfreq == 0 {
			now := time.Now()
			d := now.Sub(prev)
			log.Printf("Processed %s pages total (%.2f/s)",
				humanize.Comma(pages), float64(reportfreq)/d.Seconds())
			prev = now
		}
	}
	wg.Wait()
	close(ch)
	log.Printf("Ended with err after %v:  %v after %s pages",
		time.Now().Sub(start), err, humanize.Comma(pages))

}
