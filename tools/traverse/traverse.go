// Sample program that finds all the geo data in wikipedia pages.
package main

import (
	"compress/bzip2"
	"encoding/gob"
	"encoding/xml"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/dustin/go-wikiparse"
)

var wg, errwg sync.WaitGroup

func doPage(p *wikiparse.Page, cherr chan<- *wikiparse.Page) {
	defer wg.Done()
	_, err := wikiparse.ParseCoords(p.Revision.Text)
	if err == nil {
		// log.Printf("Found geo data in %q: %#v", p.Title, gl)
	} else {
		if err != wikiparse.NoCoordFound {
			cherr <- p
			log.Printf("Error parsing geo from %#v: %v", *p, err)
		}
	}

}

func pageHandler(ch <-chan *wikiparse.Page, cherr chan<- *wikiparse.Page) {
	for p := range ch {
		doPage(p, cherr)
	}
}

func parsePage(d *xml.Decoder, ch chan<- *wikiparse.Page) error {
	page := wikiparse.Page{}
	err := d.Decode(&page)
	if err != nil {
		return err
	}
	wg.Add(1)
	ch <- &page
	return nil
}

func errorHandler(ch <-chan *wikiparse.Page) {
	defer errwg.Done()
	f, err := os.Create("errors.gob")
	if err != nil {
		log.Fatalf("Error creating error file: %v", err)
	}
	defer f.Close()
	g := gob.NewEncoder(f)

	for p := range ch {
		err = g.Encode(p)
		if err != nil {
			log.Fatalf("Error gobbing page: %v\n%#v", err, p)
		}
	}
}

func main() {
	filename := os.Args[1]
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

	log.Printf("Got site info:  %+v", p.SiteInfo)

	ch := make(chan *wikiparse.Page, 1000)
	cherr := make(chan *wikiparse.Page, 10)

	for i := 0; i < 8; i++ {
		go pageHandler(ch, cherr)
	}

	errwg.Add(1)
	go errorHandler(cherr)

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
	close(cherr)
	errwg.Wait()
	log.Printf("Ended with err after %v:  %v after %s pages",
		time.Now().Sub(start), err, humanize.Comma(pages))

}
