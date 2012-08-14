// Sample program that finds all the geo data in wikipedia pages.
package main

import (
	"compress/bzip2"
	"encoding/gob"
	"encoding/xml"
	"flag"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/dustin/go-wikiparse"
)

var numWorkers int
var parseCoords bool

var wg, errwg sync.WaitGroup

func parsePageCoords(p *wikiparse.Page, cherr chan<- *wikiparse.Page) {
	_, err := wikiparse.ParseCoords(p.Revision[0].Text)
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
		if parseCoords {
			parsePageCoords(p, cherr)
		}
		wg.Done()
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

func process(p wikiparse.Parser) {
	log.Printf("Got site info:  %+v", p.SiteInfo())

	ch := make(chan *wikiparse.Page, 1000)
	cherr := make(chan *wikiparse.Page, 10)

	for i := 0; i < numWorkers; i++ {
		go pageHandler(ch, cherr)
	}

	errwg.Add(1)
	go errorHandler(cherr)

	pages := int64(0)
	start := time.Now()
	prev := start
	reportfreq := int64(1000)
	var err error
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
	d := time.Since(start)
	log.Printf("Ended with err after %v:  %v after %s pages (%.2f p/s)",
		d, err, humanize.Comma(pages), float64(pages)/d.Seconds())
}

func processSingleStream(filename string) {
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

	process(p)
}

func processMultiStream(idx, data string) {
	p, err := wikiparse.NewIndexedParser(idx, data, runtime.GOMAXPROCS(0))
	if err != nil {
		log.Fatalf("Error initializing multistream parser: %v", err)
	}
	process(p)
}

func main() {
	var cpus int
	flag.IntVar(&numWorkers, "workers", 8, "Number of parsing workers")
	flag.IntVar(&cpus, "cpus", runtime.GOMAXPROCS(0), "Number of CPUS to utilize")
	flag.BoolVar(&parseCoords, "parseCoords", false,
		"Try to parse geo data while traversing")
	flag.Parse()

	runtime.GOMAXPROCS(cpus)

	switch flag.NArg() {
	case 1:
		processSingleStream(flag.Arg(0))
	case 2:
		processMultiStream(flag.Arg(0), flag.Arg(1))
	default:
		log.Fatalf("Need either a single stream dump, or index and multi-stream")
	}
}
