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
)

var wg, errwg sync.WaitGroup

type SiteInfo struct {
	SiteName   string `xml:"sitename"`
	Base       string `xml:"base"`
	Generator  string `xml:"generator"`
	Case       string `xml:"case"`
	Namespaces []struct {
		Key   string `xml:"key,attr"`
		Case  string `xml:"case,attr"`
		Value string `xml:",chardata"`
	} `xml:"namespaces>namespace"`
}

type Contributor struct {
	ID       uint64 `xml:"id"`
	Username string `xml:"username"`
}

type Revision struct {
	ID          int         `xml:"id"`
	Timestamp   string      `xml:"timestamp"`
	Contributor Contributor `xml:"contributor"`
	Comment     string      `xml:"comment"`
	Text        string      `xml:"text"`
}

type Page struct {
	Title    string   `xml:"title"`
	ID       uint64   `xml:"id"`
	Revision Revision `xml:"revision"`
}

func doPage(p *Page, cherr chan<- *Page) {
	defer wg.Done()
	_, err := ParseCoords(p.Revision.Text)
	if err == nil {
		// log.Printf("Found geo data in %q: %#v", p.Title, gl)
	} else {
		if err != NoCoordFound {
			cherr <- p
			log.Printf("Error parsing geo from %#v: %v", *p, err)
		}
	}

}

func pageHandler(ch <-chan *Page, cherr chan<- *Page) {
	for p := range ch {
		doPage(p, cherr)
	}
}

func parsePage(d *xml.Decoder, ch chan<- *Page) error {
	page := Page{}
	err := d.Decode(&page)
	if err != nil {
		return err
	}
	wg.Add(1)
	ch <- &page
	return nil
}

func errorHandler(ch <-chan *Page) {
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
	d := xml.NewDecoder(z)
	t, err := d.Token()
	if err != nil {
		log.Fatalf("Error reading first token: %v", err)
	}
	log.Printf("Read: %v", t)

	si := SiteInfo{}
	err = d.Decode(&si)
	if err != nil {
		log.Fatalf("Error decoding next thing:  %v", err)
	}
	log.Printf("Got site info:  %+v", si)

	ch := make(chan *Page, 1000)
	cherr := make(chan *Page, 10)

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
		err = parsePage(d, ch)
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
