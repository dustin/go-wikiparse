package main

import (
	"compress/bzip2"
	"encoding/xml"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

var wg = sync.WaitGroup{}

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

func doPage(p *Page) {
	defer wg.Done()
	gl, err := ParseCoords(p.Revision.Text)
	if err == nil {
		log.Printf("Found geo data in %q: %#v", p.Title, gl)
	} else {
		if err != NoCoordFound {
			log.Fatalf("Error parsing geo from %#v: %v", *p, err)
		}
	}

}

func pageHandler(ch <-chan *Page) {
	for p := range ch {
		doPage(p)
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

	for i := 0; i < 8; i++ {
		go pageHandler(ch)
	}

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
	log.Printf("Ended with err after %v:  %v after %s pages",
		time.Now().Sub(start), err, humanize.Comma(pages))

}
