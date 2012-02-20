package main

import (
	"compress/bzip2"
	"encoding/xml"
	"log"
	"os"

	"github.com/dustin/go-humanize"
)

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

func parsePage(d *xml.Decoder) error {
	page := Page{}
	err := d.Decode(&page)
	if err != nil {
		return err
	}
	// log.Printf("Got page:  %+v", page)
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

	pages := int64(0)
	for err == nil {
		err = parsePage(d)
		pages++
		if pages%1000 == 0 {
			log.Printf("Processed %s pages", humanize.Comma(pages))
		}
	}
	log.Printf("Ended with err:  %v after %s pages",
		err, humanize.Comma(pages))

}
