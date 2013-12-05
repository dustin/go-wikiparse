// Load a wikipedia dump into CouchDB
package main

import (
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-couch"
	"github.com/dustin/go-humanize"
	"github.com/dustin/go-wikiparse"
)

var wg sync.WaitGroup

type Geo struct {
	Geometry struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	} `json:"geometry"`
	Type string `json:"type"`
}

type Article struct {
	ID      string `json:"_id"`
	Rev     string `json:"_rev"`
	RevInfo struct {
		ID            uint64 `json:"id"`
		Timestamp     string `json:"timestamp"`
		Contributor   string `json:"contributor"`
		ContributorId uint64 `json:"contributorid"`
		Comment       string `json:"comment"`
	} `json:"revinfo"`
	Text  string   `json:"text"`
	Geo   *Geo     `json:"geo,omitempty"`
	Files []string `json:"files,omitempty"`
	Links []string `json:"links,omitempty"`
}

func escapeTitle(in string) string {
	return strings.Replace(strings.Replace(in, "/", "%2f", -1),
		"+", "%2b", -1)
}

func resolveConflict(db *couch.Database, a *Article) {
	log.Printf("Resolving conflict on %s", a.ID)
	var prev Article
	err := db.Retrieve(escapeTitle(a.ID), &prev)
	if err != nil {
		log.Printf("  Error retrieving existing %v: %v", a.ID, err)
		return
	}
	if prev.Rev == "" {
		log.Printf("Got no rev from %v", a.ID)
		return
	}
	if a.RevInfo.Timestamp > prev.RevInfo.Timestamp {
		log.Printf("  This one is newer...replacing %s.", prev.Rev)
		_, err = db.EditWith(a, a.ID, prev.Rev)
		if err != nil {
			log.Printf("  Error updating %v: %v", prev.ID, err)
		}
	}
}

func doPage(db *couch.Database, p *wikiparse.Page) {
	defer wg.Done()
	article := Article{}
	gl, err := wikiparse.ParseCoords(p.Revisions[0].Text)
	if err == nil {
		article.Geo = &Geo{Type: "Feature"}
		article.Geo.Geometry.Type = "Point"
		article.Geo.Geometry.Coordinates = []float64{gl.Lon, gl.Lat}
	}
	article.RevInfo.ID = p.Revisions[0].ID
	article.RevInfo.Timestamp = p.Revisions[0].Timestamp
	article.RevInfo.Contributor = p.Revisions[0].Contributor.Username
	article.RevInfo.ContributorId = p.Revisions[0].Contributor.ID
	article.RevInfo.Comment = p.Revisions[0].Comment
	article.Text = p.Revisions[0].Text
	article.ID = escapeTitle(p.Title)
	article.Files = wikiparse.FindFiles(article.Text)
	article.Links = wikiparse.FindLinks(article.Text)

	_, _, err = db.Insert(&article)
	httpe, isHttpError := err.(*couch.HTTPError)
	switch {
	case err == nil:
		// yay
	case isHttpError && httpe.Status == 409:
		resolveConflict(db, &article)
	default:
		log.Printf("Error inserting %#v: %v", article, err)
	}
}

func pageHandler(db couch.Database, ch <-chan *wikiparse.Page) {
	for p := range ch {
		doPage(&db, p)
	}
}

func main() {
	dburl, idx, file := os.Args[1], os.Args[2], os.Args[3]

	db, err := couch.Connect(dburl)
	if err != nil {
		log.Fatalf("Error connecting to couchdb: %v", err)
	}

	p, err := wikiparse.NewIndexedParser(idx, file, runtime.GOMAXPROCS(0))
	if err != nil {
		log.Fatalf("Error initializing multistream parser: %v", err)
	}

	log.Printf("Got site info:  %+v", p.SiteInfo())

	ch := make(chan *wikiparse.Page, 1000)

	for i := 0; i < 20; i++ {
		go pageHandler(db, ch)
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
