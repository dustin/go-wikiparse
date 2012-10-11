// Load a wikipedia dump into CouchBase
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/dustin/go-humanize"
	"github.com/dustin/go-wikiparse"
)

var numWorkers = flag.Int("numWorkers", 8, "Number of page workers")

var wg sync.WaitGroup

func init() {
	flag.Usage = usage
}

func usage() {
	fmt.Fprintf(os.Stderr,
		"Usage:\n  %s [opts] wikipedia.index.bz2 wikipedia.xml.bz2\n",
		os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
	os.Exit(1)
}

type Geo struct {
	Geometry struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	} `json:"geometry"`
	Type string `json:"type"`
}

type Article struct {
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

func doPage(db *couchbase.Bucket, p *wikiparse.Page) {
	article := Article{Text: p.Revisions[0].Text}
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
	article.Files = wikiparse.FindFiles(article.Text)
	article.Links = wikiparse.FindLinks(article.Text)

	err = db.Set(p.Title, 0, article)
	if err != nil {
		log.Printf("Error setting %v: %v", p.Title, err)
		return
	}
}

func pageHandler(db *couchbase.Bucket, ch <-chan *wikiparse.Page) {
	defer wg.Done()
	for p := range ch {
		doPage(db, p)
	}
}

func main() {
	couchbaseServer := flag.String("couchbase", "http://localhost:8091/",
		"Couchbase URL")
	couchbaseBucket := flag.String("bucket", "default", "Couchbase bucket")
	procs := flag.Int("cpus", runtime.NumCPU(), "Number of CPUS to use")
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	db, err := couchbase.GetBucket(*couchbaseServer,
		"default", *couchbaseBucket)
	if err != nil {
		log.Fatalf("Error connecting to couchbase: %v", err)
	}

	p, err := wikiparse.NewIndexedParser(flag.Arg(0), flag.Arg(1),
		runtime.GOMAXPROCS(0))
	if err != nil {
		log.Fatalf("Error initializing multistream parser: %v", err)
	}

	ch := make(chan *wikiparse.Page, 1000)

	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
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
	close(ch)
	wg.Wait()
	log.Printf("Ended with err after %v:  %v after %s pages",
		time.Now().Sub(start), err, humanize.Comma(pages))

}
