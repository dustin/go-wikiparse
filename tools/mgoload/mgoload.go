package main

import (
	"compress/bzip2"
	"flag"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/dustin/go-wikiparse"
	"labix.org/v2/mgo"
)

var proc = flag.Int("proc", 8, "How many processes to run")
var file = flag.String("file", "", "The bz2 dump file")
var cpus = flag.Int("cpus", runtime.NumCPU(), "Number of CPUs to use")
var dburl = flag.String("dburl", "localhost", "The dburl(s). I.e. localhost")
var verbose = flag.Bool("v", false, "Verbose logging?")

var wg sync.WaitGroup

// We want unique titles and htey should be since the title is the URL path
// in wikimedia My Title => My_Title
var titleIndex = mgo.Index{
	Key:        []string{"title"},
	Unique:     true,
	DropDups:   true,
	Background: true,
	Sparse:     true,
}

type article struct {
	ID      string `json:"_id", omitempty`
	Title   string `"json: title"`
	Rev     string `"json: rev"`
	RevInfo struct {
		ID            uint64 "json: id,omitempty"
		Timestamp     string ",omitempty"
		Contributor   string ",omitempty"
		ContributorID uint64 ",omitempty"
		Comment       string ",omitempty"
	}
	Text  string   ",omitempty"
	Files []string ",omitempty"
	Links []string ",omitempty"
}

func pageHandler(db *mgo.Database, ch <-chan *wikiparse.Page) {
	for p := range ch {
		makeArticle(db, p)
	}
	wg.Done()
}

func makeArticle(db *mgo.Database, p *wikiparse.Page) {
	a := article{}
	a.RevInfo.ID = p.Revisions[0].ID
	a.RevInfo.Timestamp = p.Revisions[0].Timestamp
	a.RevInfo.Contributor = p.Revisions[0].Contributor.Username
	a.RevInfo.ContributorID = p.Revisions[0].Contributor.ID
	a.RevInfo.Comment = p.Revisions[0].Comment

	a.Title = p.Title
	a.Text = p.Revisions[0].Text
	a.Links = wikiparse.FindLinks(a.Text)
	a.Files = wikiparse.FindFiles(a.Text)
	err := db.C("articles").Insert(&a)
	if err != nil {
		if mgo.IsDup(err) {
			if *verbose == true {
				log.Printf("Duplicate Key Error inserting %s", a.Title)
			}
		} else {
			log.Printf("Error inserting %s: %s", a.Title, err)
		}
	}
}

func processDump(p wikiparse.Parser, db *mgo.Database) {
	ch := make(chan *wikiparse.Page, 10000)
	for i := 0; i < *proc; i++ {
		go pageHandler(db, ch)
	}

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
			log.Printf("Processed %s pages total (%.2f/s): Queue Len: %d",
				humanize.Comma(pages), float64(reportfreq)/d.Seconds(), len(ch))
			prev = now
		}
	}
	wg.Wait()
	close(ch)

	d := time.Since(start)
	log.Printf("Ended with err after %v:  %v after %s pages (%.2f p/s)",
		d, err, humanize.Comma(pages), float64(pages)/d.Seconds())
}

func main() {
	flag.Parse()
	if *file == "" {
		log.Fatal("You must supply a bz2 dump file.")
	}
	session, err := mgo.Dial(*dburl)
	if err != nil {
		panic(err)
	}

	f, err := os.Open(*file)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()

	z := bzip2.NewReader(f)

	p, err := wikiparse.NewParser(z)
	if err != nil {
		log.Fatalf("Error setting up new page parser:  %v", err)
	}

	err = session.DB("wp").C("articles").EnsureIndex(titleIndex)
	if err != nil {
		log.Fatal("Error creating title index", err)
	}
	processDump(p, session.DB("wp"))
}
