package wikiparse

import (
	"compress/bzip2"
	"encoding/xml"
	"io"
	"log"
	"os"
	"sync"
)

type indexChunk struct {
	offset int64
	count  int
}

type multiStreamParser struct {
	siteInfo SiteInfo

	workerch chan indexChunk
	entries  chan *Page
}

func multiStreamIndexWorker(indexfn string, p *multiStreamParser) {
	defer close(p.workerch)

	r, err := os.Open(indexfn)
	if err != nil {
		log.Fatalf("Error opening %v: %v", indexfn, err)
	}
	defer r.Close()

	bz := bzip2.NewReader(r)

	isr, err := NewIndexSummaryReader(bz)
	if err != nil {
		log.Fatalf("Error creating index summary: %v", err)
	}
	for {
		offset, count, err := isr.Next()
		p.workerch <- indexChunk{offset, count}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error reading stream:  %v", err)
		}
	}
}

func multiStreamWorker(datafn string, wg *sync.WaitGroup,
	p *multiStreamParser) {
	defer wg.Done()

	r, err := os.Open(datafn)
	if err != nil {
		log.Fatalf("Error opening %v: %v", datafn, err)
	}
	defer r.Close()

	for idxChunk := range p.workerch {
		_, err := r.Seek(idxChunk.offset, 0)
		if err != nil {
			log.Fatalf("Error seeking to specified offset: %v", err)
		}
		bz := bzip2.NewReader(r)
		d := xml.NewDecoder(bz)

		for i := 0; i < idxChunk.count && err != io.EOF; i++ {
			newpage := new(Page)
			err = d.Decode(newpage)
			if err == nil {
				p.entries <- newpage
			}
		}
	}
}

// Get a wikipedia dump parser reading from the given reader.
func NewIndexedParser(indexfn, datafn string, numWorkers int) (Parser, error) {
	r, err := os.Open(datafn)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	bz := bzip2.NewReader(r)

	d := xml.NewDecoder(bz)
	_, err = d.Token()
	if err != nil {
		return nil, err
	}

	si := SiteInfo{}
	err = d.Decode(&si)
	if err != nil {
		return nil, err
	}

	rv := &multiStreamParser{
		siteInfo: si,
		workerch: make(chan indexChunk, 1000),
		entries:  make(chan *Page, 1000),
	}

	wg := sync.WaitGroup{}
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go multiStreamWorker(datafn, &wg, rv)
	}

	go multiStreamIndexWorker(indexfn, rv)

	go func() {
		wg.Wait()
		close(rv.entries)
	}()

	return rv, nil
}

func (p *multiStreamParser) Next() (rv *Page, err error) {
	var ok bool
	rv, ok = <-p.entries
	if !ok {
		return nil, io.EOF
	}
	return
}

func (p *multiStreamParser) SiteInfo() SiteInfo {
	return p.siteInfo
}
