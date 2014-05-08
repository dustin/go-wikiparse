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

func multiStreamIndexWorker(r io.ReadCloser, p *multiStreamParser) {
	defer close(p.workerch)
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

func multiStreamWorker(src IndexedParseSource, wg *sync.WaitGroup,
	p *multiStreamParser) {
	defer wg.Done()

	r, err := src.OpenData()
	if err != nil {
		log.Fatalf("Error opening data: %v", err)
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
			newpage := &Page{}
			err = d.Decode(newpage)
			if err == nil {
				p.entries <- newpage
			}
		}
	}
}

// ReadSeekCloser is io.ReadSeeker + io.Closer.
type ReadSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

// An IndexedParseSource provides access to a multistream xml dump and
// its index.
//
// This is typically downloaded as two files, but a seekable interface
// such as HTTP with range requests can also serve.
type IndexedParseSource interface {
	OpenIndex() (io.ReadCloser, error)
	OpenData() (ReadSeekCloser, error)
}

type filesSource struct {
	idxfile, datafile string
}

func (f filesSource) OpenIndex() (io.ReadCloser, error) {
	return os.Open(f.idxfile)
}

func (f filesSource) OpenData() (ReadSeekCloser, error) {
	return os.Open(f.datafile)
}

// NewIndexedParserFromSrc creates a Parser that can parse multiple
// pages concurrently from a single source.
func NewIndexedParserFromSrc(src IndexedParseSource, numWorkers int) (Parser, error) {
	r, err := src.OpenData()
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
		go multiStreamWorker(src, &wg, rv)
	}

	ridx, err := src.OpenIndex()
	if err != nil {
		log.Fatalf("Error opening index: %v", err)
	}

	go multiStreamIndexWorker(ridx, rv)

	go func() {
		wg.Wait()
		close(rv.entries)
	}()

	return rv, nil
}

// NewIndexedParser gets an indexed/parallel wikipedia dump parser
// from the given index and data files.
func NewIndexedParser(indexfn, datafn string, numWorkers int) (Parser, error) {
	return NewIndexedParserFromSrc(filesSource{indexfn, datafn}, numWorkers)
}

func (p *multiStreamParser) Next() (*Page, error) {
	rv, ok := <-p.entries
	if !ok {
		return nil, io.EOF
	}
	return rv, nil
}

func (p *multiStreamParser) SiteInfo() SiteInfo {
	return p.siteInfo
}
