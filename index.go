package wikiparse

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// An IndexEntry is an individual article from the index.
type IndexEntry struct {
	StreamOffset int64
	PageOffset   int
	ArticleName  string
}

func (i IndexEntry) String() string {
	return fmt.Sprintf("%v:%v:%v",
		i.StreamOffset, i.PageOffset, i.ArticleName)
}

// An IndexReader is a wikipedia multistream index reader.
type IndexReader struct {
	r          *bufio.Scanner
	base       int64
	prevOffset int64
}

// Next gets the next entry from the index stream.
//
// This assumes the numbers were meant to be incremental.
func (ir *IndexReader) Next() (IndexEntry, error) {
	if !ir.r.Scan() {
		err := ir.r.Err()
		if err == nil {
			err = io.EOF
		}
		return IndexEntry{}, err
	}
	parts := strings.SplitN(ir.r.Text(), ":", 3)
	if len(parts) != 3 {
		return IndexEntry{}, errors.New("bad record")
	}
	rv := IndexEntry{ArticleName: parts[2]}
	offset, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return IndexEntry{}, err
	}
	if offset < ir.prevOffset {
		ir.base += (1 << 32)
	}
	rv.StreamOffset = offset + ir.base
	i, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return IndexEntry{}, err
	}
	rv.PageOffset = int(i)
	ir.prevOffset = offset

	return rv, nil
}

// NewIndexReader gets a wikipedia index reader.
func NewIndexReader(r io.Reader) *IndexReader {
	return &IndexReader{r: bufio.NewScanner(r)}
}

// IndexSummaryReader gets offsets and counts from an index.
//
// If you don't want to know the individual articles, just how many
// and where, this is for you.
type IndexSummaryReader struct {
	index      *IndexReader
	prevOffset int64
	count      int
}

// NewIndexSummaryReader gets a new IndexSummaryReader from the given
// stream of index lines.
func NewIndexSummaryReader(r io.Reader) (rv *IndexSummaryReader, err error) {
	rv = &IndexSummaryReader{index: NewIndexReader(r)}
	first, err := rv.index.Next()
	if err != nil {
		return nil, err
	}
	rv.prevOffset = first.StreamOffset
	rv.count = 1

	return rv, nil
}

// Next gets the next offset and count from the index summary reader.
//
// Note that the last returns io.EOF as an error, but a valid offset
// and count.
func (isr *IndexSummaryReader) Next() (offset int64, count int, err error) {
	for {
		e, err := isr.index.Next()
		if err != nil {
			offset = isr.prevOffset
			count = isr.count
			isr.prevOffset = 0
			isr.count = 0
			return offset, count, err
		}

		if e.StreamOffset != isr.prevOffset {
			offset = isr.prevOffset
			count = isr.count
			isr.prevOffset = e.StreamOffset
			isr.count = 1
			return offset, count, nil
		}
		isr.count++
	}
}
