package wikiparse

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// An individual article from the index.
type IndexEntry struct {
	StreamOffset int64
	PageOffset   int
	ArticleName  string
}

func (i IndexEntry) String() string {
	return fmt.Sprintf("%v:%v:%v",
		i.StreamOffset, i.PageOffset, i.ArticleName)
}

// A wikipedia multistream index reader.
type IndexReader struct {
	r          *bufio.Reader
	base       int64
	prevOffset int64
}

// Get the next entry from the index stream.
//
// This assumes the numbers were meant to be incremental.
func (ir *IndexReader) Next() (rv IndexEntry, err error) {
	lb, isPrefix, err := ir.r.ReadLine()
	if err != nil {
		return rv, err
	}
	if isPrefix {
		return rv, errors.New("Partial read")
	}
	parts := strings.SplitN(string(lb), ":", 3)
	if len(parts) != 3 {
		return rv, errors.New("Bad record")
	}
	rv.ArticleName = parts[2]
	offset, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return rv, err
	}
	if offset < ir.prevOffset {
		ir.base += (1 << 32)
	}
	rv.StreamOffset = offset + ir.base
	i, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return rv, err
	}
	rv.PageOffset = int(i)
	ir.prevOffset = offset

	return rv, nil
}

// Get a wikipedia index reader.
func NewIndexReader(r io.Reader) *IndexReader {
	return &IndexReader{r: bufio.NewReader(r)}
}
