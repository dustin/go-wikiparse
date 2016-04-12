package wikiparse

import (
	"encoding/xml"
	"io"
)

// SiteInfo is the toplevel site info describing basic dump properties.
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

// A Contributor is a user who contributed a revision.
type Contributor struct {
	ID       uint64 `xml:"id"`
	Username string `xml:"username"`
}

// A Redirect to another Page.
type Redirect struct {
	Title string `xml:"title,attr"`
}

// A Revision to a page.
type Revision struct {
	ID          uint64      `xml:"id"`
	Timestamp   string      `xml:"timestamp"`
	Contributor Contributor `xml:"contributor"`
	Comment     string      `xml:"comment"`
	Text        string      `xml:"text"`
}

// A Page in the wiki.
type Page struct {
	Title     string     `xml:"title"`
	ID        uint64     `xml:"id"`
	Redir     Redirect   `xml:"redirect"`
	Revisions []Revision `xml:"revision"`
	Ns        uint64     `xml:"ns"`
}

// A Parser emits wiki pages.
type Parser interface {
	// Get the next page from the parser
	Next() (*Page, error)
	// Get the toplevel site info from the stream
	SiteInfo() SiteInfo
}

type singleStreamParser struct {
	siteInfo SiteInfo
	x        *xml.Decoder
}

// NewParser gets a wikipedia dump parser reading from the given
// reader.
func NewParser(r io.Reader) (Parser, error) {
	d := xml.NewDecoder(r)
	_, err := d.Token()
	if err != nil {
		return nil, err
	}

	si := SiteInfo{}
	err = d.Decode(&si)
	if err != nil {
		return nil, err
	}

	return &singleStreamParser{
		siteInfo: si,
		x:        d,
	}, nil
}

func (p *singleStreamParser) Next() (*Page, error) {
	rv := &Page{}
	return rv, p.x.Decode(rv)
}

func (p *singleStreamParser) SiteInfo() SiteInfo {
	return p.siteInfo
}
