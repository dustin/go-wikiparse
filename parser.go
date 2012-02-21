// A library to understand the wikipedia xml dump format.
package wikiparse

import (
	"encoding/xml"
	"io"
)

// The toplevel site info describing basic dump properties.
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

// A user who contributed a revision.
type Contributor struct {
	ID       uint64 `xml:"id"`
	Username string `xml:"username"`
}

// A revision to a page.
type Revision struct {
	ID          uint64      `xml:"id"`
	Timestamp   string      `xml:"timestamp"`
	Contributor Contributor `xml:"contributor"`
	Comment     string      `xml:"comment"`
	Text        string      `xml:"text"`
}

// A wiki page.
type Page struct {
	Title    string   `xml:"title"`
	ID       uint64   `xml:"id"`
	Revision Revision `xml:"revision"`
}

// That which emits wiki pages.
type Parser struct {
	// The toplevel site info.
	SiteInfo SiteInfo
	x        *xml.Decoder
}

// Get a wikipedia dump parser reading from the given reader.
func NewParser(r io.Reader) (*Parser, error) {
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

	return &Parser{
		SiteInfo: si,
		x:        d,
	}, nil
}

// Get the next page from the parser
func (p *Parser) Next() (rv *Page, err error) {
	rv = new(Page)
	err = p.x.Decode(rv)
	return
}
