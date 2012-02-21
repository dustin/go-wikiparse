# go-wikiparse

If you're like me, then you enjoy playing with lots of textual data
and scour the internet for sources of it.

[mediawiki's dumps][dumps] are a pretty awesome chunk that's fun to
work with.

## Installation

    go get github.com/dustin/go-wikiparse

## Usage

The parser takes any `io.Reader` as a source assuming it's a complete
XML dump and lets you pull `wikiparse.Page` objects out of it.  These
typically arrive as `bzip2` files, so I make my program open the file
and set up a bzip reader over it and all that.  But you don't need to
do that if you want to read off of `stdin`.  Here's a complete example
that emits page titles from a decompressing stream on stdin:

    package main

    import (
    	"fmt"
    	"os"

    	"github.com/dustin/go-wikiparse"
    )

    func main() {
    	p, err := wikiparse.NewParser(os.Stdin)
    	if err != nil {
    		fmt.Fprintf(os.Stderr, "Error setting up parser", err)
    		os.Exit(1)
    	}

    	for err == nil {
    		var page *wikiparse.Page
    		page, err = p.Next()
    		if err == nil {
    			fmt.Println(page.Title)
    		}
    	}
    }

Example invocation:

    bzcat enwiki-20120211-pages-articles.xml.bz2 | ./sample

## Geographical Information

Because it's interesting to me, I wrote a parser for the
[wikiproject geographical coordinates][geo] that are found on many
pages.  Use this on the page's content to find out if it's a place or
not.  Then go there.

[dumps]: http://meta.wikimedia.org/wiki/Data_dumps
[geo]: http://en.wikipedia.org/wiki/Wikipedia:WikiProject_Geographical_coordinates
