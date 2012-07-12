package main

import (
	"compress/bzip2"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/dustin/go-wikiparse"
)

func main() {
	r, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("Error opening %v: %v", os.Args[1], err)
	}
	defer r.Close()

	bz := bzip2.NewReader(r)

	ir := wikiparse.NewIndexReader(bz)
	for {
		e, err := ir.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error reading stream:  %v", err)
		}

		fmt.Println(e.String())
	}
}
