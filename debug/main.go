package main

import (
	"flag"
	"fmt"
	"log"
	"maxtgz/internal"
)

// scans a tar.gz and prints the compression ratio for each file
func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("usage: debug <tar.gz file>")
	}
	for _, fn := range flag.Args() {
		if err := debug(fn); err != nil {
			log.Fatalf("error recompressing %s: %v", fn, err)
		}
	}
}

func debug(fn string) error {
	originalContents, err := internal.ReadOriginal(fn)
	if err != nil {
		return err
	}

	fmt.Printf("original contents: %d\n", len(originalContents))
	return nil
}
