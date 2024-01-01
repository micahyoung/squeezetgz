package main

import (
	"flag"
	"fmt"
	"log"
	"squeezetgz/internal"
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

	totalFactor := int64(0)
	for i := 0; i < len(originalContents)-1; i++ {
		testPerm := []int{i, i + 1}
		compressionFactor, _ := internal.RewritePermToBuffer(testPerm, originalContents, map[string]int64{}, map[string]int64{})
		fmt.Printf("%d :\n", compressionFactor)
		for _, j := range testPerm {
			fmt.Printf("  %s\n", originalContents[j].Header.Name)
		}

		totalFactor += compressionFactor
	}
	fmt.Printf("total: %d\n", totalFactor)

	return nil
}
