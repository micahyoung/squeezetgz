package main

import (
	"archive/tar"
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
	partialCache, totalCache := map[int]int64{}, map[int]int64{}
	for i := 1; i < len(originalContents)-2; i++ {
		testPerm := []int{i - 1, i}
		partialCompressionFactor, _ := internal.RewritePermToBuffer(testPerm, originalContents, true, partialCache)
		totalCompressionFactor, _ := internal.RewritePermToBuffer(testPerm, originalContents, false, totalCache)
		fmt.Printf("  %s (%s) %d:%d\n", originalContents[i].Header.Name, getTypeShort(originalContents[i].Header.Typeflag), partialCompressionFactor, totalCompressionFactor)

		totalFactor += totalCompressionFactor
	}
	fmt.Printf("total: %d\n", totalFactor)

	return nil
}

func getTypeShort(t byte) string {
	switch t {
	case tar.TypeReg:
		return "f"
	case tar.TypeDir:
		return "d"
	case tar.TypeSymlink:
		return "l"
	case tar.TypeChar:
		return "c"
	case tar.TypeBlock:
		return "b"
	case tar.TypeFifo:
		return "p"
	case tar.TypeCont:
		return "c"
	case tar.TypeXHeader:
		return "x"
	case tar.TypeXGlobalHeader:
		return "x"
	case tar.TypeGNULongName:
		return "g"
	case tar.TypeGNULongLink:
		return "g"
	default:
		return "?"
	}
}
