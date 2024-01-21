package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"squeezetgz/internal"
)

// add flag to remove unclean files
var (
	cleanFlag = flag.Bool("rm", false, "remove unclean files")
)

// read file arg and call internal.Check on it
func main() {
	flag.Parse()
	if flag.NArg() < 2 {
		log.Fatal("usage: debug <reference tar file> <test tar files>")
	}
	if err := clean(flag.Arg(0), flag.Args()[1:]...); err != nil {
		log.Fatalf("error cleaning: %v", err)
	}
}

func clean(referenceTarPath string, testTarGzipPaths ...string) error {
	referenceTarFile, err := os.Open(referenceTarPath)
	if err != nil {
		return err
	}
	defer referenceTarFile.Close()

	referenceContents, err := internal.TarFileToEntries(referenceTarFile)
	if err != nil {
		return err
	}

	for _, testPath := range testTarGzipPaths {
		testfile, err := os.Open(testPath)
		if err != nil {
			return err
		}
		gzipReader, err := gzip.NewReader(testfile)
		if err != nil {
			return err
		}

		if err := internal.Check(gzipReader, referenceContents); err != nil {
			fmt.Printf("error checking: %s: %s\n", testPath, err.Error())
			if *cleanFlag {
				if err := os.Remove(testPath); err != nil {
					return err
				}
			}
		}
		fmt.Printf("clean: %s\n", testPath)
	}

	return nil
}
