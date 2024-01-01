// recompresses tar gz files by varying their order to maximize compression
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type tarEntry struct {
	header  *tar.Header
	content []byte
}

var (
	// flags
	verbose         = flag.Bool("v", false, "verbose output")
	numFiles        = flag.Int("n", 0, "number of files to test in the recompressed tar.gz file")
	workers         = flag.Int("w", 1, "number of workers to use")
	rewriteContents [][]byte
	wg              sync.WaitGroup
)

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("usage: recompress <tar.gz file>")
	}
	for _, fn := range flag.Args() {
		if err := recompress(fn); err != nil {
			log.Fatalf("error recompressing %s: %v", fn, err)
		}
	}
}

func factorial(n int) int {
	if n == 0 {
		return 1
	}
	return n * factorial(n-1)
}

func recompress(fn string) error {
	if *verbose {
		log.Printf("recompressing %s", fn)
	}

	originalContents, err := readOriginal(fn)
	if err != nil {
		return err
	}

	origContentLen := len(originalContents)
	totalResultsLen := factorial(origContentLen)

	jobs := make(chan job, origContentLen)
	results := make(chan result, totalResultsLen)

	for w := 1; w <= *workers; w++ {
		go worker(w, originalContents, jobs, results)
	}

	origPerm := []int{}
	for i := 0; i < origContentLen; i++ {
		origPerm = append(origPerm, i)
	}

	go func() {
		currentPerm := make([]int, origContentLen)
		for i := 0; nextPerm(currentPerm); i++ {
			perm := getPerm(origPerm, currentPerm)
			fmt.Println(perm)

			jobs <- job{i, perm}
		}
		fmt.Println("dones")
		close(jobs)
	}()

	var minResult *result
	for i := 0; i < totalResultsLen-1; i++ {
		result := <-results
		if minResult == nil || minResult.size > result.size {
			minResult = &result
		}
		fmt.Println(i, result)
	}
	fmt.Println(minResult)

	return nil
}

type job struct {
	id   int
	perm []int
}

type result struct {
	perm []int
	size int64
}

func worker(id int, originalContents []*tarEntry, jobs <-chan job, results chan<- result) {
	for job := range jobs {
		content := rewritePermToBuffer(job.perm, originalContents)
		results <- result{job.perm, int64(len(content))}
	}
}

func readOriginal(fn string) ([]*tarEntry, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tarReader := tar.NewReader(gz)

	originalContents := []*tarEntry{}
	for {
		hdr, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		content, err := io.ReadAll(tarReader)
		if err != nil {
			return nil, err
		}

		entry := &tarEntry{
			header:  hdr,
			content: content,
		}

		originalContents = append(originalContents, entry)
	}

	return originalContents, nil
}

func rewritePermToBuffer(perm []int, originalContents []*tarEntry) []byte {
	bufferWriter := &bytes.Buffer{}

	gzipWriter, _ := gzip.NewWriterLevel(bufferWriter, gzip.BestCompression)
	defer gzipWriter.Close()
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	for n, i := range perm {
		if *numFiles > 0 && n >= *numFiles {
			break
		}

		if err := tarWriter.WriteHeader(originalContents[i].header); err != nil {
			log.Fatal(err)
		}

		if _, err := tarWriter.Write(originalContents[i].content); err != nil {
			log.Fatal(err)
		}
	}

	tarWriter.Close()
	gzipWriter.Close()

	return bufferWriter.Bytes()
}

func nextPerm(p []int) bool {
	for i := 0; i < len(p)-1; i++ {
		if p[i] < len(p)-i-1 {
			p[i]++
			return true
		}
		p[i] = 0
	}
	return false
}

func getPerm(orig, p []int) []int {
	result := append([]int{}, orig...)
	for i, v := range p {
		if i+v < len(result) {
			result[i], result[i+v] = result[i+v], result[i]
		}
	}
	return result
}
