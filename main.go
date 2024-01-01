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
	"slices"
	"sync"
)

type tarEntry struct {
	header  *tar.Header
	content []byte
}

type job struct {
	perm []int
}

type result struct {
	perm              []int
	compressionFactor int64
	cached            bool
}

var (
	// flags
	outFile         = flag.String("o", "", "optional output file")
	numFiles        = flag.Int("n", 0, "number of files to test in the recompressed tar.gz file")
	workers         = flag.Int("w", 1, "number of workers to use")
	jobCache        = map[int64]*result{}
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

func cacheKey(perm []int) int64 {
	if len(perm) != 2 {
		panic("cacheKey only supports 2 element perms")
	}

	// pack two ints into one int64
	return int64(perm[0])<<32 + int64(perm[1])
}

func getNext(origPerm []int, origContent []*tarEntry, jobs chan<- *job, results chan *result) *result {
	jobCount := 0
	for i := range origContent {
		if slices.Contains(origPerm, i) {
			continue
		}

		lastIdx := origPerm[len(origPerm)-1]

		testPerm := []int{lastIdx, i}
		cachekey := cacheKey(testPerm)
		// check for cached result
		if cachedResult, ok := jobCache[cachekey]; ok {
			go func() {
				results <- cachedResult
			}()
		} else {
			go func(lastIdx, i int) {
				jobs <- &job{perm: testPerm}
			}(lastIdx, i)
		}
		jobCount++
	}

	var bestPairResult *result
	for i := 0; i < jobCount; i++ {
		result := <-results
		if !result.cached {
			result.cached = true
			jobCache[cacheKey(result.perm)] = result
		}

		// fmt.Println("result", result)

		if bestPairResult == nil || compareCompression(result, bestPairResult) {
			bestPairResult = result
		}
	}
	fmt.Println("best", bestPairResult)

	return bestPairResult
}

func recompress(fn string) error {
	originalContents, err := readOriginal(fn)
	if err != nil {
		return err
	}

	origContentLen := len(originalContents)
	fmt.Println("rewriting files:", origContentLen)

	jobs := make(chan *job)
	results := make(chan *result)

	for w := 1; w <= *workers; w++ {
		go worker(w, originalContents, jobs, results)
	}

	// loop through contents for directory entries and add them to the beginning of the perm
	currentPerm := []int{}
	for i, tarEntry := range originalContents {
		if tarEntry.header.Typeflag == tar.TypeDir {
			currentPerm = append(currentPerm, i)
		}
	}
	fmt.Println("dirPerm", currentPerm)

	var firstBestResult *result
	for i := 0; i < origContentLen; i++ {
		if slices.Contains(currentPerm, i) {
			fmt.Println("skipping", i)
			continue
		}

		result := getNext(append(currentPerm, i), originalContents, jobs, results)

		if firstBestResult == nil || compareCompression(result, firstBestResult) {
			firstBestResult = result
		}
	}
	fmt.Println("firstBEstPerm", firstBestResult.perm)
	currentPerm = append(currentPerm, firstBestResult.perm...)
	fmt.Println("firstPerm", currentPerm)

	for i := 0; i < origContentLen-len(currentPerm); i++ {
		result := getNext(currentPerm, originalContents, jobs, results) //TODO: cache previous comparisons
		currentPerm = append(currentPerm, result.perm[1])
		fmt.Println("nextPerm", currentPerm)
	}

	// write recompressed file
	compressionFactor, compressedBytes := rewritePermToBuffer(currentPerm, originalContents)
	fmt.Println("compressionFactor", compressionFactor)

	if *outFile != "" {
		f, err := os.Create(*outFile)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := f.Write(compressedBytes); err != nil {
			return err
		}
	}

	// go func() {
	// 	currentPerm := make([]int, origContentLen)
	// 	for i := 0; nextPerm(currentPerm); i++ {
	// 		perm := getPerm(origPerm, currentPerm)
	// 		fmt.Println(perm)

	// 		jobs <- job{i, perm}
	// 	}
	// 	fmt.Println("dones")
	// 	close(jobs)
	// }()

	// var minFullResult *result
	// for i := 0; i < totalResultsLen-1; i++ {
	// 	result := <-results
	// 	if minFullResult == nil || minFullResult.compressedSize > result.compressedSize {
	// 		minFullResult = &result
	// 	}
	// 	fmt.Println(i, result)
	// }
	// fmt.Println(minFullResult)

	return nil
}

func compareCompression(a, b *result) bool {
	if a.compressionFactor > b.compressionFactor {
		return true
	}

	// stable tiebreaker when factors are equal
	if a.compressionFactor == b.compressionFactor && a.perm[0] < b.perm[0] && a.perm[1] < b.perm[1] {
		return true
	}

	return false
}

func worker(id int, originalContents []*tarEntry, jobs <-chan *job, results chan<- *result) {
	for job := range jobs {
		compressionFactor, _ := rewritePermToBuffer(job.perm, originalContents)
		results <- &result{job.perm, compressionFactor, false}
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

func rewritePermToBuffer(perm []int, originalContents []*tarEntry) (int64, []byte) {
	bufferWriter := &bytes.Buffer{}

	gzipWriter, _ := gzip.NewWriterLevel(bufferWriter, gzip.BestCompression)
	defer gzipWriter.Close()
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	uncompressedSize := int64(0)
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

		uncompressedSize += originalContents[i].header.Size
	}

	tarWriter.Close()
	gzipWriter.Close()

	compressedBytes := bufferWriter.Bytes()
	return uncompressedSize * 10000 / int64(len(compressedBytes)), compressedBytes
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
