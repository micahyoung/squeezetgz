// recompresses tar gz files by varying their order to maximize compression
package main

// TODO:
// adjust findfirst to exclude files over a certain size
// adjust compression ratio to favor header similarity
// adjust GetNext to search within binariers before next directory/symlink
// adjust GetNext to try permutations up to a buffer size
// record each files individual compression ratio and compare with combined ration

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
	mode            = flag.Int("m", 0, "mode (0: default, 1: brute force)")
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

func getNext(origPerm []int, origContent []*tarEntry, limit int64, jobs chan<- *job, results chan *result) *result {
	jobCount := 0
	for i := range origContent {
		if slices.Contains(origPerm, i) {
			continue
		}

		if limit > 0 && origContent[i].header.Size > limit {
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

	var currentPerm []int
	switch *mode {
	case 0:
		currentPerm = []int{}
		// loop through contents for directory entries and add them to the beginning of the perm
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

			// windowsize := int64(32000)
			// if originalContents[i].header.Size > windowsize {
			// 	fmt.Println("skipping", i)
			// 	continue
			// }

			result := getNext(append(currentPerm, i), originalContents, 0, jobs, results)

			if firstBestResult == nil || compareCompression(result, firstBestResult) {
				firstBestResult = result
			}
		}
		fmt.Println("firstBEstPerm", firstBestResult.perm)
		currentPerm = append(currentPerm, firstBestResult.perm...)
		fmt.Println("firstPerm", currentPerm)

		for {
			result := getNext(currentPerm, originalContents, 0, jobs, results)
			if result == nil {
				break
			}
			currentPerm = append(currentPerm, result.perm[1])
			fmt.Println("nextPerm", currentPerm)
		}
	case 1:
		statePerm := make([]int, origContentLen)
		origPerm := make([]int, origContentLen)
		for i := 0; i < origContentLen; i++ {
			origPerm[i] = i
		}

		jobCount := 0
		for {
			if !nextPerm(statePerm) {
				fmt.Println("jobs", jobCount)
				// close(jobs)
				break
			}

			perm := getPerm(origPerm, statePerm)

			go func() {
				jobs <- &job{perm}
			}()

			jobCount++
		}

		var minFullResult *result
		for i := 0; i < jobCount; i++ {
			result := <-results
			if minFullResult == nil || compareCompression(result, minFullResult) {
				minFullResult = result
			}
			fmt.Println(i, result)
		}
		fmt.Println(minFullResult)
		currentPerm = minFullResult.perm
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
	jointBufferWriter := &bytes.Buffer{}
	jointCountingCompressedWriter := &CountingWriter{writer: jointBufferWriter}
	jointGzipWriter, _ := gzip.NewWriterLevel(jointCountingCompressedWriter, gzip.BestCompression)
	jointTarWriter := tar.NewWriter(jointGzipWriter)

	totalUncompressedSize := int64(0)
	totalSoloCompressedSize := int64(0)
	for _, i := range perm {
		if err := jointTarWriter.WriteHeader(originalContents[i].header); err != nil {
			log.Fatal(err)
		}

		if _, err := jointTarWriter.Write(originalContents[i].content); err != nil {
			log.Fatal(err)
		}

		soloBufferWriter := &bytes.Buffer{}
		soloCountingCompressedWriter := &CountingWriter{writer: soloBufferWriter}
		soloGzipWriter, _ := gzip.NewWriterLevel(soloCountingCompressedWriter, gzip.BestCompression)
		soloCountingUncompressedWriter := &CountingWriter{writer: soloGzipWriter}
		soloTarWriter := tar.NewWriter(soloCountingUncompressedWriter)

		if err := soloTarWriter.WriteHeader(originalContents[i].header); err != nil {
			log.Fatal(err)
		}

		if _, err := soloTarWriter.Write(originalContents[i].content); err != nil {
			log.Fatal(err)
		}

		soloTarWriter.Close()
		soloGzipWriter.Close()

		totalSoloCompressedSize += int64(soloCountingCompressedWriter.BytesWritten)
		totalUncompressedSize += int64(soloCountingUncompressedWriter.BytesWritten)
	}

	jointTarWriter.Close()
	jointGzipWriter.Close()

	totalJointCompressedSize := int64(jointCountingCompressedWriter.BytesWritten)

	// soloCompressionFactor := (totalSoloCompressedSize * 100000) / totalUncompressedSize
	// jointCompressionFactor := (totalJointCompressedSize * 100000) / totalUncompressedSize

	// fmt.Printf("size: solo: %d, joint: %d, uncompressed: %d\n", totalSoloCompressedSize, totalJointCompressedSize, totalUncompressedSize)
	// fmt.Printf("factor: solo: %d, joint: %d\n", soloCompressionFactor, jointCompressionFactor)
	return totalSoloCompressedSize - totalJointCompressedSize, jointBufferWriter.Bytes()
	// return soloCompressionFactor - jointCompressionFactor, jointBufferWriter.Bytes()
}

type CountingWriter struct {
	writer       io.Writer
	BytesWritten int
}

func (w *CountingWriter) Write(p []byte) (n int, err error) {
	n, err = w.writer.Write(p)
	w.BytesWritten += n
	return n, err
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
