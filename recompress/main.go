// recompresses tar gz files by varying their order to maximize compression
package main

// TODO:
// adjust compression ratio to favor header similarity
// adjust GetNext to search within binariers before next directory/symlink
// adjust GetNext to try permutations up to a buffer size

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"slices"
	"sort"
	"squeezetgz/internal"

	"github.com/klauspost/compress/gzip"
)

type job struct {
	perm []int
}

type result struct {
	perm              []int
	compressionFactor int64
}

var (
	// flags
	outFile   = flag.String("o", "", "optional output file")
	mode      = flag.Int("m", 0, "mode (0: default, 1: brute force)")
	batchSize = flag.Int("b", 1, "batch size")
	blockSize = flag.Int("k", 44000, "block size")
	workers   = flag.Int("w", runtime.NumCPU()-1, "number of workers to use. Default: num CPUs - 1")
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

func getNextFiles(origPerm []int, origContent []*internal.TarEntry, jobs chan<- *job, results chan *result) []*result {
	jobCount := 0
	lastIdx := origPerm[len(origPerm)-1]
	for i := range origContent {
		if origContent[i].Header.Typeflag != tar.TypeReg {
			continue
		}
		if slices.Contains(origPerm, i) {
			continue
		}

		comboPerm := []int{lastIdx, i}

		go func(comboPerm []int) {
			jobs <- &job{perm: comboPerm}
		}(comboPerm)
		jobCount++
	}
	fmt.Printf("jobs %d\n", jobCount)

	var bestBatchResults []*result
	for i := 0; i < jobCount; i++ {
		result := <-results

		// skip if best-batch is not empty and new result is worse than lowest result in best-batch
		if len(bestBatchResults) != 0 && !compareCompression(result, bestBatchResults[len(bestBatchResults)-1]) {
			continue
		}

		// add new result to current best
		testResults := append(bestBatchResults, result)

		// re-sort all by compression factor
		sort.Slice(testResults, func(i, j int) bool {
			return compareCompression(testResults[i], testResults[j])
		})

		// if not yet batchSize, use full test-batch as best-batch
		if len(testResults) <= *batchSize {
			bestBatchResults = testResults
			continue
		}

		// update best-batch up to batch size, dropping worst result
		bestBatchResults = testResults[:len(testResults)-1]
	}

	// if any best-match result's files were 0, only return single-best result instead of whole batch
	// this avoids giving batch suggestions from unrepresentative perms
	if len(bestBatchResults) > 1 {
		for _, i := range bestBatchResults[0].perm {
			if origContent[i].Header.Size == 0 {
				return bestBatchResults[:1]
			}
		}
	}

	// otherwise, return full set of results
	return bestBatchResults
}

func recompress(fn string) error {
	originalContents, err := internal.ReadOriginal(fn)
	if err != nil {
		return err
	}

	origContentLen := len(originalContents)
	fmt.Println("rewriting entries:", origContentLen)

	jobs := make(chan *job)
	results := make(chan *result)

	for w := 1; w <= *workers; w++ {
		go worker(w, originalContents, jobs, results)
	}

	var bestPerm []int
	switch *mode {
	case 0:
		// close(jobs)
		bestPerm = bruteforce(originalContents, origContentLen, jobs, results)
	case 1:
		// skip large files for both first-best candidates
		// loop through contents for directory entries and add them to the beginning of the perm
		// scan for pair of entries with best joint compression factor
		// add both files to perm
		// scan all subsequent entries, smaller than limit, that pair with current file for best compression factor
		// add only new file to perm
		// scan all remaining entries that pair with current file for best compression factor
		// add only new file to perm
		bestPerm = optimized(originalContents, origContentLen, jobs, results)
	case 2:
		bestPerm = partitioned(originalContents, origContentLen, jobs, results)
	}

	if len(bestPerm) != origContentLen {
		log.Fatal("perm length does not match original contents length")
	}

	compressedBuffer := &bytes.Buffer{}
	gzipWriter, _ := gzip.NewWriterLevel(compressedBuffer, gzip.BestCompression)
	tarWriter := tar.NewWriter(gzipWriter)

	for _, i := range bestPerm {
		if err := tarWriter.WriteHeader(originalContents[i].Header); err != nil {
			return err
		}

		if _, err := tarWriter.Write(originalContents[i].Content); err != nil {
			return err
		}
	}

	tarWriter.Close()
	gzipWriter.Close()
	compressedBytes := compressedBuffer.Bytes()

	if err := internal.Check(compressedBytes, originalContents); err != nil {
		return fmt.Errorf("comparing bytes: %w", err)
	}

	if *outFile != "" {
		fmt.Printf("writing %s\n", *outFile)
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

func bruteforce(originalContents []*internal.TarEntry, origContentLen int, jobs chan *job, results chan *result) []int {
	// add directories first, in original order
	dirPerm := []int{}
	for i, tarEntry := range originalContents {
		if tarEntry.Header.Typeflag == tar.TypeDir {
			dirPerm = append(dirPerm, i)
		}
	}
	fmt.Println("dirPerm", dirPerm)

	// collect regular files separately
	filePerm := []int{}
	for i, tarEntry := range originalContents {
		if tarEntry.Header.Typeflag == tar.TypeReg {
			filePerm = append(filePerm, i)
		}
	}

	statePerm := make([]int, len(filePerm))

	jobCount := int64(0)
	for {
		if !nextPerm(statePerm) {
			fmt.Println("jobs", jobCount)

			break
		}

		perm := getPerm(filePerm, statePerm)

		go func() {
			jobs <- &job{append(dirPerm, perm...)}
		}()

		jobCount++
	}

	var minAllFilesResult *result
	for i := int64(0); i < jobCount; i++ {
		result := <-results
		if minAllFilesResult == nil || compareCompression(result, minAllFilesResult) {
			minAllFilesResult = result
		}

		// print every 10 percent
		if i%(jobCount/10) == 0 {
			fmt.Printf("best @ %2d%%: %v\n", i*101/jobCount, minAllFilesResult)
		}
	}
	fmt.Println(minAllFilesResult)

	currentPerm := minAllFilesResult.perm

	// add remaining files last (symlinks, etc)
	for i := range originalContents {
		if slices.Contains(currentPerm, i) {
			continue
		}

		currentPerm = append(currentPerm, i)
	}

	return currentPerm
}

func partitioned(originalContents []*internal.TarEntry, origContentLen int, jobs chan *job, results chan *result) []int {
	currentPerm := []int{}

	// add directories first, in original order
	for i, tarEntry := range originalContents {
		if tarEntry.Header.Typeflag == tar.TypeDir {
			currentPerm = append(currentPerm, i)
		}
	}
	fmt.Println("dirPerm", currentPerm)

	// add regular files based on compression factor
	for {
		results := getNextFiles(currentPerm, originalContents, jobs, results)
		if len(results) == 0 {
			break
		}
		fmt.Println("best", results[0])

		for _, result := range results {
			currentPerm = append(currentPerm, result.perm[1:]...)
		}

		fmt.Println("limitPerm", currentPerm)
	}

	// add remaining files last (symlinks, etc)
	for i := range originalContents {
		if slices.Contains(currentPerm, i) {
			continue
		}

		currentPerm = append(currentPerm, i)
	}

	return currentPerm
}

func optimized(originalContents []*internal.TarEntry, origContentLen int, jobs chan *job, results chan *result) []int {
	currentPerm := []int{}

	// add directories first, in original order
	for i, tarEntry := range originalContents {
		if tarEntry.Header.Typeflag == tar.TypeDir {
			currentPerm = append(currentPerm, i)
		}
	}
	fmt.Println("dirPerm", currentPerm)

	// add regular files based on compression factor
	for {
		results := getNextFiles(currentPerm, originalContents, jobs, results)
		if len(results) == 0 {
			break
		}

		currentPerm = append(currentPerm, results[0].perm[1:]...)
		fmt.Println("limitPerm", currentPerm)
	}

	// add remaining files last (symlinks, etc)
	for i := range originalContents {
		if slices.Contains(currentPerm, i) {
			continue
		}

		currentPerm = append(currentPerm, i)
	}

	return currentPerm
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

func worker(id int, originalContents []*internal.TarEntry, jobs <-chan *job, results chan<- *result) {
	soloCache := map[int]int64{}
	for job := range jobs {
		compressionFactor, _ := internal.RewritePermToBuffer(job.perm, originalContents, *blockSize, soloCache)
		results <- &result{job.perm, compressionFactor}
	}
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
