// recompresses tar gz files by varying their order to maximize compression
package main

// TODO:
// adjust compression ratio to favor header similarity
// adjust GetNext to search within binariers before next directory/symlink
// adjust GetNext to try permutations up to a buffer size

import (
	"archive/tar"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"slices"
	"squeezetgz/internal"
)

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
	outFile   = flag.String("o", "", "optional output file")
	mode      = flag.Int("m", 0, "mode (0: default, 1: brute force)")
	workers   = flag.Int("w", runtime.NumCPU()-1, "number of workers to use. Default: num CPUs - 1")
	batchSize = flag.Int64("b", 32000, "total size of uncompressed files to evaluate")
	jobCache  = map[string]*result{}
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

func cacheKey(perm []int) string {
	// pack two ints into one int64
	key := ""
	for _, i := range perm {
		key += fmt.Sprintf("%d-", i)
	}
	return key
}

func getNext(origPerm []int, origContent []*internal.TarEntry, batchSize int64, jobs chan<- *job, results chan *result) *result {
	jobCount := 0
	lastIdx := origPerm[len(origPerm)-1]
	for i := range origContent {
		if slices.Contains(origPerm, i) {
			continue
		}

		testPerm := origPerm

		// if filesize is large, add directly to perm
		if origContent[i].Header.Size >= batchSize {
			comboPerm := []int{lastIdx, i}

			cachekey := cacheKey(comboPerm)

			// check for cached result
			if cachedResult, ok := jobCache[cachekey]; ok {
				go func() {
					results <- cachedResult
				}()
			} else {
				go func(comboPerm []int) {
					jobs <- &job{perm: comboPerm}
				}(comboPerm)
			}

			jobCount++

			continue
		}

		testPerm = append(testPerm, i)
		// fmt.Printf("testPerm %v\n", testPerm)

		// otherwise, compare with next files
		for j := range origContent {
			if slices.Contains(testPerm, j) {
				continue
			}

			comboPerm := []int{lastIdx, i, j}

			cachekey := cacheKey(comboPerm)

			// check for cached result
			if cachedResult, ok := jobCache[cachekey]; ok {
				go func() {
					results <- cachedResult
				}()
			} else {
				go func(comboPerm []int) {
					jobs <- &job{perm: comboPerm}
				}(comboPerm)
			}

			jobCount++
		}
	}

	var bestBatchResult *result
	for i := 0; i < jobCount; i++ {
		result := <-results
		if !result.cached {
			result.cached = true
			key := cacheKey(result.perm)

			jobCache[key] = result
		}

		// fmt.Println("result", result)

		if bestBatchResult == nil || compareCompression(result, bestBatchResult) {
			bestBatchResult = result
		}
	}
	fmt.Println("best", bestBatchResult)

	return bestBatchResult
}

func recompress(fn string) error {
	originalContents, err := internal.ReadOriginal(fn)
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

	var bestPerm []int
	switch *mode {
	case 0:
		// skip large files for both first-best candidates
		// loop through contents for directory entries and add them to the beginning of the perm
		// scan for pair of entries with best joint compression factor
		// add both files to perm
		// scan all subsequent entries, smaller than limit, that pair with current file for best compression factor
		// add only new file to perm
		// scan all remaining entries that pair with current file for best compression factor
		// add only new file to perm
		bestPerm = optimized(originalContents, origContentLen, jobs, results)
	case 1:
		// close(jobs)
		bestPerm = bruteforce(origContentLen, jobs, results)
	}

	// write recompressed file
	compressionFactor, compressedBytes := internal.RewritePermToBuffer(bestPerm, originalContents)
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

func bruteforce(origContentLen int, jobs chan *job, results chan *result) []int {
	statePerm := make([]int, origContentLen)
	origPerm := make([]int, origContentLen)
	for i := 0; i < origContentLen; i++ {
		origPerm[i] = i
	}

	jobCount := 0
	for {
		if !nextPerm(statePerm) {
			fmt.Println("jobs", jobCount)

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
	t := minFullResult.perm
	return t
}

func optimized(originalContents []*internal.TarEntry, origContentLen int, jobs chan *job, results chan *result) []int {
	currentPerm := []int{}

	for i, tarEntry := range originalContents {

		if tarEntry.Header.Typeflag == tar.TypeDir {
			currentPerm = append(currentPerm, i)
		}
	}
	fmt.Println("dirPerm", currentPerm)

	for {
		result := getNext(currentPerm, originalContents, *batchSize, jobs, results)
		if result == nil {
			break
		}

		currentPerm = append(currentPerm, result.perm[1:]...)
		fmt.Println("limitPerm", currentPerm)
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
	for job := range jobs {
		compressionFactor, _ := internal.RewritePermToBuffer(job.perm, originalContents)
		results <- &result{job.perm, compressionFactor, false}
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
