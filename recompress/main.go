// recompresses tar gz files by varying their order to maximize compression
package main

// TODO:
// adjust findfirst to exclude files over a certain size
// adjust compression ratio to favor header similarity
// adjust GetNext to search within binariers before next directory/symlink
// adjust GetNext to try permutations up to a buffer size
// record each files individual compression ratio and compare with combined ratio

import (
	"archive/tar"
	"flag"
	"fmt"
	"log"
	"maxtgz/internal"
	"os"
	"slices"
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
	outFile      = flag.String("o", "", "optional output file")
	mode         = flag.Int("m", 0, "mode (0: default, 1: brute force)")
	workers      = flag.Int("w", 1, "number of workers to use")
	tresholdSize = flag.Int64("l", 32000, "file size threshold for first pass")
	jobCache     = map[int64]*result{}
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

func getNext(origPerm []int, origContent []*internal.TarEntry, limit int64, jobs chan<- *job, results chan *result) *result {
	jobCount := 0
	for i := range origContent {
		if slices.Contains(origPerm, i) {
			continue
		}

		if limit > 0 && origContent[i].Header.Size > limit {
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
			key := cacheKey(result.perm)

			jobCache[key] = result
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
		currentPerm := []int{}

		// skip large files for both first-best candidates
		skipLargeFirst := false
		skipSize := int64(*tresholdSize)
		for i, tarEntry := range originalContents {
			// loop through contents for directory entries and add them to the beginning of the perm
			if tarEntry.Header.Typeflag == tar.TypeDir {
				currentPerm = append(currentPerm, i)
			}
			if !skipLargeFirst && tarEntry.Header.Typeflag == tar.TypeReg && tarEntry.Header.Size < int64(skipSize) {
				skipLargeFirst = true
			}
		}
		fmt.Println("dirPerm", currentPerm)

		// scan for pair of entries with best joint compression factor
		var firstBestResult *result
		for i := 0; i < origContentLen; i++ {
			if slices.Contains(currentPerm, i) {
				fmt.Println("skipping seen", i)
				continue
			}

			if skipLargeFirst && originalContents[i].Header.Size > skipSize {
				fmt.Println("skipping limit", i)
				continue
			}

			result := getNext(append(currentPerm, i), originalContents, skipSize, jobs, results)

			if firstBestResult == nil || compareCompression(result, firstBestResult) {
				firstBestResult = result
			}
		}
		// add both files to perm
		currentPerm = append(currentPerm, firstBestResult.perm...)
		fmt.Println("firstPerm", currentPerm)

		// scan all subsequent entries, smaller than limit, that pair with current file for best compression factor
		for {
			result := getNext(currentPerm, originalContents, skipSize, jobs, results)
			if result == nil {
				break
			}
			// add only new file to perm
			currentPerm = append(currentPerm, result.perm[1])
			fmt.Println("limitPerm", currentPerm)
		}

		// scan all remaining entries that pair with current file for best compression factor
		for {
			result := getNext(currentPerm, originalContents, 0, jobs, results)
			if result == nil {
				break
			}
			// add only new file to perm
			currentPerm = append(currentPerm, result.perm[1])
			fmt.Println("bigPerm", currentPerm)
		}

		bestPerm = currentPerm
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
		bestPerm = minFullResult.perm
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
