package internal

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/klauspost/pgzip"
)

var (
	// blockSize = 64000
	BlockSizeStr = "44000"
	blockSize, _ = strconv.Atoi(BlockSizeStr)
)

// func init() {
// 	fmt.Println("blockSize", 1 << 20)
// 	os.Exit(1)
// }

type TarEntry struct {
	Header  *tar.Header
	Content []byte
}

func cacheKey(perm []int) string {
	// pack two ints into one int64
	key := ""
	for _, i := range perm {
		key += fmt.Sprintf("%d-", i)
	}
	return key
}

func RewritePermToBuffer(perm []int, originalContents []*TarEntry, partial bool, jointCache, soloCache map[string]int64) (int64, []byte) {
	jointCacheKey := cacheKey(perm)
	if cacheCompressionFactor, ok := jointCache[jointCacheKey]; ok {
		fmt.Println("joint cache hit", perm)
		return cacheCompressionFactor, nil
	}

	outputBufferWriter := &bytes.Buffer{}
	var jointBufferWriter io.Writer = outputBufferWriter
	if partial {
		jointBufferWriter = io.Discard
	}
	jointCountingCompressedWriter := &CountingWriter{writer: jointBufferWriter}
	jointGzipWriter, _ := pgzip.NewWriterLevel(jointCountingCompressedWriter, pgzip.BestCompression)
	jointGzipWriter.SetConcurrency(blockSize, 1)
	soloGzipWriter, _ := pgzip.NewWriterLevel(io.Discard, pgzip.BestCompression) // writer will be reset
	soloGzipWriter.SetConcurrency(blockSize, 1)

	jointTarWriter := tar.NewWriter(jointGzipWriter)

	totalSoloCompressedSize := int64(0)
	for _, i := range perm {
		var content []byte
		var header *tar.Header
		// if content is larger than 2 x blockSize, copy only the first and last blockSize bytes
		// creat a new header with the new size
		if partial && len(originalContents[i].Content) >= blockSize*2 {
			content = append(originalContents[i].Content[:blockSize], originalContents[i].Content[len(originalContents[i].Content)-blockSize:]...)
			headerStruct := *originalContents[i].Header
			header = &headerStruct
			header.Size = int64(len(content))
		} else {
			content = originalContents[i].Content
			header = originalContents[i].Header
		}

		if err := jointTarWriter.WriteHeader(header); err != nil {
			log.Fatal(err)
		}

		if _, err := jointTarWriter.Write(content); err != nil {
			log.Fatal(err)
		}

		soloCacheKey := strconv.Itoa(i)
		if cacheSize, ok := soloCache[soloCacheKey]; ok {
			// fmt.Println("solo cache hit", i)

			totalSoloCompressedSize += cacheSize

			continue
		}

		soloCountingCompressedWriter := &CountingWriter{writer: io.Discard}
		soloGzipWriter.Reset(soloCountingCompressedWriter)
		soloTarWriter := tar.NewWriter(soloCountingCompressedWriter)

		if err := soloTarWriter.WriteHeader(header); err != nil {
			log.Fatal(err)
		}

		if _, err := soloTarWriter.Write(content); err != nil {
			log.Fatal(err)
		}

		soloTarWriter.Close()
		soloGzipWriter.Close()

		soloCompressedSize := int64(soloCountingCompressedWriter.BytesWritten)
		totalSoloCompressedSize += soloCompressedSize
		soloCache[soloCacheKey] = soloCompressedSize
	}

	jointTarWriter.Close()
	jointGzipWriter.Close()

	totalJointCompressedSize := int64(jointCountingCompressedWriter.BytesWritten)
	totalCompressionDiff := totalSoloCompressedSize - totalJointCompressedSize

	jointCache[jointCacheKey] = totalCompressionDiff
	return totalCompressionDiff, outputBufferWriter.Bytes()
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

func ReadOriginal(fn string) ([]*TarEntry, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := pgzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tarReader := tar.NewReader(gz)

	originalContents := []*TarEntry{}
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

		entry := &TarEntry{
			Header:  hdr,
			Content: content,
		}

		originalContents = append(originalContents, entry)
	}

	return originalContents, nil
}
