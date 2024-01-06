package internal

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"

	"github.com/klauspost/pgzip"
)

var (
	// allow override with -ldflags "-X squeezetgz/internal.BlockSizeStr=44000"
	BlockSizeStr = "44000"
	blockSize, _ = strconv.Atoi(BlockSizeStr)
)

// func init() {
// 	fmt.Println("blockSize", blockSize)
// 	os.Exit(1)
// }

type TarEntry struct {
	Header  *tar.Header
	Content []byte
}

func Check(compressedBytes []byte, originalContents []*TarEntry) error {
	gzipReader, err := pgzip.NewReader(bytes.NewReader(compressedBytes))
	if err != nil {
		return err
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)

	originalContentNameLookup := map[string]int{}
	for i, originalContent := range originalContents {
		originalContentNameLookup[originalContent.Header.Name] = i
	}

	entryCount := 0
	for {
		hdr, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		originalEntry := originalContents[originalContentNameLookup[hdr.Name]]
		if originalEntry == nil {
			return fmt.Errorf("missing entry: %s", hdr.Name)
		}
		if !reflect.DeepEqual(*hdr, *originalEntry.Header) {
			return fmt.Errorf("header mismatch: %s: %#+v != %#+v", hdr.Name, hdr, originalEntry.Header)
		}

		content, err := io.ReadAll(tarReader)
		if err != nil {
			return err
		}

		if !reflect.DeepEqual(content, originalEntry.Content) {
			return fmt.Errorf("content mismatch: %s", hdr.Name)
		}

		entryCount++
	}

	if entryCount != len(originalContents) {
		return fmt.Errorf("entry count mismatch: %d != %d", entryCount, len(originalContents))
	}

	return nil
}

func RewritePermToBuffer(perm []int, originalContents []*TarEntry, partial bool, soloCache map[int]int64) (int64, []byte) {
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
		// if content is larger than 2 x blockSize, and

		if partial && len(originalContents[i].Content) > blockSize {
			// if over threshold, copy first AND last blockSize-bytes
			// not clear why this works so well, since it duplicates when content is less than 2xblockSize
			content = append(originalContents[i].Content[:blockSize], originalContents[i].Content[len(originalContents[i].Content)-blockSize:]...)

			// rewrite header size to new content size
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

		if cacheSize, ok := soloCache[i]; ok {
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
		soloCache[i] = soloCompressedSize
	}

	jointTarWriter.Close()
	jointGzipWriter.Close()

	totalJointCompressedSize := int64(jointCountingCompressedWriter.BytesWritten)
	totalCompressionDiff := totalSoloCompressedSize - totalJointCompressedSize

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
