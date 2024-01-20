package internal

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
)

type TarEntry struct {
	Header  *tar.Header
	Content []byte
}

func Check(compressedBytes []byte, originalContents []*TarEntry) error {
	gzipReader, err := gzip.NewReader(bytes.NewReader(compressedBytes))
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

func partialEntry(originalContents []*TarEntry, i int, partialBlockSize int) ([]byte, *tar.Header) {
	content := originalContents[i].Content
	header := originalContents[i].Header

	if partialBlockSize > 0 && len(content) > partialBlockSize {
		// if over threshold, copy first AND last blockSize-bytes
		// not clear why this works so well, since it duplicates when content is less than 2xblockSize
		content = append(content[:partialBlockSize], content[len(content)-partialBlockSize:]...)

		// rewrite header size to new content size
		headerStruct := *header
		header = &headerStruct
		header.Size = int64(len(content))
	}

	return content, header
}

func RewritePermToBuffer(perm []int, originalContents []*TarEntry, partialBlockSize int, soloCache map[int]int64) (int64, []byte) {
	firstDictBuffer := &bytes.Buffer{}
	firstTarWriter := tar.NewWriter(firstDictBuffer)

	firstContent, firstHeader := partialEntry(originalContents, perm[0], partialBlockSize)
	if err := firstTarWriter.WriteHeader(firstHeader); err != nil {
		log.Fatal(err)
	}
	if _, err := firstTarWriter.Write(firstContent); err != nil {
		log.Fatal(err)
	}
	firstTarWriter.Close()
	firstDictBytes := firstDictBuffer.Bytes()

	soloCountingWriter := &CountingWriter{writer: io.Discard}
	jointCountingWriter := &CountingWriter{writer: io.Discard}

	soloGzipWriter, err := flate.NewWriter(soloCountingWriter, gzip.BestCompression)
	if err != nil {
		log.Fatal(err)
	}
	jointGzipWriter, err := flate.NewWriterDict(jointCountingWriter, gzip.BestCompression, firstDictBytes)
	if err != nil {
		log.Fatal(err)
	}

	secondTarWriter := tar.NewWriter(io.MultiWriter(soloGzipWriter, jointGzipWriter))

	secondContent, secondHeader := partialEntry(originalContents, perm[1], partialBlockSize)

	if err := secondTarWriter.WriteHeader(secondHeader); err != nil {
		log.Fatal(err)
	}

	if _, err := secondTarWriter.Write(secondContent); err != nil {
		log.Fatal(err)
	}

	secondTarWriter.Close()
	jointGzipWriter.Close()
	soloGzipWriter.Close()

	totalCompressionDiff := soloCountingWriter.BytesWritten - jointCountingWriter.BytesWritten

	return totalCompressionDiff, nil
}

type CountingWriter struct {
	writer       io.Writer
	BytesWritten int64
}

func (w *CountingWriter) Write(p []byte) (n int, err error) {
	n, err = w.writer.Write(p)
	w.BytesWritten += int64(n)
	return n, err
}

func ReadOriginal(fn string) ([]*TarEntry, error) {
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
