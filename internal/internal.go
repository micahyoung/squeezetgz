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

func partialEntry(origEntry *TarEntry, partialBlockSize int) *TarEntry {
	origContentSize := origEntry.Header.Size

	// if over threshold, copy first AND last blockSize-bytes
	// not clear why this works so well, since it duplicates when content is less than 2xblockSize
	newContent := append(origEntry.Content[:partialBlockSize], origEntry.Content[int(origContentSize)-partialBlockSize:]...)

	// clonse underlying header struct
	headerStruct := *origEntry.Header
	newHeader := &headerStruct
	newHeader.Size = int64(len(newContent))

	return &TarEntry{newHeader, newContent}
}

func RewritePermToBuffer(perm []int, originalContents []*TarEntry, partialBlockSize int, partialCache map[int]*TarEntry) (int64, []byte) {
	var firstEntry *TarEntry
	firstId := perm[0]
	if cacheEntry, found := partialCache[firstId]; found {
		firstEntry = cacheEntry
	} else if partialBlockSize >= 0 && originalContents[firstId].Header.Size > int64(partialBlockSize) {
		firstEntry = partialEntry(originalContents[firstId], partialBlockSize)
		partialCache[firstId] = firstEntry
	} else {
		firstEntry = originalContents[firstId]
	}

	soloCountingWriter := &CountingWriter{writer: io.Discard}
	jointCountingWriter := &CountingWriter{writer: io.Discard}

	soloGzipWriter, err := flate.NewWriter(soloCountingWriter, gzip.BestCompression)
	if err != nil {
		log.Fatal(err)
	}
	jointGzipWriter, err := flate.NewWriterDict(jointCountingWriter, gzip.BestCompression, firstEntry.Content)
	if err != nil {
		log.Fatal(err)
	}

	secondTarWriter := tar.NewWriter(io.MultiWriter(soloGzipWriter, jointGzipWriter))

	var secondEntry *TarEntry
	secondId := perm[1]
	if cacheEntry, found := partialCache[secondId]; found {
		secondEntry = cacheEntry
	} else if partialBlockSize >= 0 && originalContents[secondId].Header.Size > int64(partialBlockSize) {
		secondEntry = partialEntry(originalContents[secondId], partialBlockSize)
		partialCache[secondId] = secondEntry
	} else {
		secondEntry = originalContents[secondId]
	}

	if err := secondTarWriter.WriteHeader(secondEntry.Header); err != nil {
		log.Fatal(err)
	}

	if _, err := secondTarWriter.Write(secondEntry.Content); err != nil {
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

func RewriteOriginal(infilepath, outfilepath string, outperm []int) error {
	origEntries, err := ReadOriginal(infilepath)
	if err != nil {
		return err
	}
	outfile, err := os.Create(outfilepath)
	if err != nil {
		return err
	}
	defer outfile.Close()

	gzipWriter, err := gzip.NewWriterLevel(outfile, gzip.BestCompression)
	if err != nil {
		return err
	}
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	for _, i := range outperm {
		if err := tarWriter.WriteHeader(origEntries[i].Header); err != nil {
			return err
		}
		if _, err := tarWriter.Write(origEntries[i].Content); err != nil {
			return err
		}
	}

	return nil
}
