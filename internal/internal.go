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

func Check(tarFile io.Reader, originalContents []*TarEntry) error {
	tarReader := tar.NewReader(tarFile)

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

func RewritePermToBuffer(firstId, secondId int, originalContents []*TarEntry, soloCache map[int]int64) int64 {
	firstEntry := originalContents[firstId]

	jointCountingWriter := &CountingWriter{writer: io.Discard}

	jointGzipWriter, err := flate.NewWriterDict(jointCountingWriter, gzip.BestCompression, firstEntry.Content)
	if err != nil {
		log.Fatal(err)
	}

	jointTarWriter := tar.NewWriter(jointGzipWriter)

	secondEntry := originalContents[secondId]

	if err := jointTarWriter.WriteHeader(secondEntry.Header); err != nil {
		log.Fatal(err)
	}

	if _, err := jointTarWriter.Write(secondEntry.Content); err != nil {
		log.Fatal(err)
	}

	jointTarWriter.Close()
	jointGzipWriter.Close()

	jointBytesWritten := jointCountingWriter.BytesWritten

	var soloBytesWritten int64
	if cacheBytesWritten, found := soloCache[secondId]; found {
		soloBytesWritten = cacheBytesWritten
	} else {
		soloCountingWriter := &CountingWriter{writer: io.Discard}

		soloGzipWriter, err := flate.NewWriter(soloCountingWriter, gzip.BestCompression)
		if err != nil {
			log.Fatal(err)
		}

		soloTarWriter := tar.NewWriter(soloGzipWriter)

		if err := soloTarWriter.WriteHeader(secondEntry.Header); err != nil {
			log.Fatal(err)
		}

		if _, err := soloTarWriter.Write(secondEntry.Content); err != nil {
			log.Fatal(err)
		}

		soloTarWriter.Close()
		soloGzipWriter.Close()

		soloBytesWritten = soloCountingWriter.BytesWritten

		soloCache[secondId] = soloBytesWritten
	}

	totalCompressionDiff := soloBytesWritten - jointBytesWritten

	return totalCompressionDiff
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

func ReadOriginal(fn string, partialBlockSize int64) ([]*TarEntry, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tarReader := tar.NewReader(f)

	originalContents := []*TarEntry{}
	for {
		header, err := tarReader.Next()
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

		if partialBlockSize > 0 && header.Size > partialBlockSize {
			// if over threshold, copy first AND last blockSize-bytes
			// not clear why this works so well, since it duplicates when content is less than 2xblockSize
			newContentBuffer := &bytes.Buffer{}
			if _, err := io.Copy(newContentBuffer, bytes.NewReader(content[:partialBlockSize])); err != nil {
				return nil, err
			}
			if _, err := io.Copy(newContentBuffer, bytes.NewReader(content[header.Size-partialBlockSize:])); err != nil {
				return nil, err
			}

			content = newContentBuffer.Bytes()

			header.Size = int64(newContentBuffer.Len())
		}

		entry := &TarEntry{
			Header:  header,
			Content: content,
		}

		originalContents = append(originalContents, entry)
	}

	return originalContents, nil
}

func RewriteOriginal(infilepath, outfilepath string, outperm []int) error {
	origEntries, err := ReadOriginal(infilepath, 0)
	if err != nil {
		return err
	}
	outfile, err := os.Create(outfilepath)
	if err != nil {
		return err
	}
	defer outfile.Close()

	tarWriter := tar.NewWriter(outfile)
	defer tarWriter.Close()

	for _, i := range outperm {
		if err := tarWriter.WriteHeader(origEntries[i].Header); err != nil {
			return err
		}
		if _, err := tarWriter.Write(origEntries[i].Content); err != nil {
			return err
		}
	}

	outfile.Seek(0, 0)

	if err := Check(outfile, origEntries); err != nil {
		return err
	}

	return nil
}
