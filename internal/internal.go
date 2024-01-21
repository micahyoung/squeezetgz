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
	Header       *tar.Header
	StartContent []byte
	EndContent   []byte
}

func Check(tarFile io.Reader, originalContents []*tarStore) error {
	tarReader := tar.NewReader(tarFile)

	originalContentNameLookup := map[string]int{}
	for i, originalContent := range originalContents {
		if _, found := originalContentNameLookup[originalContent.Header.Name]; found {
			log.Fatalf("duplicate entry in original, can't proceed: %s", originalContent.Header.Name)
		}
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

	jointGzipWriter, err := flate.NewWriterDict(jointCountingWriter, gzip.BestCompression, firstEntry.EndContent)
	if err != nil {
		log.Fatal(err)
	}

	secondEntry := originalContents[secondId]

	if _, err := jointGzipWriter.Write(secondEntry.StartContent); err != nil {
		log.Fatal(err)
	}

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

		if _, err := soloGzipWriter.Write(secondEntry.StartContent); err != nil {
			log.Fatal(err)
		}

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

func ReadOriginal(fn string, partialBlockSize int) ([]*TarEntry, error) {
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

		tarContent, err := tarEntryToBytes(header, content)
		if err != nil {
			return nil, fmt.Errorf("short file %w", err)
		}

		var startContent, endContent []byte
		if partialBlockSize > 0 && len(tarContent) > partialBlockSize*2 {
			// if over threshold, copy only last partialBlockSize-bytes
			startContentBuffer := &bytes.Buffer{}
			endContentBuffer := &bytes.Buffer{}

			startSize, err := io.Copy(startContentBuffer, bytes.NewReader(tarContent[:partialBlockSize]))
			if err != nil {
				return nil, err
			}
			if startSize != int64(partialBlockSize) {
				return nil, fmt.Errorf("short file start %d != %d", startSize, partialBlockSize)
			}
			endSize, err := io.Copy(endContentBuffer, bytes.NewReader(tarContent[len(tarContent)-partialBlockSize:]))
			if err != nil {
				return nil, err
			}
			if endSize != int64(partialBlockSize) {
				return nil, fmt.Errorf("short file end %d != %d", endSize, partialBlockSize)
			}

			header.Size = int64(partialBlockSize)

			startContent = startContentBuffer.Bytes()
			endContent = endContentBuffer.Bytes()
		} else {
			startContentBuffer := &bytes.Buffer{}

			_, err := io.Copy(startContentBuffer, bytes.NewReader(tarContent))
			if err != nil {
				return nil, err
			}

			startContent = startContentBuffer.Bytes()
			endContent = tarContent
		}

		entry := &TarEntry{
			Header:       header,
			StartContent: startContent,
			EndContent:   endContent,
		}

		originalContents = append(originalContents, entry)
	}

	return originalContents, nil
}

func tarEntryToBytes(tarHeader *tar.Header, tarContent []byte) ([]byte, error) {
	buffer := &bytes.Buffer{}
	tarWriter := tar.NewWriter(buffer)
	if err := tarWriter.WriteHeader(tarHeader); err != nil {
		return nil, err
	}

	if _, err := tarWriter.Write(tarContent); err != nil {
		return nil, err
	}
	if err := tarWriter.Close(); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

type tarStore struct {
	Header  *tar.Header
	Content []byte
}

func TarFileToEntries(tarFile io.Reader) ([]*tarStore, error) {
	tarReader := tar.NewReader(tarFile)

	var origEntries []*tarStore
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

		origEntries = append(origEntries, &tarStore{header, content})
	}

	return origEntries, nil
}

func RewriteOriginal(infilepath, outfilepath string, outperm []int) error {
	infile, err := os.Open(infilepath)
	if err != nil {
		return err
	}
	defer infile.Close()

	origEntries, err := TarFileToEntries(infile)
	if err != nil {
		return err
	}

	outfile, err := os.Create(outfilepath)
	if err != nil {
		return err
	}
	defer outfile.Close()

	outTarWriter := tar.NewWriter(outfile)
	for _, i := range outperm {
		if err := outTarWriter.WriteHeader(origEntries[i].Header); err != nil {
			return err
		}
		if _, err := outTarWriter.Write(origEntries[i].Content); err != nil {
			return err
		}
	}
	outTarWriter.Close()

	outfile.Seek(0, 0)

	if err := Check(outfile, origEntries); err != nil {
		return err
	}

	return nil
}
