package internal

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"os"
)

type TarEntry struct {
	Header  *tar.Header
	Content []byte
}

func RewritePermToBuffer(perm []int, originalContents []*TarEntry) (int64, []byte) {
	jointBufferWriter := &bytes.Buffer{}
	jointCountingCompressedWriter := &CountingWriter{writer: jointBufferWriter}
	jointGzipWriter, _ := gzip.NewWriterLevel(jointCountingCompressedWriter, gzip.BestCompression)
	jointTarWriter := tar.NewWriter(jointGzipWriter)

	totalUncompressedSize := int64(0)
	totalSoloCompressedSize := int64(0)
	for _, i := range perm {
		if err := jointTarWriter.WriteHeader(originalContents[i].Header); err != nil {
			log.Fatal(err)
		}

		if _, err := jointTarWriter.Write(originalContents[i].Content); err != nil {
			log.Fatal(err)
		}

		soloBufferWriter := &bytes.Buffer{}
		soloCountingCompressedWriter := &CountingWriter{writer: soloBufferWriter}
		soloGzipWriter, _ := gzip.NewWriterLevel(soloCountingCompressedWriter, gzip.BestCompression)
		soloCountingUncompressedWriter := &CountingWriter{writer: soloGzipWriter}
		soloTarWriter := tar.NewWriter(soloCountingUncompressedWriter)

		if err := soloTarWriter.WriteHeader(originalContents[i].Header); err != nil {
			log.Fatal(err)
		}

		if _, err := soloTarWriter.Write(originalContents[i].Content); err != nil {
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

	return totalSoloCompressedSize - totalJointCompressedSize, jointBufferWriter.Bytes()
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
