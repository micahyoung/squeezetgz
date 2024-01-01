package internal

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

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

func RewritePermToBuffer(perm []int, originalContents []*TarEntry, jointCache, soloCache map[string]int64) (int64, []byte) {
	jointCacheKey := cacheKey(perm)
	if cacheSize, ok := jointCache[jointCacheKey]; ok {
		fmt.Println("joint cache hit", perm)
		return cacheSize, nil
	}

	jointBufferWriter := &bytes.Buffer{}
	jointCountingCompressedWriter := &CountingWriter{writer: jointBufferWriter}
	jointGzipWriter, _ := gzip.NewWriterLevel(jointCountingCompressedWriter, gzip.BestCompression)
	jointTarWriter := tar.NewWriter(jointGzipWriter)

	totalSoloCompressedSize := int64(0)
	for _, i := range perm {
		if err := jointTarWriter.WriteHeader(originalContents[i].Header); err != nil {
			log.Fatal(err)
		}

		if _, err := jointTarWriter.Write(originalContents[i].Content); err != nil {
			log.Fatal(err)
		}

		soloCacheKey := strconv.Itoa(i)
		if cacheSize, ok := soloCache[soloCacheKey]; ok {
			// fmt.Println("solo cache hit", i)

			totalSoloCompressedSize += cacheSize

			continue
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

		soloCompressedSize := int64(soloCountingCompressedWriter.BytesWritten)
		totalSoloCompressedSize += soloCompressedSize
		soloCache[soloCacheKey] = soloCompressedSize
	}

	jointTarWriter.Close()
	jointGzipWriter.Close()

	totalJointCompressedSize := int64(jointCountingCompressedWriter.BytesWritten)
	jointCache[jointCacheKey] = totalJointCompressedSize
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
