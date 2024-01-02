package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
)

// flags for tgz file
var (
	outFile = flag.String("o", "", "optional output file")
)

func main() {
	flag.Parse()

	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func (b *BasicReadWriter) Write(p []byte) (n int, err error) {
	b.Bytes = append(b.Bytes, p...)
	return len(p), nil
}

func (b *BasicReadWriter) Read(p []byte) (n int, err error) {
	copy(p, b.Bytes)
	return len(b.Bytes), nil
}

// io.Writer definition
type BasicReadWriter struct {
	Bytes []byte
}

func run() error {
	ogWriterPtr := &BasicReadWriter{}
	ogGzipWriterPtr := gzip.NewWriter(ogWriterPtr)
	ogTarWriterPtr := tar.NewWriter(ogGzipWriterPtr)

	if err := ogTarWriterPtr.WriteHeader(
		&tar.Header{
			Name: "test1.txt",
		},
	); err != nil {
		return err
	}

	// duplicate structs
	copyTarWriterStruct := *ogTarWriterPtr
	copyGzipWriterStruct := *ogGzipWriterPtr
	copyWriterStruct := *ogWriterPtr

	// add another file to original structs
	if err := ogTarWriterPtr.WriteHeader(
		&tar.Header{
			Name: "test2.txt",
		},
	); err != nil {
		return err
	}

	// replace original structs with duplicates
	*copyWriterStruct = &BasicReadWriter{}
	*ogGzipWriterPtr = copyGzipWriterStruct
	*ogTarWriterPtr = copyTarWriterStruct

	if err := ogTarWriterPtr.WriteHeader(
		&tar.Header{
			Name: "test3.txt",
		},
	); err != nil {
		return err
	}

	ogTarWriterPtr.Close()
	ogGzipWriterPtr.Close()

	fmt.Println("og")
	ogGzipReader, err := gzip.NewReader(ogWriterPtr)
	if err != nil {
		return err
	}
	ogTarReader := tar.NewReader(ogGzipReader)
	for {
		hdr, err := ogTarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		log.Println(hdr.Name)
	}
	ogGzipReader.Close()

	// fmt.Println("copy")
	// copyGzipReader, err := gzip.NewReader(copyWriter)
	// if err != nil {
	// 	return err
	// }
	// copyTarReader := tar.NewReader(copyGzipReader)
	// for {
	// 	hdr, err := copyTarReader.Next()
	// 	if err == io.EOF {
	// 		break
	// 	}
	// 	if err != nil {
	// 		return err
	// 	}
	// 	log.Println(hdr.Name)
	// }
	// copyGzipReader.Close()

	return nil
}
