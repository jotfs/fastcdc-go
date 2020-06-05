package fastcdc_test

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"math/rand"

	"github.com/jotfs/fastcdc-go"
)

func Example_basic() {

	data := make([]byte, 10*1024*1024)
	rand.Seed(4542)
	rand.Read(data)
	rd := bytes.NewReader(data)

	chunker, err := fastcdc.NewChunker(rd, fastcdc.Options{
		AverageSize: 1024 * 1024, // target 1 MiB average chunk size
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%-32s  %s\n", "CHECKSUM", "CHUNK SIZE")

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%x  %d\n", md5.Sum(chunk.Data), chunk.Length)
	}

	// Output:
	// CHECKSUM                          CHUNK SIZE
	// d5bb40f862d68f4c3a2682e6d433f0d7  1788060
	// 113a0aa2023d7dce6a2aac1f807b5bd2  1117240
	// 5b9147b10d4fe6f96282da481ce848ca  1180487
	// dcc4644befb599fa644635b0c5a1ea2c  1655501
	// 224db3de422ad0dd2c840e3e24e0cb03  363172
	// e071658eccda587789f1dabb6f773851  1227750
	// 215868103f0b4ea7f715e179e5b9a6c7  1451026
	// 21e65e40970ec22f5b13ddf60493b746  1150129
	// b8209a1dbef955ef64636af796450252  552395
}
