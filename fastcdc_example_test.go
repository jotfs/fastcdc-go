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
	// dee6e6c5cff96b97879c8ccc3a0816c4  1073134
	// febbb26d9293e4f7bbbd2690a2689bb0  1475338
	// f749cff5958a66592ae9a5e1040da2e0  733274
	// 1a111ef81439612d5ea511012fc53a99  1431958
	// 7d37d2aec1ce28f52a09a74c3a6afb3c  1108001
	// 045020dd21550af5c3494aab873b865e  901625
	// ed746b6c49369f31db6fe01f783abbbc  1433591
	// 666bdd16f26cc78fe682a124be777161  1230739
	// 044df6a4f25c7817f420e12939db71cf  1098100
}
