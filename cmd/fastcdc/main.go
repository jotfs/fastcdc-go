package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jotfs/fastcdc-go"
)

const kiB = 1024
const miB = 1024 * kiB

var defaultOpts = fastcdc.Options{
	AverageSize: 1 * miB,
}

var fileName = flag.String("file", "", "input file (required)")
var avgSize = flag.Int("avg", 1*miB, "average chunk size")
var minSize = flag.Int("min", 0, "minimum chunk size. (default avg / 4)")
var maxSize = flag.Int("max", 0, "maximum chunk size (default avg * 4)")
var normalization = flag.Int("normalization", 0, "normalization level (default 2)")
var disableNormalization = flag.Bool("no-normalization", false, "disable normalization (default false)")
var csv = flag.Bool("csv", false, "output as CSV (default false)")

func main() {
	flag.Parse()
	if *fileName == "" {
		fatalf("flag -file is required")
	}
	f, err := os.Open(*fileName)
	if err != nil {
		fatalf("unable to open file: %v", err)
	}

	chunker, err := fastcdc.NewChunker(f, fastcdc.Options{
		AverageSize:          *avgSize,
		MinSize:              *minSize,
		MaxSize:              *maxSize,
		Normalization:        *normalization,
		DisableNormalization: *disableNormalization,
	})
	if err != nil {
		fatalf("%v", err)
	}

	if *csv {
		fmt.Printf("%s,%s\n", "Offset", "Size")
	} else {
		fmt.Printf("%9s  %9s\n", "OFFSET", "SIZE")
	}

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
		if *csv {
			fmt.Printf("%d,%d\n", chunk.Offset, chunk.Length)
		} else {
			fmt.Printf("%9d  %9d\n", chunk.Offset, chunk.Length)
		}
	}

}

func fatalf(format string, a ...interface{}) {
	format = fmt.Sprintf("ERROR: %s\n", format)
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
}
