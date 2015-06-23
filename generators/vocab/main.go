package main

import (
	"compress/gzip"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

var usageText = `usage: origins-generate-pedsnet-vocab <file> [options]

This command takes an CSV/TSV file with OMOP-based concepts and converts them
into Origins facts. The fact data is written to stdout.

Options:
`

var defaultUsage = flag.Usage

var ErrInvalidHeader = errors.New("header does not have concept_id or vocabulary_id")

var originsHeader = []string{
	"domain",
	"entity",
	"attribute_domain",
	"attribute",
	"value",
}

func indexHeader(r []string) (map[string]int, error) {
	var id, vocab bool

	index := make(map[string]int)

	for i, c := range r {
		c = strings.ToLower(strings.TrimSpace(c))
		r[i] = c
		index[c] = i

		if c == "concept_id" {
			id = true
		} else if c == "vocabulary_id" {
			vocab = true
		}
	}

	if !id || !vocab {
		return nil, ErrInvalidHeader
	}

	return index, nil
}

func main() {
	// Alter the usage function.
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, usageText)
		flag.PrintDefaults()
	}

	var (
		delim   string
		gzipped bool
	)

	flag.StringVar(&delim, "delim", ",", "delimiter between columns.")
	flag.BoolVar(&gzipped, "gzip", false, "gzip the output stream.")

	flag.Parse()

	if len(delim) > 1 {
		log.Fatal("delimiter must be a single character")
	}

	args := flag.Args()

	if len(args) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	// Filename is required.
	fin, err := os.Open(args[0])

	if err != nil {
		log.Fatal(err)
	}

	defer fin.Close()

	// Setup CSV reader.
	cr := csv.NewReader(fin)

	cr.Comma = rune(delim[0])
	cr.LazyQuotes = true

	// Begin reading.
	var (
		header []string
		row    []string
		index  map[string]int
	)

	if header, err = cr.Read(); err != nil {
		log.Fatalf("error reading file: %s", err)
	}

	// Check and clean the header.
	index, err = indexHeader(header)

	if err != nil {
		log.Fatal(err)
	}

	var fout io.Writer

	fout = os.Stdout

	if gzipped {
		fout = gzip.NewWriter(fout)
	}

	cw := csv.NewWriter(fout)

	// Start with the required header.
	cw.Write(originsHeader)

	var (
		i      int
		domain string
		entity string
		attr   string
	)

	for {
		row, err = cr.Read()

		if err != nil {
			if err == io.EOF {
				break
			}

			log.Fatal(err)
		}

		// Domain is vocabulary
		domain = fmt.Sprintf("pedsnet.concepts.%s", row[index["vocabulary_id"]])

		// Concept id is the entity identifier of the concept.
		entity = row[index["concept_id"]]

		// Each column in the header is expanded into a fact.
		for i, attr = range header {
			// entity, attribute, value
			cw.Write([]string{
				domain,
				entity,
				"datamodels.omop.v5.concept",
				attr,
				row[i],
			})
		}

		if err = cw.Error(); err != nil {
			log.Fatalf("error writing: %s", err)
		}
	}

	cw.Flush()

	// Close the gzip writer.
	if gzipped {
		fout.(*gzip.Writer).Close()
	}
}
