package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	tsvExt = regexp.MustCompile(`(?i)\.tsv\b`)

	zeroTime = time.Time{}
)

func detectDelimiter(s string) rune {
	if tsvExt.MatchString(s) {
		return '\t'
	}

	// Assume CSV.
	return ','
}

// UniversalReader wraps an io.Reader and replaces carriage returns with newlines.
type UniversalReader struct {
	r io.Reader
}

func (c *UniversalReader) Read(buf []byte) (int, error) {
	n, err := c.r.Read(buf)

	// Replace carriage returns with newlines
	for i, b := range buf {
		if b == '\r' {
			buf[i] = '\n'
		}
	}

	return n, err
}

// NewUniversalReader returns a UniversalReader that wraps the passed io.Reader.
func NewUniversalReader(r io.Reader) *UniversalReader {
	return &UniversalReader{r}
}

type ConceptReader struct {
	csv.Reader

	line int
}

func (r *ConceptReader) parse(row []string) (*Concept, error) {
	if len(row) != 10 {
		return nil, fmt.Errorf("Wrong number of values on line %d. Expected 10 got %d\n%s", r.line+1, len(row), strings.Join(row, ","))
	}

	id, err := strconv.Atoi(row[0])

	if err != nil {
		return nil, fmt.Errorf("Error parsing concept_id: %s", err)
	}

	var (
		sd time.Time
		ed time.Time
	)

	if row[7] == "" {
		sd = zeroTime
	} else {
		sd, err = time.Parse("20060102", row[7])

		if err != nil {
			return nil, fmt.Errorf("Error parsing valid_start_date: %s", err)
		}
	}

	if row[8] == "20991231" || row[8] == "" {
		ed = zeroTime
	} else {
		ed, err = time.Parse("20060102", row[8])

		if err != nil {
			return nil, fmt.Errorf("Error parsing valid_end_date: %s", err)
		}
	}

	c := Concept{
		ConceptID:       id,
		ConceptName:     strings.TrimSpace(row[1]),
		DomainID:        strings.TrimSpace(row[2]),
		VocabularyID:    strings.TrimSpace(row[3]),
		ConceptClassID:  strings.TrimSpace(row[4]),
		StandardConcept: strings.TrimSpace(row[5]),
		ConceptCode:     strings.TrimSpace(row[6]),
		ValidStartDate:  sd,
		ValidEndDate:    ed,
		InvalidReason:   strings.TrimSpace(row[9]),
	}

	return &c, nil
}

func (r *ConceptReader) Read() (*Concept, error) {
	// Read the header.
	if r.line == 0 {
		_, err := r.Reader.Read()

		if err != nil {
			return nil, err
		}
	}

	row, err := r.Reader.Read()

	// End of file.
	if err == io.EOF {
		return nil, err
	}

	if err != nil {
		return nil, fmt.Errorf("Could not read line: %s", err)
	}

	r.line++

	// Sentinel? Skip.
	if row[0] == "0" {
		return r.Read()
	}

	return r.parse(row)
}

func NewConceptReader(r io.Reader) *ConceptReader {
	cr := ConceptReader{
		Reader: *csv.NewReader(NewUniversalReader(r)),
	}

	cr.Comment = '#'
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true
	cr.FieldsPerRecord = -1

	return &cr
}
