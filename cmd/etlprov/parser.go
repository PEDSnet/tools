package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	dms "github.com/chop-dbhi/data-models-service/client"
)

const (
	TokenDelim  = ","
	EntityDelim = "."
)

// trimSpace trims the whitespace from each string in the passed slice.
func trimSpace(slice []string) []string {
	for i, v := range slice {
		slice[i] = strings.TrimSpace(v)
	}

	return slice
}

func indexOfInt(slice []int, v int) int {
	for i, s := range slice {
		if v == s {
			return i
		}
	}

	return -1
}

// Errors is a slice of errors to be aggregated and reported during validation.
type Errors []error

func (es Errors) Error() string {
	strs := make([]string, len(es))

	for i, e := range es {
		strs[i] = fmt.Sprintf("* %s", e)
	}

	return strings.Join(strs, "\n")
}

// flatten outputs a set of strings identifiers for the entities.
func flatten(m *dms.Model) []string {
	names := make([]string, 0)

	for _, t := range m.Tables.List() {
		for _, f := range t.Fields.List() {
			names = append(names, fmt.Sprintf("%s%s%s", t.Name, EntityDelim, f.Name))
		}
	}

	return names
}

type crCleaner struct {
	r io.Reader
}

func (c *crCleaner) Read(buf []byte) (int, error) {
	n, err := c.r.Read(buf)

	// Replace carriage returns with newlines
	for i, b := range buf {
		if b == '\r' {
			buf[i] = '\n'
		}
	}

	return n, err
}

// ReplaceCRs wraps an io.Reader and replaces carriage returns with newlines.
func ReplaceCRs(r io.Reader) *crCleaner {
	return &crCleaner{
		r: r,
	}
}

// Parser is an interface for parsing data a record of data.
type Parser interface {
	Parse([]string) (interface{}, error)
}

func parseFile(dir, name string, parser Parser) ([]interface{}, error) {
	f, err := os.Open(filepath.Join(dir, name))

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer f.Close()

	cr := csv.NewReader(ReplaceCRs(f))

	cr.Comment = '#'
	cr.LazyQuotes = true
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true

	var (
		record []string
		values []interface{}
		errs   Errors
	)

	for {
		record, err = cr.Read()

		if err == io.EOF {
			break
		}

		// In-place
		trimSpace(record)

		if err != nil {
			errs = append(errs, err)
			continue
		}

		value, err := parser.Parse(record)

		if err != nil {
			errs = append(errs, err)
		} else if value != nil {
			values = append(values, value)
		}
	}

	if len(errs) > 0 {
		fmt.Fprintf(os.Stderr, "A few problems have been detected with the %s file:\n%s\n", name, errs)
		return values, errs
	}

	fmt.Fprintf(os.Stderr, "File %s looks good!\n", name)
	return values, nil
}

type ToolParser struct {
	stepParser *StepParser
}

func (p *ToolParser) Parse(record []string) (interface{}, error) {
	// Skip header
	if record[0] == "name" {
		return nil, nil
	}

	t := Tool{}

	t.Name = record[0]

	steps, err := p.stepParser.parseSteps(record[1])

	if err != nil {
		return nil, err
	}

	t.Steps = steps
	t.Usage = record[2]
	t.Version = record[3]

	return &t, nil
}

var stepRange = regexp.MustCompile(`(\d+)\s*-\s*(\d+)`)

func (sp *StepParser) parseSteps(v string) ([]int, error) {
	steps := make([]int, 0)

	toks := strings.Split(v, TokenDelim)
	trimSpace(toks)

	for _, t := range toks {
		if match := stepRange.FindStringSubmatch(t); match != nil {
			// Ignore errors since regexp matched on them
			l, _ := strconv.Atoi(match[0])
			u, _ := strconv.Atoi(match[1])

			if u < l {
				return nil, errors.New(fmt.Sprintf("invalid step range %s", t))
			}

			for ; u <= l; u++ {
				if indexOfInt(sp.steps, u) == -1 {
					return nil, errors.New(fmt.Sprintf("step %s not defined", u))
				}

				steps = append(steps, u)
			}
		} else if t != "" {
			i, err := strconv.Atoi(t)

			if err != nil {
				return nil, errors.New(fmt.Sprintf("invalid step number %s", t))
			}

			if indexOfInt(sp.steps, i) == -1 {
				return nil, errors.New(fmt.Sprintf("step %s not defined", i))
			}

			steps = append(steps, i)
		}
	}

	return steps, nil
}

type StepParser struct {
	entityParser *EntityParser
	steps        []int
}

func (p *StepParser) Parse(record []string) (interface{}, error) {
	// Skip header
	if record[0] == "step" {
		return nil, nil
	}

	s := Step{}

	var (
		id   int
		prev int
		err  error
	)

	if record[0] == "" {
		id = -1
	} else {
		id, err = strconv.Atoi(record[0])

		if err != nil {
			return nil, err
		}
	}

	s.ID = id
	s.Description = record[1]

	entities, err := parseEntities(p.entityParser, record[2])

	if err != nil {
		return nil, err
	}

	s.Entities = entities

	if record[3] == "" {
		prev = -1
	} else {
		prev, err = strconv.Atoi(record[3])

		if err != nil {
			return nil, err
		}
	}

	s.PreviousStep = prev
	s.Time = record[4]

	p.steps = append(p.steps, s.ID)

	return &s, nil
}

func parseEntities(ep *EntityParser, s string) ([]string, error) {
	names := trimSpace(strings.Split(s, TokenDelim))

	for i, n := range names {
		if n == "all" || n == "" {
			return flatten(ep.model), nil
		}

		n, err := ep.ValidateName(n)

		if err != nil {
			return nil, err
		}

		names[i] = n
	}

	return names, nil
}

type SourceParser struct {
	stepParser *StepParser
}

func (p *SourceParser) Parse(record []string) (interface{}, error) {
	// Skip header
	if record[0] == "name" {
		return nil, nil
	}

	s := Source{}

	s.Name = record[0]

	steps, err := p.stepParser.parseSteps(record[1])

	if err != nil {
		return nil, err
	}

	s.Steps = steps
	s.Usage = record[2]
	s.Version = record[3]

	return &s, nil
}

type PersonParser struct {
	stepParser *StepParser
}

func (p *PersonParser) Parse(record []string) (interface{}, error) {
	// Skip header
	if record[0] == "name" {
		return nil, nil
	}

	b := Person{}

	b.Name = record[0]
	b.Email = record[1]
	b.Role = record[2]

	steps, err := p.stepParser.parseSteps(record[3])

	if err != nil {
		return nil, err
	}

	b.Steps = steps

	return &b, nil
}

type EntityParser struct {
	model *dms.Model
}

func (p *EntityParser) Parse(record []string) (interface{}, error) {
	// Skip header
	if record[0] == "entity" {
		return nil, nil
	}

	e := Entity{}

	name, err := p.ValidateName(record[0])

	if err != nil {
		return nil, err
	}

	e.Name = name

	avail, err := parseAvailability(record[1])

	if err != nil {
		return nil, err
	}

	e.Availability = avail

	trans, err := parseTransmitting(record[2])

	if err != nil {
		return nil, err
	}

	e.Transmitting = trans
	e.Comment = record[3]
	e.Truncation = record[4]
	e.Limit = record[5]

	return &e, nil
}

func (p *EntityParser) ValidateName(name string) (string, error) {
	// Tokens are expected to be period-delimited
	toks := strings.SplitN(strings.ToLower(name), EntityDelim, 2)

	table := p.model.Tables.Get(toks[0])

	if table == nil {
		return "", fmt.Errorf("Unknown table `%s`", toks[0])
	}

	if len(toks) == 1 {
		return name, nil
	}

	field := table.Fields.Get(toks[1])

	if field == nil {
		return "", fmt.Errorf("Unknown field `%s`", name)
	}

	return name, nil
}

func parseAvailability(v string) (string, error) {
	x := strings.ToLower(v)

	switch x {
	case "available", "unavailable", "unknown":
		return x, nil
	}

	return "", fmt.Errorf("Invalid choice for availability: %s", v)
}

func parseTransmitting(v string) (bool, error) {
	x := strings.ToLower(v)

	switch x {
	case "", "yes", "1", "true":
		return true, nil
	case "no", "0", "false":
		return false, nil
	}

	return false, fmt.Errorf("Invalid choice for transmitting: %s", v)
}
