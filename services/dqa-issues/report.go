package main

import (
	"encoding/csv"
	"errors"
	"io"
	"sort"
	"strings"
	"text/template"
)

var (
	markdownTemplate = `{{with $R := .}}{{range .Ranks}}# {{.Name}}

{{range .Tables}}## {{.Name}}

{{range .Fields}}{{range .Results}}{{if .IssueCode}}- [ ] {{$R.Incr}}. **{{.Field}}**: {{.IssueDescription}} {{if .Finding}}({{.Finding}})
{{end}}{{end}}{{end}}{{end}}
{{end}}
{{end}}{{end}}`

	tmpl *template.Template
)

func init() {
	tmpl = template.Must(template.New("dqa").Parse(markdownTemplate))
}

type universalReader struct {
	r io.Reader
}

func (r *universalReader) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)

	// Replace carriage returns with newlines
	for i, b := range buf {
		if b == '\r' {
			buf[i] = '\n'
		}
	}

	return n, err
}

var ErrInvalidHeader = errors.New("invalid results header")

// Header of a DQA results file.
var ResultsHeader = []string{
	"model",
	"model version",
	"data version",
	"dqa version",
	"table",
	"field",
	"goal",
	"issue code",
	"issue description",
	"finding",
	"prevalence",
	"rank",
	"site response",
	"cause",
	"status",
	"reviewer",
}

// CheckHeader checks the header of a results file is valid.
func CheckHeader(h []string) bool {
	if len(ResultsHeader) != len(h) {
		return false
	}

	for i, f := range h {
		f = strings.TrimSpace(strings.ToLower(f))

		if f != ResultsHeader[i] {
			return false
		}
	}

	return true
}

// csvResult builds a result from a CSV row. Since this is position-based,
// CheckHeader should have been called to confirm the ordering.
func csvResult(row []string) *Result {
	// Clean the values.
	for i, v := range row {
		row[i] = strings.TrimSpace(v)
	}

	return &Result{
		Model:            row[0],
		ModelVersion:     row[1],
		DataVersion:      row[2],
		DQAVersion:       row[3],
		Table:            row[4],
		Field:            row[5],
		Goal:             row[6],
		IssueCode:        row[7],
		IssueDescription: row[8],
		Finding:          row[9],
		Prevalence:       row[10],
		Rank:             row[11],
		SiteResponse:     row[12],
		Cause:            row[13],
		Status:           row[14],
		Reviewer:         row[15],
	}
}

// Report references a set of results for a DQA analysis.
type Report struct {
	ranks map[string]*Rank
	seq   int
}

func (r *Report) Incr() int {
	r.seq++
	return r.seq
}

// Ranks returns an ordered set of ranked results. Unranked results
// are not included in the output.
func (r *Report) Ranks() []*Rank {
	var (
		rk  *Rank
		ok  bool
		rks []*Rank
	)

	if rk, ok = r.ranks["High"]; ok {
		rks = append(rks, rk)
	}

	if rk, ok = r.ranks["Medium"]; ok {
		rks = append(rks, rk)
	}

	if rk, ok = r.ranks["Low"]; ok {
		rks = append(rks, rk)
	}

	return rks
}

func (r *Report) Add(result *Result) {
	var (
		k  *Rank
		t  *Table
		f  *Field
		ok bool
	)

	// Add the rank if it does not exist.
	if k, ok = r.ranks[result.Rank]; !ok {
		k = &Rank{
			Name:   result.Rank,
			tables: make(map[string]*Table),
		}

		r.ranks[k.Name] = k
	}

	// Add the table if it does not exist.
	if t, ok = k.tables[result.Table]; !ok {
		t = &Table{
			Name:   result.Table,
			fields: make(map[string]*Field),
		}

		k.tables[t.Name] = t
	}

	// Add the field if it does not exist.
	if f, ok = t.fields[result.Field]; !ok {
		f = &Field{
			Name: result.Field,
		}

		t.fields[f.Name] = f
	}

	// Append the result.
	f.Results = append(f.Results, result)
}

func NewReport() *Report {
	return &Report{
		ranks: make(map[string]*Rank),
	}
}

type Rank struct {
	Name   string
	tables map[string]*Table
}

// Fields returns a sorted array of tables by name.
func (r *Rank) Tables() []*Table {
	var i int

	names := make([]string, len(r.tables))

	for _, f := range r.tables {
		names[i] = f.Name
		i++
	}

	sort.Strings(names)

	tables := make([]*Table, len(r.tables))

	for i, name := range names {
		tables[i] = r.tables[name]
	}

	return tables
}

type Table struct {
	Name   string
	fields map[string]*Field
}

// Fields returns a sorted array of fields by name.
func (t *Table) Fields() []*Field {
	var i int

	names := make([]string, len(t.fields))

	for _, f := range t.fields {
		names[i] = f.Name
		i++
	}

	sort.Strings(names)

	fields := make([]*Field, len(t.fields))

	for i, name := range names {
		fields[i] = t.fields[name]
	}

	return fields
}

type Field struct {
	Name    string
	Results []*Result
}

// Result targets a specific goal an is tied to a Field.
type Result struct {
	Model            string
	ModelVersion     string
	DataVersion      string
	DQAVersion       string
	Table            string
	Field            string
	Goal             string
	IssueCode        string
	IssueDescription string
	Finding          string
	Prevalence       string
	Rank             string
	SiteResponse     string
	Cause            string
	Status           string
	Reviewer         string
	Source           string
}

// ReadResults reads results from an reader and adds them to the report.
func ReadResults(report *Report, reader io.Reader) (int, error) {
	var (
		err error
		row []string
	)

	cr := csv.NewReader(reader)
	cr.FieldsPerRecord = len(ResultsHeader)
	cr.Comment = '#'
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true

	// Read the header.
	if row, err = cr.Read(); err != nil {
		return 0, err
	}

	if !CheckHeader(row) {
		return 0, ErrInvalidHeader
	}

	var n int

	for {
		row, err = cr.Read()

		if err != nil {
			if err == io.EOF {
				return n, nil
			}

			return n, err
		}

		report.Add(csvResult(row))
		n++
	}

	return n, nil
}

func RenderMarkdown(w io.Writer, r *Report) error {
	return tmpl.Lookup("dqa").Execute(w, r)
}
