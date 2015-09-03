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
	pedsnetTemplate = `{{with $R := .}}{{range .Ranks}}# {{.Name}}

{{range .Tables}}## {{.Name}}

{{range .Fields}}{{range .Results}}{{if .IssueCode}}- [ ] {{$R.Incr}}. **{{.Field}}**: {{.IssueDescription}} {{if .Finding}}({{.Finding}}){{end}}
{{end}}{{end}}{{end}}
{{end}}
{{end}}{{end}}`

	i2b2Template = `{{with $R := .}}{{range .Tables}}# {{.Name}}

{{range .Fields}}{{range .Results}}{{if .IssueCode}}- [ ] {{$R.Incr}}. **{{.Field}}**: {{.IssueDescription}} {{if .Finding}}({{.Finding}}){{end}}
{{end}}{{end}}{{end}}
{{end}}
{{end}}`

	tmpl *template.Template
)

func init() {
	tmpl = template.New("reports")

	template.Must(tmpl.New("pedsnet").Parse(pedsnetTemplate))
	template.Must(tmpl.New("i2b2").Parse(i2b2Template))
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

type Rank int

func (r Rank) String() string {
	switch r {
	case HighRank:
		return "High"
	case MediumRank:
		return "Medium"
	case LowRank:
		return "Low"
	}

	return ""
}

const (
	_ Rank = iota
	HighRank
	MediumRank
	LowRank
)

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

	var rank Rank

	switch row[11] {
	case "High":
		rank = HighRank
	case "Medium":
		rank = MediumRank
	case "Low":
		rank = LowRank
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
		Rank:             rank,
		SiteResponse:     row[12],
		Cause:            row[13],
		Status:           row[14],
		Reviewer:         row[15],
	}
}

type Results []*Result

func (r Results) Less(i, j int) bool {
	return r[i].Field < r[j].Field
}

func (r Results) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r Results) Len() int {
	return len(r)
}

type GroupFunc func(r *Result) (string, bool)

func ByRank(r *Result) (string, bool) {
	if r.Rank == 0 {
		return "", false
	}

	return r.Rank.String(), true
}

func ByTable(r *Result) (string, bool) {
	return r.Table, true
}

func ByField(r *Result) (string, bool) {
	return r.Field, true
}

// Report contains a set of results for a DQA analysis.
type Report struct {
	Name    string
	Results Results
	I2b2    bool

	seq *int
}

type Reports []*Report

type reportSorter struct {
	items Reports
	less  func(a, b *Report) bool
}

func (s *reportSorter) Len() int {
	return len(s.items)
}

func (s *reportSorter) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
}

func (s *reportSorter) Less(i, j int) bool {
	return s.less(s.items[i], s.items[j])
}

func sortReports(reports []*Report, less func(a, b *Report) bool) {
	sort.Sort(&reportSorter{
		items: reports,
		less:  less,
	})
}

// Sub creates a set of sub-reports by the GroupFunc.
func (r *Report) Sub(f GroupFunc) []*Report {
	gs := make(map[string]*Report)

	var (
		g    *Report
		ok   bool
		keep bool
		key  string
		keys []string
	)

	for _, s := range r.Results {
		if r.I2b2 {
			if s.Cause != "i2b2 transform" || s.Status != "solution proposed" {
				continue
			}
		}

		if key, keep = f(s); !keep {
			continue
		}

		if g, ok = gs[key]; !ok {
			keys = append(keys, key)

			g = &Report{
				Name: key,
				seq:  r.seq,
			}

			gs[key] = g
		}

		g.Results = append(g.Results, s)
	}

	sort.Strings(keys)

	groups := make([]*Report, len(keys))

	var i int

	for _, key = range keys {
		groups[i] = gs[key]
		i++
	}

	return groups
}

func (r *Report) Ranks() []*Report {
	rs := r.Sub(ByRank)

	sortReports(rs, func(a, b *Report) bool {
		return a.Results[0].Rank < b.Results[0].Rank
	})

	return rs
}

func (r *Report) Tables() []*Report {
	rs := r.Sub(ByTable)

	sortReports(rs, func(a, b *Report) bool {
		return a.Name < b.Name
	})

	return rs
}

func (r *Report) Fields() []*Report {
	rs := r.Sub(ByField)

	sortReports(rs, func(a, b *Report) bool {
		return a.Name < b.Name
	})

	return rs
}

func (r *Report) Incr() int {
	*r.seq++
	return *r.seq
}

func NewReport(name string) *Report {
	var seq int

	return &Report{
		Name: name,
		seq:  &seq,
	}
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
	Rank             Rank
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

		report.Results = append(report.Results, csvResult(row))
		n++
	}

	sort.Sort(report.Results)

	return n, nil
}

func Render(w io.Writer, r *Report) error {
	var t *template.Template

	if r.I2b2 {
		t = tmpl.Lookup("i2b2")
	} else {
		t = tmpl.Lookup("pedsnet")
	}

	return t.Execute(w, r)
}
