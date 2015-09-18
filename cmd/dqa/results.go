package main

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"text/template"
)

var (
	pedsnetTemplate = `{{with $R := .}}{{range .Sections}}# {{.Name}}
{{range .Tables}}## {{.Name}}
{{range .Ranks}}### {{.Name}}

{{range .Fields}}{{range .Results}}{{if .IssueCode}}- [ ] {{$R.Incr}}. **{{.Field}}**: {{.IssueDescription}} {{if .Finding}}({{.Finding}}){{end}}
{{end}}{{end}}{{end}}
{{end}}
{{end}}{{end}}{{end}}`

	i2b2Template = `{{with $R := .}}{{range .Tables}}# {{.Name}}

{{range .Fields}}{{range .Results}}- [ ] {{$R.Incr}}. **{{.Field}}**: {{.IssueDescription}} {{if .Finding}}({{.Finding}})
{{end}}{{end}}{{end}}
{{end}}
{{end}}`

	tmpl *template.Template
)

var (
	tableSections = map[string]map[string]int{
		"Demographic Tables": {
			"person":             0,
			"death":              1,
			"observation_period": 2,
		},

		"Fact Tables": {
			"visit_occurrence":     0,
			"condition_occurrence": 1,
			"procedure_occurrence": 2,
			"drug_exposure":        3,
			"observation":          4,
			"measurement":          5,
			"fact_relationship":    6,
			"visit_payer":          7,
		},

		"Admin Tables": {
			"care_site": 0,
			"location":  1,
			"provider":  2,
		},
	}

	sectionOrder = map[string]int{
		"Demographic Tables": 0,
		"Fact Tables":        1,
		"Admin Tables":       2,
		"Other Tables":       3,
	}
)

func init() {
	tmpl = template.New("reports")

	template.Must(tmpl.New("pedsnet").Parse(pedsnetTemplate))
	template.Must(tmpl.New("i2b2").Parse(i2b2Template))
}

// ResultGroupFunc is a function that returns the value of the result
// to be used for comparing and therefore grouping.
type ResultGroupFunc func(r *Result) (string, bool)

func ByRank(r *Result) (string, bool) {
	if r.Rank == 0 {
		return "", false
	}

	return r.Rank.String(), true
}

func ByTable(r *Result) (string, bool) {
	return r.Table, true
}

func BySection(r *Result) (string, bool) {
	for section, tables := range tableSections {
		if _, ok := tables[r.Table]; ok {
			return section, true
		}
	}

	return "Other Tables", true
}

func ByField(r *Result) (string, bool) {
	return r.Field, true
}

// ReadResultsFromDir reads all files in a directory and returns reports
// for each.
func ReadResultsFromDir(dir string, skip bool) (map[string]*Report, error) {
	fns, err := ioutil.ReadDir(dir)

	if err != nil {
		return nil, err
	}

	var (
		path string
		f    *os.File
	)

	reports := make(map[string]*Report)

	// Iterate over each CSV file in the directory.
	for _, fi := range fns {
		if fi.IsDir() {
			continue
		}

		path = filepath.Join(dir, fi.Name())

		if f, err = os.Open(path); err != nil {
			if skip {
				continue
			}

			return nil, err
		}

		report := &Report{}
		reports[fi.Name()] = report

		_, err := report.ReadResults(f)

		f.Close()

		// Presumably not a valid file.
		if err != nil {
			if skip {
				continue
			}

			return nil, err
		}
	}

	return reports, nil
}

// Report contains a set of results for a DQA analysis.
type Report struct {
	Name    string
	Results Results
	I2b2    bool

	seq *int
}

func (r *Report) String() string {
	return r.Name
}

// ReadResults reads results from an reader and adds them to the report.
func (r *Report) ReadResults(reader io.Reader) (int, error) {
	var (
		n      int
		err    error
		result *Result
	)

	rr, err := NewResultsReader(reader)

	if err != nil {
		return 0, err
	}

	for {
		result, err = rr.ReadResult()

		if err != nil {
			if err == io.EOF {
				return n, nil
			}

			return n, err
		}

		r.Results = append(r.Results, result)
		n++
	}

	sort.Sort(r.Results)

	return n, nil
}

// Render renders the report to the io.Writer.
func (r *Report) Render(w io.Writer) error {
	var t *template.Template

	if r.I2b2 {
		t = tmpl.Lookup("i2b2")
	} else {
		t = tmpl.Lookup("pedsnet")
	}

	return t.Execute(w, r)
}

// Sub creates a set of sub-reports by the ResultGroupFunc.
func (r *Report) Sub(f ResultGroupFunc) []*Report {
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

		if s.Status == "persistent" {
			continue
		}

		if s.IssueCode == "" {
			continue
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

	var groups []*Report

	for _, key = range keys {
		if len(gs[key].Results) == 0 {
			continue
		}

		groups = append(groups, gs[key])
	}

	return groups
}

func (r *Report) Sections() []*Report {
	rs := r.Sub(BySection)

	sortReports(rs, func(a, b *Report) bool {
		return sectionOrder[a.Name] < sectionOrder[b.Name]
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

func (r *Report) Ranks() []*Report {
	rs := r.Sub(ByRank)

	sortReports(rs, func(a, b *Report) bool {
		return a.Results[0].Rank < b.Results[0].Rank
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

// Reports is a set of reports.
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

// ResultsReader reads a DQA exposing a header with mapped positions
// to the field.
type ResultsReader struct {
	Head *ResultsReaderHeader
	csv  *csv.Reader
}

// Read reads the next raw row from the underlying CSV reader.
func (r *ResultsReader) Read() ([]string, error) {
	return r.csv.Read()
}

// ReadResult returns a result
func (r *ResultsReader) ReadResult() (*Result, error) {
	row, err := r.Read()

	if err != nil {
		return nil, err
	}

	var rank Rank

	switch row[r.Head.Rank] {
	case "High":
		rank = HighRank
	case "Medium":
		rank = MediumRank
	case "Low":
		rank = LowRank
	}

	return &Result{
		Model:            row[r.Head.Model],
		ModelVersion:     row[r.Head.ModelVersion],
		DataVersion:      row[r.Head.DataVersion],
		DQAVersion:       row[r.Head.DQAVersion],
		Table:            row[r.Head.Table],
		Field:            row[r.Head.Field],
		Goal:             row[r.Head.Goal],
		IssueCode:        row[r.Head.IssueCode],
		IssueDescription: row[r.Head.IssueDescription],
		Finding:          row[r.Head.Finding],
		Prevalence:       row[r.Head.Prevalence],
		Rank:             rank,
		SiteResponse:     row[r.Head.SiteResponse],
		Cause:            row[r.Head.Cause],
		Status:           row[r.Head.Status],
		Reviewer:         row[r.Head.Reviewer],
	}, nil
}

// NewResultsReader initializes a new results reader.
func NewResultsReader(r io.Reader) (*ResultsReader, error) {
	cr := csv.NewReader(&UniversalReader{r})

	cr.FieldsPerRecord = len(ResultsTemplateHeader)
	cr.Comment = '#'
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true

	row, err := cr.Read()

	if err != nil {
		return nil, err
	}

	head, err := NewResultsHeader(row)

	if err != nil {
		return nil, err
	}

	return &ResultsReader{
		Head: head,
		csv:  cr,
	}, nil
}

// ResultsWriter reads a DQA exposing a header with mapped positions
// to the field.
type ResultsWriter struct {
	csv  *csv.Writer
	head bool
}

// Read reads the next raw row from the underlying CSV reader.
func (w *ResultsWriter) Write(r *Result) error {
	if !w.head {
		if err := w.csv.Write(ResultsTemplateHeader); err != nil {
			return err
		}

		w.head = true
	}

	return w.csv.Write(r.Row())
}

func (w *ResultsWriter) Flush() error {
	w.csv.Flush()
	return w.csv.Error()
}

func NewResultsWriter(w io.Writer) *ResultsWriter {
	return &ResultsWriter{
		csv: csv.NewWriter(w),
	}
}
