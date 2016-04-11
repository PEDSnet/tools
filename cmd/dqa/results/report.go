package results

import (
	"html/template"
	"io"
	"sort"
	"sync"
)

var (
	tmpl *template.Template

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
	tmpl = template.New("results")

	template.Must(tmpl.New("pedsnet").Parse(pedsnetTemplate))
	template.Must(tmpl.New("i2b2").Parse(i2b2Template))
}

// MarkdownReport
type MarkdownReport struct {
	File *File
}

// prepareResults removes results that should not be added in the report.
func (m *MarkdownReport) prepareResults() Results {
	var out Results

	for _, r := range m.File.Results {
		// i2b2-related conditions.
		if m.File.I2b2 {
			if r.Cause != "i2b2 transform" || r.Status != "solution proposed" {
				continue
			}
		} else if r.Cause == "i2b2 transform" {
			continue
		}

		if r.Status == "persistent" {
			continue
		}

		if r.IssueCode == "" {
			continue
		}

		out = append(out, r)
	}

	return out
}

// Render renders the report to the io.Writer.
func (r *MarkdownReport) Render(w io.Writer) error {
	var t *template.Template

	if r.File.I2b2 {
		t = tmpl.Lookup("i2b2")
	} else {
		t = tmpl.Lookup("pedsnet")
	}

	var seq int

	s := ResultSection{
		Results: r.prepareResults(),
		seq:     &seq,
	}

	return t.Execute(w, &s)
}

func NewMarkdownReport(f *File) *MarkdownReport {
	return &MarkdownReport{
		File: f,
	}
}

type ResultSection struct {
	Name    string
	Results Results

	// Pointer to a int that keeps a sequence number for all sub-sections.
	seq *int
	mux sync.Mutex
}

func (r *ResultSection) Sections() []*ResultSection {
	rs := splitSection(r, bySection)

	sortSections(rs, func(a, b *ResultSection) bool {
		return sectionOrder[a.Name] < sectionOrder[b.Name]
	})

	return rs
}

func (r *ResultSection) Tables() []*ResultSection {
	rs := splitSection(r, byTable)

	sortSections(rs, func(a, b *ResultSection) bool {
		return a.Name < b.Name
	})

	return rs
}

func (r *ResultSection) Ranks() []*ResultSection {
	rs := splitSection(r, byRank)

	sortSections(rs, func(a, b *ResultSection) bool {
		return a.Results[0].Rank < b.Results[0].Rank
	})

	return rs
}

func (r *ResultSection) Fields() []*ResultSection {
	rs := splitSection(r, byField)

	sortSections(rs, func(a, b *ResultSection) bool {
		return a.Name < b.Name
	})

	return rs
}

func (r *ResultSection) Incr() int {
	r.mux.Lock()
	defer r.mux.Unlock()
	*r.seq++
	return *r.seq
}

// sectionSorter implements the sort.Sort inteface. The less function must be
// provided to do the comparison.
type sectionSorter struct {
	sections []*ResultSection
	less     func(a, b *ResultSection) bool
}

func (s *sectionSorter) Len() int {
	return len(s.sections)
}

func (s *sectionSorter) Swap(i, j int) {
	s.sections[i], s.sections[j] = s.sections[j], s.sections[i]
}

func (s *sectionSorter) Less(i, j int) bool {
	return s.less(s.sections[i], s.sections[j])
}

// sortSections takes a set of files and a function that performs the sort
// comparison.
func sortSections(sections []*ResultSection, less func(a, b *ResultSection) bool) {
	sort.Sort(&sectionSorter{
		sections: sections,
		less:     less,
	})
}

// splitSection splits the results in a file by the splitter.
// Note: this applies to report generation which is the the results
// are being filtered.
func splitSection(section *ResultSection, split sectionSplitter) []*ResultSection {
	var (
		g    *ResultSection
		ok   bool
		keep bool
		key  string
		keys []string
	)

	var groups []*ResultSection

	gs := make(map[string]*ResultSection)

	for _, r := range section.Results {
		if key, keep = split(r); !keep {
			continue
		}

		if g, ok = gs[key]; !ok {
			keys = append(keys, key)

			g = &ResultSection{
				Name: key,
				seq:  section.seq,
			}

			gs[key] = g
		}

		g.Results = append(g.Results, r)
	}

	sort.Strings(keys)

	for _, key = range keys {
		if len(gs[key].Results) == 0 {
			continue
		}

		groups = append(groups, gs[key])
	}

	return groups
}

// sectionSplitter is a function that returns the value of the result
// to be used for comparing and therefore grouping.
type sectionSplitter func(r *Result) (string, bool)

func byRank(r *Result) (string, bool) {
	if r.Rank == 0 {
		return "", false
	}

	return r.Rank.String(), true
}

func byTable(r *Result) (string, bool) {
	return r.Table, true
}

func bySection(r *Result) (string, bool) {
	for section, tables := range tableSections {
		if _, ok := tables[r.Table]; ok {
			return section, true
		}
	}

	return "Other Tables", true
}

func byField(r *Result) (string, bool) {
	return r.Field, true
}
