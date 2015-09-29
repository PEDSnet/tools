package main

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidHeader = errors.New("invalid results header")

// Rank is an ordered enumeration of result issue rankings.
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

// Goals are a set of goals that each field try to achieve. For template generation,
// a result line is created for each goal.
var Goals = []string{
	"Fidelity",
	"Consistency",
	"Accuracy",
	"Feasibility",
}

// ExcludedTables are a set of tables excluded from analysis.
var ExcludedTables = map[string]struct{}{
	"concept":               struct{}{},
	"concept_ancestor":      struct{}{},
	"concept_class":         struct{}{},
	"concept_relationship":  struct{}{},
	"concept_synonym":       struct{}{},
	"domain":                struct{}{},
	"source_to_concept_map": struct{}{},
	"relationship":          struct{}{},
	"vocabulary":            struct{}{},
}

// ResultsReaderHeader stores the column position for each field.
type ResultsReaderHeader struct {
	Model            int
	ModelVersion     int
	DataVersion      int
	DQAVersion       int
	Table            int
	Field            int
	Goal             int
	IssueCode        int
	IssueDescription int
	Finding          int
	Prevalence       int
	Rank             int
	SiteResponse     int
	Cause            int
	Status           int
	Reviewer         int
}

func NewResultsHeader(row []string) (*ResultsReaderHeader, error) {
	if len(row) != len(ResultsTemplateHeader) {
		return nil, ErrInvalidHeader
	}

	h := ResultsReaderHeader{}

	for i, col := range row {
		// Normalize column name for comparison.
		col = strings.Replace(strings.ToLower(strings.TrimSpace(col)), " ", "_", -1)

		switch col {
		case "model":
			h.Model = i
		case "model_version":
			h.ModelVersion = i
		case "data_version":
			h.DataVersion = i
		case "dqa_version":
			h.DQAVersion = i
		case "table":
			h.Table = i
		case "field":
			h.Field = i
		case "goal":
			h.Goal = i
		case "issue_code":
			h.IssueCode = i
		case "issue_description":
			h.IssueDescription = i
		case "finding":
			h.Finding = i
		case "prevalence":
			h.Prevalence = i
		case "rank":
			h.Rank = i
		case "site_response":
			h.SiteResponse = i
		case "cause":
			h.Cause = i
		case "status":
			h.Status = i
		case "reviewer":
			h.Reviewer = i
		default:
			return nil, fmt.Errorf("invalid column: %s", row[i])
		}
	}

	return &h, nil
}

// Header of a DQA results file.
var ResultsTemplateHeader = []string{
	"Model",
	"Model Version",
	"Data Version",
	"DQA Version",
	"Table",
	"Field",
	"Goal",
	"Issue Code",
	"Issue Description",
	"Finding",
	"Prevalence",
	"Rank",
	"Site Response",
	"Cause",
	"Status",
	"Reviewer",
}

type ResultsTemplate struct {
	Model        string
	ModelVersion string
	SiteName     string
	Extract      string
	DataVersion  string
	Version      string
}

func NewResultsTemplate(m, v, s, e string) *ResultsTemplate {
	return &ResultsTemplate{
		Model:        m,
		ModelVersion: v,
		SiteName:     s,
		Extract:      e,
		DataVersion:  fmt.Sprintf("%s-%s-%s-%s", m, v, s, e),
		Version:      "0",
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
}

func (r *Result) Row() []string {
	return []string{
		r.Model,
		r.ModelVersion,
		r.DataVersion,
		r.DQAVersion,
		r.Table,
		r.Field,
		r.Goal,
		r.IssueCode,
		r.IssueDescription,
		r.Finding,
		r.Prevalence,
		r.Rank.String(),
		r.SiteResponse,
		r.Cause,
		r.Status,
		r.Reviewer,
	}
}

// Results is a sortable set of results by field. Each set should be specific
// to a table.
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
