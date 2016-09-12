package results

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/blang/semver"
)

const (
	// The initial format that does not include the Github ID column. However
	// since all files were migrated to include the new column, this won't
	// be referenced.
	fileVersion1 uint8 = iota + 1

	// Adds the Github ID column.
	fileVersion2

	// Add the Method column and removes Reviewer, Site Response, and Goal.
	fileVersion3

	currentFileVersion = fileVersion3
)

var githubIssueURL = "https://github.com/PEDSnet/%s/issues/%s"

// inStringSlice returns true if the string is in the slice.
func inStringSlice(s string, l []string) bool {
	// Ignore leading and trailing whitespace.
	s = strings.TrimSpace(s)

	for _, x := range l {
		if s == x {
			return true
		}
	}

	return false
}

func fileHeader(v uint8) []string {
	switch v {
	case fileVersion1:
		return []string{
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

	case fileVersion2:
		return []string{
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
			"Github ID",
		}

	case fileVersion3:
		return []string{
			"Model",
			"Model Version",
			"Data Version",
			"DQA Version",
			"Table",
			"Field",
			"Issue Code",
			"Issue Description",
			"Finding",
			"Prevalence",
			"Rank",
			"Cause",
			"Status",
			"Github ID",
			"Method",
		}
	}

	panic("unknown file version")
}

// FileHeader stores the column position for each field.
type FileHeader struct {
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
	GithubID         int
	Method           int

	fileVersion uint8
}

func (h *FileHeader) Fields() []string {
	return fileHeader(h.fileVersion)
}

// NewFileHeader initializes a new file header. The latest version
// is already used.
func NewFileHeader() *FileHeader {
	head, err := ParseFileHeader(fileHeader(currentFileVersion))
	if err != nil {
		panic(fmt.Sprintf("Unexpected internal error: %s", err))
	}
	return head
}

func normalizeColName(s string) string {
	return strings.Replace(strings.ToLower(strings.TrimSpace(s)), " ", "_", -1)
}

// ParseFileHeader parses a file header and indexes the position of each field
// for accessing values. The filer version is determined by the fields present.
func ParseFileHeader(row []string) (*FileHeader, error) {
	h := FileHeader{
		fileVersion: fileVersion1,
	}

	for i, col := range row {
		col = normalizeColName(col)

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
		case "github_id", "githubid":
			h.GithubID = i
			h.fileVersion = fileVersion2
		case "method":
			h.Method = i
			h.fileVersion = fileVersion3
		default:
			return nil, fmt.Errorf("invalid column: %s", row[i])
		}
	}

	return &h, nil
}

// Result targets a Field.
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
	GithubID         string
	Method           string

	rank        string
	fileVersion uint8
}

func (r *Result) Row() []string {
	switch r.fileVersion {
	case fileVersion1:
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

	case fileVersion2:
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
			r.GithubID,
		}

	case fileVersion3:
		return []string{
			r.Model,
			r.ModelVersion,
			r.DataVersion,
			r.DQAVersion,
			r.Table,
			r.Field,
			r.IssueCode,
			r.IssueDescription,
			r.Finding,
			r.Prevalence,
			r.Rank.String(),
			r.Cause,
			r.Status,
			r.GithubID,
			r.Method,
		}
	}

	panic("unknown file version")
}

func (r *Result) String() string {
	return fmt.Sprintf("%s.%s", r.Table, r.Field)
}

func (r *Result) IsIssue() bool {
	return !r.IsPersistent() && r.IssueCode != ""
}

func (r *Result) IsPersistent() bool {
	return strings.ToLower(r.Status) == "persistent"
}

func (r *Result) IsUnresolved() bool {
	return strings.ToLower(r.Status) == "under review"
}

func (r *Result) SiteName() string {
	if r.DataVersion == "" {
		return ""
	}

	return strings.Split(r.DataVersion, "-")[2]
}

func (r *Result) ETLVersion() string {
	if r.DataVersion == "" {
		return ""
	}

	return strings.Split(r.DataVersion, "-")[3]
}

func (r *Result) GithubURL() string {
	if r.GithubID == "" {
		return ""
	}

	site := r.SiteName()

	if site == "" {
		return ""
	}

	// Template of the Github issues URL.
	return fmt.Sprintf(githubIssueURL, site, r.GithubID)
}

func NewResult() *Result {
	return &Result{
		fileVersion: currentFileVersion,
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

// File contains a set of results.
type File struct {
	Name    string
	Results Results

	fileVersion uint8
}

// String returns the name of associated with this file.
func (f *File) String() string {
	return f.Name
}

// Read reads results from an reader and adds them to the report.
func (f *File) Read(r io.Reader) (int, error) {
	rr, err := NewReader(r)

	results, err := rr.ReadAll()
	if err != nil {
		return 0, err
	}

	f.fileVersion = rr.head.fileVersion
	f.Results = append(f.Results, results...)
	sort.Sort(f.Results)

	return len(results), nil
}

// Validate results and returns a map of the result index to all errors for the result.
func (f *File) Validate() map[int][]string {
	errs := make(map[int][]string)

	for i, res := range f.Results {
		// Model version.
		if _, err := semver.Parse(res.ModelVersion); err != nil {
			errs[i] = append(errs[i], fmt.Sprintf("model version = '%s'", res.ModelVersion))
		}

		// Goal.
		if f.fileVersion < fileVersion3 && !inStringSlice(res.Goal, Goals) {
			errs[i] = append(errs[i], fmt.Sprintf("goal = '%s'", res.Goal))
		}

		// Prevalence.
		if res.Prevalence != "" && !inStringSlice(res.Prevalence, Prevalences) {
			errs[i] = append(errs[i], fmt.Sprintf("prevalence = '%s'", res.Prevalence))
		}

		// Rank.
		if res.Rank == 0 && res.rank != "" {
			errs[i] = append(errs[i], fmt.Sprintf("rank = '%s'", res.rank))
		}

		// Cause
		if res.Cause != "" && !inStringSlice(res.Cause, Causes) {
			errs[i] = append(errs[i], fmt.Sprintf("cause = '%s'", res.Cause))
		}

		// Status.
		if res.Status != "" && !inStringSlice(res.Status, Statuses) {
			errs[i] = append(errs[i], fmt.Sprintf("status = '%s'", res.Status))
		}
	}

	return errs
}

// NewFile initializes a new file of results.
func NewFile(name string) *File {
	return &File{
		Name:        name,
		fileVersion: currentFileVersion,
	}
}
