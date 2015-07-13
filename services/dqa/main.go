package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
)

const usage = `dqa-files [-model <model>]
                         [-version <version>]
                         [-root <root>]
                         [-data-models <url>]

                         <site> <extract>

Generates a set of DQA files for the site and extract version.
`

// Goals that each field can have a metric for.
var Goals = []string{
	"Fidelity",
	"Consistency",
	"Accuracy",
	"Feasibility",
}

var ExcludedTables = map[string]struct{}{
	"concept":               struct{}{},
	"concept_ancestor":      struct{}{},
	"concept_class":         struct{}{},
	"concept_relationship":  struct{}{},
	"concept_synonym":       struct{}{},
	"domain":                struct{}{},
	"source_to_concept_map": struct{}{},
	"vocabulary":            struct{}{},
}

// Header of a DQA report.
var ReportHeader = []string{
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

// Prints the usage of the command.
func PrintUsage() {
	fmt.Println(usage)
	flag.PrintDefaults()
	os.Exit(1)
}

type Template struct {
	Model        string
	ModelVersion string
	SiteName     string
	Extract      string
	DataVersion  string
	Version      string
}

func NewTemplate(m, v, s, e string) *Template {
	return &Template{
		Model:        m,
		ModelVersion: v,
		SiteName:     s,
		Extract:      e,
		DataVersion:  fmt.Sprintf("%s-%s-%s-%s", m, v, s, e),
		Version:      "0",
	}
}

type Model struct {
	Name    string
	Version string
	Tables  []*Table
}

type Table struct {
	Name    string
	Model   string
	Version string
	Fields  []*Field
}

type Field struct {
	Name  string
	Table string
}

// modelFields retrieves all model fields from the DataModels service.
func fetchModel(base, model, version string) (*Model, error) {
	u, err := url.Parse(base)

	if err != nil {
		return nil, err
	}

	u.Path = fmt.Sprintf("models/%s/%s", model, version)

	req, err := http.NewRequest("GET", u.String(), nil)

	if err != nil {
		return nil, err
	}

	// We want the JSON output.
	req.Header.Add("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var m Model

	if err = json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}

	return &m, nil
}

func main() {
	var (
		root    string
		model   string
		version string
		url     string
	)

	flag.StringVar(&root, "root", "", "Root directory of output directory.")
	flag.StringVar(&model, "model", "pedsnet", "The model the DQA files are generated for.")
	flag.StringVar(&version, "version", "v2", "The version of the model the DQA files are generated for.")
	flag.StringVar(&url, "data-models", "http://data-models.origins.link", "URL to a DataModels service.")

	flag.Parse()

	args := flag.Args()

	if len(args) < 2 {
		PrintUsage()
	}

	dqa := NewTemplate(model, version, args[0], args[1])

	dir := filepath.Join(root, dqa.SiteName, dqa.Extract)

	// Create the necessary directories to write the files to.
	if err := os.MkdirAll(dir, os.ModeDir|0775); err != nil {
		log.Fatal("mkdir:", err)
	}

	m, err := fetchModel(url, dqa.Model, dqa.ModelVersion)

	if err != nil {
		log.Fatal("fetch:", err)
	}

	var (
		p   string
		f   *os.File
		w   *csv.Writer
		row = make([]string, len(ReportHeader))
	)

	// Model level fields.
	row[0] = m.Name
	row[1] = m.Version
	row[2] = dqa.DataVersion
	row[3] = dqa.Version

	// Create a file per table.
	for _, table := range m.Tables {
		if _, ok := ExcludedTables[table.Name]; ok {
			continue
		}

		p = filepath.Join(dir, fmt.Sprintf("%s.csv", table.Name))

		if f, err = os.Create(p); err != nil {
			log.Fatal("create:", err)
		}

		// Initialize CSV writer and start with the header.
		w = csv.NewWriter(f)
		w.Write(ReportHeader)

		// Table level fields.
		row[4] = table.Name

		for _, field := range table.Fields {
			row[5] = field.Name

			for _, goal := range Goals {
				row[6] = goal
				w.Write(row)
			}

		}

		w.Flush()
		f.Close()
	}

	fmt.Printf("Wrote files to '%s' for model '%s/%s'\n", dir, m.Name, m.Version)
}
