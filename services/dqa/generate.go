package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

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
	"relationship":          struct{}{},
	"vocabulary":            struct{}{},
}

// Header of a DQA results file.
var TemplateHeader = []string{
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

var generateCmd = &cobra.Command{
	Use: "generate-templates <site> <extract>",

	Short: "Generates a set of DQA files for the site and extract version.",

	Example: `
  pedsnet-dqa generate-templates --root=SecondaryReports/CHOP/ETLv5 CHOP ETLv5`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			cmd.Usage()
			return
		}

		model := viper.GetString("generate.model")
		version := viper.GetString("generate.version")
		root := viper.GetString("generate.root")
		url := viper.GetString("generate.url")

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
			row = make([]string, len(TemplateHeader))
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
			w.Write(TemplateHeader)

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
	},
}

func init() {
	flags := generateCmd.Flags()

	flags.String("root", "", "Root directory of output directory.")
	flags.String("model", "pedsnet", "The model the DQA files are generated for.")
	flags.String("version", "2.0.0", "The version of the model the DQA files are generated for.")
	flags.String("url", "http://data-models.origins.link", "URL to a DataModels service.")

	viper.BindPFlag("generate.root", flags.Lookup("root"))
	viper.BindPFlag("generate.model", flags.Lookup("model"))
	viper.BindPFlag("generate.version", flags.Lookup("version"))
	viper.BindPFlag("generate.url", flags.Lookup("url"))
}
