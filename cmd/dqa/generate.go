package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

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
		cpp := viper.GetString("generate.copy-persistent")

		var (
			err     error
			reports map[string]*Report
		)

		// Load the previous set of results.
		if cpp != "" {
			reports, err = ReadResultsFromDir(cpp, false)

			if err != nil {
				log.Fatal(err)
			}
		}

		dqa := NewResultsTemplate(model, version, args[0], args[1])

		dir := root

		// Create the necessary directories to write the files to.
		if err := os.MkdirAll(dir, os.ModeDir|0775); err != nil {
			log.Fatal("mkdir:", err)
		}

		m, err := FetchModel(url, dqa.Model, dqa.ModelVersion)

		if err != nil {
			log.Fatal("fetch:", err)
		}

		var (
			ok   bool
			p    string
			f    *os.File
			w    *csv.Writer
			res  *Result
			resl []*Result
			row  []string
		)

		head, err := NewResultsHeader(ResultsTemplateHeader)

		if err != nil {
			panic(err)
		}

		var pindex map[[2]string][]*Result

		// Create a file per table.
		for _, table := range m.Tables {
			if _, ok := ExcludedTables[table.Name]; ok {
				continue
			}

			p = filepath.Join(dir, fmt.Sprintf("%s.csv", table.Name))

			if reports != nil {
				if report, ok := reports[fmt.Sprintf("%s.csv", table.Name)]; ok {
					pindex = indexPersistentIssues(report)
				}
			}

			if f, err = os.Create(p); err != nil {
				log.Fatal("create:", err)
			}

			// Initialize CSV writer and start with the header.
			w = csv.NewWriter(f)
			w.Write(ResultsTemplateHeader)

			for _, field := range table.Fields {
				for _, goal := range Goals {
					row = make([]string, len(ResultsTemplateHeader))

					row[head.Model] = m.Name
					row[head.ModelVersion] = m.Version
					row[head.DataVersion] = dqa.DataVersion
					row[head.DQAVersion] = dqa.Version
					row[head.Table] = table.Name
					row[head.Field] = field.Name
					row[head.Goal] = goal

					if pindex == nil {
						w.Write(row)
						continue
					}

					// Iterate the persistent issues for a field an copy them.
					// Otherwise just write the row.
					if resl, ok = pindex[[2]string{field.Name, goal}]; ok {
						for _, res = range resl {
							row[head.IssueCode] = res.IssueCode
							row[head.IssueDescription] = res.IssueDescription
							row[head.Finding] = res.Finding
							row[head.Prevalence] = res.Prevalence
							row[head.Rank] = res.Rank.String()
							row[head.SiteResponse] = res.SiteResponse
							row[head.Cause] = res.Cause
							row[head.Status] = res.Status
							row[head.Reviewer] = res.Reviewer

							w.Write(row)
						}
					} else {
						w.Write(row)
					}
				}
			}

			// reset index
			pindex = nil

			w.Flush()
			f.Close()
		}

		fmt.Printf("Wrote files to '%s' for model '%s/%s'\n", dir, m.Name, m.Version)
		fmt.Printf("Copied persistent issues from '%s'\n", cpp)
	},
}

// Index persistent issues by field and goal. Multiple issues can be present
// so a slice is used here.
func indexPersistentIssues(r *Report) map[[2]string][]*Result {
	index := make(map[[2]string][]*Result)

	for _, res := range r.Results {
		if strings.ToLower(res.Status) == "persistent" {
			results := index[[2]string{res.Field, res.Goal}]
			results = append(results, res)
			index[[2]string{res.Field, res.Goal}] = results
		}
	}

	return index
}

func init() {
	flags := generateCmd.Flags()

	flags.String("root", "", "Root directory of output directory.")
	flags.String("model", "pedsnet", "The model the DQA files are generated for.")
	flags.String("version", "2.0.0", "The version of the model the DQA files are generated for.")
	flags.String("url", "http://data-models.origins.link", "URL to a DataModels service.")
	flags.String("copy-persistent", "", "Copies issues in the specified path with a status of 'persistent' from an existing analysis.")

	viper.BindPFlag("generate.root", flags.Lookup("root"))
	viper.BindPFlag("generate.model", flags.Lookup("model"))
	viper.BindPFlag("generate.version", flags.Lookup("version"))
	viper.BindPFlag("generate.url", flags.Lookup("url"))
	viper.BindPFlag("generate.copy-persistent", flags.Lookup("copy-persistent"))
}
