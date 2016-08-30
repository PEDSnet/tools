package issues

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/PEDSnet/tools/cmd/dqa/results"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use: "merge-issues <reportdir> <logfile>...",

	Short: "Merge issues into a secondary report.",

	Long: ``,

	Example: `Merge issues into a secondary report.
  pedsnet-dqa merge-issues SecondaryReports/CHOP/ETLv5 person_issue.csv

Multiple log files can be applied:
  pedsnet-dqa merge-issues SecondaryReports/CHOP/ETLv5 *.csv`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			cmd.Help()
			os.Exit(1)
		}

		dir := args[0]

		// Map of results files by filename.
		// Each filename corresponds to a table name.
		files, err := results.ReadFromDir(dir)
		if err != nil {
			cmd.Printf("Error reading files in '%s'\n", err)
			os.Exit(1)
		}

		// Count of issues merged by file name.
		merged := make(map[string]uint)

		// Process all files
		for _, fn := range args[1:] {
			issues, err := readIssues(fn)
			if err != nil {
				log.Println(err)
				continue
			}

			for _, issue := range issues {
				lookup := fmt.Sprintf("%s.csv", issue.Table)
				report, ok := files[lookup]
				if !ok {
					log.Fatalf("no report file for table: %s", issue.Table)
				}

				var (
					found bool
				)

				// Scan results for a match. If none is found, add it.
				for _, r := range report.Results {
					// Ensure we are comparing the correct result.
					if r.Model != issue.Model || r.ModelVersion != issue.ModelVersion || r.DataVersion != issue.DataVersion || r.Table != issue.Table {
						cmd.Println("comparing different versions")
						os.Exit(1)
					}

					if r.Field == issue.Field && r.IssueCode == issue.IssueCode {
						if r.IsUnresolved() || r.IsPersistent() {
							cmd.Printf("Conflict: %s/%s for issue code %s\n", issue.Table, issue.Field, issue.IssueCode)
						}

						found = true
						break
					}
				}

				if found {
					continue
				}

				if _, ok := merged[lookup]; !ok {
					merged[lookup] = 0
				}
				merged[lookup]++

				report.Results = append(report.Results, issue)
			}
		}

		if len(merged) == 0 {
			cmd.Println("No new issues found.")
			return
		}

		for name, count := range merged {
			file := files[name]
			sort.Sort(file.Results)

			// File opened successfully.
			f, err := os.Create(filepath.Join(dir, name))
			if err != nil {
				cmd.Printf("Error opening file to write new issues: %s\n", err)
				continue
			}
			defer f.Close()
			w := results.NewWriter(f)

			if err := w.WriteAll(file.Results); err != nil {
				cmd.Printf("Error writing results to file.")
				continue
			}

			if err := w.Flush(); err != nil {
				cmd.Printf("Error flushing results to file.")
				continue
			}

			cmd.Printf("Merged %d issues into %s\n", count, name)
		}
	},
}

func readIssues(fn string) ([]*results.Result, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cr := csv.NewReader(f)
	fields, err := cr.Read()

	head, err := checkFields(fields)
	if err != nil {
		return nil, err
	}

	var issues []*results.Result

	for {
		row, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		res := &results.Result{
			DataVersion:      row[head.DataVersion],
			DQAVersion:       "0",
			Table:            row[head.Table],
			Field:            row[head.Field],
			IssueCode:        row[head.IssueCode],
			IssueDescription: row[head.IssueDescription],
			Finding:          row[head.Finding],
			Prevalence:       row[head.Prevalence],
		}

		toks := strings.Split(res.DataVersion, "-")
		res.Model = toks[0]
		res.ModelVersion = toks[1]

		issues = append(issues, res)
	}

	return issues, nil
}

type issueFields struct {
	DataVersion      int
	Table            int
	Field            int
	IssueCode        int
	IssueDescription int
	Finding          int
	Prevalence       int
}

const numFields = 7

func checkFields(fields []string) (*issueFields, error) {
	var head issueFields
	var seen int

	for i, field := range fields {
		switch field {
		case "data_version":
			head.DataVersion = i

		case "table":
			head.Table = i

		case "field":
			head.Field = i

		case "issue_code":
			head.IssueCode = i

		case "issue_description":
			head.IssueDescription = i

		case "finding":
			head.Finding = i

		case "prevalence":
			head.Prevalence = i

		default:
			log.Println("unknown field %s", field)
			continue
		}

		seen++
	}

	if seen != numFields {
		return nil, errors.New("missing fields")
	}

	return &head, nil
}
