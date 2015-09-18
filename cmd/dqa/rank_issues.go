package main

import (
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rankIssuesCmd = &cobra.Command{
	Use: "assign-rank-to-issues <path>",

	Short: "Assigns ranks to detected issues in DQA analysis results.",

	Example: `
  pedsnet-dqa assign-rank-to-issues SecondaryReports/CHOP/ETLv4`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			cmd.Usage()
			return
		}

		dryRun := viper.GetBool("rankissues.dryrun")

		reports, err := ReadResultsFromDir(args[0], true)

		if err != nil {
			log.Fatal(err)
		}

		var (
			path string
			f    *os.File
		)

		bold := color.New(color.Bold, color.FgGreen).SprintFunc()

		var (
			changedText string
			rankText    string
		)

		var matches rankMatches

		for fname, report := range reports {
			fileChanged := false

			for _, r := range report.Results {
				changedText = "No"

				if ruleset, rank, ok := RunRules(r); ok {
					if r.Rank != rank {
						changedText = bold("Yes")
						rankText = bold(rank.String())
						fileChanged = true
					} else {
						rankText = r.Rank.String()
					}

					matches = append(matches, []string{
						ruleset.Name,
						r.Table,
						r.Field,
						r.IssueCode,
						r.Prevalence,
						rankText,
						r.Rank.String(),
						changedText,
					})

					r.Rank = rank
				}
			}

			if !dryRun && fileChanged {
				path = filepath.Join(args[0], fname)

				// Open the save file for writing.
				if f, err = os.Create(path); err != nil {
					log.Printf("error opening file: %s", err)
					continue
				}

				rw := NewResultsWriter(f)

				for _, r := range report.Results {
					if err = rw.Write(r); err != nil {
						log.Printf("error writing to file: %s", err)
						break
					}
				}

				if err = rw.Flush(); err != nil {
					log.Printf("error flushing file: %s", err)
				}

				f.Close()
			}
		}

		// If there are matches, print them out in a table.
		if len(matches) > 0 {
			tw := tablewriter.NewWriter(os.Stdout)

			tw.SetHeader([]string{
				"type",
				"table",
				"field",
				"issue code",
				"prevalence",
				"new rank",
				"old rank",
				"changed",
			})

			sort.Sort(matches)

			tw.AppendBulk([][]string(matches))

			tw.Render()
		}
	},
}

type rankMatches [][]string

func (r rankMatches) Len() int {
	return len(r)
}

func (r rankMatches) Less(i, j int) bool {
	a := r[i]
	b := r[j]

	// Type
	if a[0] < b[0] {
		return true
	} else if a[0] > b[0] {
		return false
	}

	// Table
	if a[1] < b[1] {
		return true
	} else if a[1] > b[1] {
		return false
	}

	// Field
	return a[2] < b[2]
}

func (r rankMatches) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func init() {
	flags := rankIssuesCmd.Flags()

	flags.Bool("dryrun", false, "Outputs a summary of what rank matches without saving the files.")

	viper.BindPFlag("rankissues.dryrun", flags.Lookup("dryrun"))
}
