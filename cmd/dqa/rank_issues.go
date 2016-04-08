package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	dms "github.com/chop-dbhi/data-models-service/client"
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
		token := viper.GetString("rankissues.token")
		url := viper.GetString("rankissues.url")

		if token == "" {
			fmt.Fprintln(os.Stderr, "Token required. Use the --token option.")
			os.Exit(1)
		}

		// Read secondary reports from directory.
		reports, err := ReadResultsFromDir(args[0], true)

		if err != nil {
			log.Fatal(err)
		}

		// Get the data model name and version to validate against.
		var model, version string
		for _, report := range reports {
			model = report.Model
			version = report.Version
			break
		}

		cmd.Printf("Ranking against model '%s/%s'\n", model, version)

		client, err := dms.New(url)
		if err != nil {
			log.Fatalf("Could not connect to service %s: %s", url, err)
		}

		if err = client.Ping(); err != nil {
			log.Fatalf("Error communicating with the service: %s", err)
		}

		m, err := client.ModelRevision(model, version)
		if err != nil {
			log.Fatalf("Error fetching model: %s", err)
		}

		ruleSets, err := FetchRules(token, m)
		if err != nil {
			log.Fatal(err)
		}

		var stop bool
		for _, s := range ruleSets {
			if errs := s.Parser.ValidationErrors(); errs != nil {
				stop = true

				fmt.Fprintf(os.Stderr, "Validation errors in '%s' rules file\n", s.Name)
				for _, err := range errs {
					fmt.Fprintln(os.Stderr, err)
				}
			}
		}

		if stop {
			os.Exit(1)
		}

		var (
			path string
			f    *os.File
		)

		bold := color.New(color.Bold, color.FgGreen).SprintFunc()

		var (
			changedText    string
			oldRankText    string
			newRankText    string
			persistentText string
		)

		var matches rankMatches

		for fname, report := range reports {
			fileChanged := false

			for _, r := range report.Results {
				changedText = "No"

				if isPersistent(r) {
					persistentText = "Yes"
				} else {
					persistentText = "No"
				}

				if ruleset, rank, ok := RunRules(ruleSets, r); ok {
					oldRankText = r.Rank.String()
					newRankText = rank.String()

					if r.Rank != rank {
						changedText = bold("Yes")
						r.Rank = rank
						fileChanged = true
					}

					matches = append(matches, []string{
						ruleset.Name,
						r.Table,
						r.Field,
						r.Goal,
						r.IssueCode,
						r.Prevalence,
						newRankText,
						oldRankText,
						changedText,
						persistentText,
					})
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
				"goal",
				"issue code",
				"prevalence",
				"new rank",
				"old rank",
				"changed",
				"persistent",
			})

			sort.Sort(matches)

			tw.AppendBulk([][]string(matches))

			tw.Render()
		} else {
			fmt.Println("All ranks already match.")
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
	flags.String("token", "", "GitHub token to fetch the rules.")
	flags.String("url", dms.DefaultServiceURL, "Data models service URL.")

	viper.BindPFlag("rankissues.dryrun", flags.Lookup("dryrun"))
	viper.BindPFlag("rankissues.token", flags.Lookup("token"))
	viper.BindPFlag("rankissues.url", flags.Lookup("url"))
}
