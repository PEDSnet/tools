package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/blang/semver"
	"github.com/spf13/cobra"
)

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

func validateReport(report *Report) map[int][]string {
	errs := make(map[int][]string)

	for i, r := range report.Results {
		// Model version.
		if _, err := semver.Parse(r.ModelVersion); err != nil {
			errs[i] = append(errs[i], fmt.Sprintf("model version = '%s'", r.ModelVersion))
		}

		// Goal.
		if !inStringSlice(r.Goal, Goals) {
			errs[i] = append(errs[i], fmt.Sprintf("goal = '%s'", r.Goal))
		}

		// Prevalence.
		if r.Prevalence != "" && !inStringSlice(r.Prevalence, Prevalences) {
			errs[i] = append(errs[i], fmt.Sprintf("prevalence = '%s'", r.Prevalence))
		}

		// Rank.
		if r.Rank == 0 && r.rank != "" {
			errs[i] = append(errs[i], fmt.Sprintf("rank = '%s'", r.rank))
		}

		// Cause
		if r.Cause != "" && !inStringSlice(r.Cause, Causes) {
			errs[i] = append(errs[i], fmt.Sprintf("cause = '%s'", r.Cause))
		}

		// Status.
		if r.Status != "" && !inStringSlice(r.Status, Statuses) {
			errs[i] = append(errs[i], fmt.Sprintf("status = '%s'", r.Status))
		}
	}

	return errs
}

var validateCmd = &cobra.Command{
	Use: "validate <path>...",

	Short: "Validates the secondary reports.",

	Example: `
  pedsnet-dqa validate SecondaryReports/CHOP/ETLv4`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Println("At least one path is required.")
			os.Exit(1)
		}

		var errs map[int][]string

		for _, path := range args {
			stat, err := os.Stat(path)

			if err != nil {
				fmt.Printf("Error inspecting file '%s': %s\n", path, err)
				continue
			}

			// Ignore normal files.
			if !stat.IsDir() {
				continue
			}

			reports, err := ReadResultsFromDir(path, true)

			if err != nil {
				fmt.Printf("Error reading files from '%s': %s\n", path, err)
				continue
			}

			fmt.Println(path)

			hasErrors := false

			for name, report := range reports {
				errs = validateReport(report)

				if len(errs) > 0 {
					hasErrors = true

					fmt.Printf("* Errors found in '%s':\n", name)

					for line, msgs := range errs {
						fmt.Printf("    line %d: %s\n", line, strings.Join(msgs, ", "))
					}

					fmt.Println("")
				}
			}

			if !hasErrors {
				fmt.Println("* Everything looks good!\n")
			}
		}
	},
}
