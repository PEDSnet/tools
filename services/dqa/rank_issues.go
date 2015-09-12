package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
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

		// Ensure this is a directory.
		fns, err := ioutil.ReadDir(args[0])

		if err != nil {
			log.Fatal(err)
		}

		var (
			path string
			f    *os.File
		)

		// Iterate over each CSV file in the directory.
		for _, fi := range fns {
			if fi.IsDir() {
				continue
			}

			path = filepath.Join(args[0], fi.Name())

			if f, err = os.Open(path); err != nil {
				log.Printf("error opening file: %s", err)
				continue
			}

			report := &Report{}

			_, err := report.ReadResults(f)

			f.Close()

			// Presumably not a valid file.
			if err != nil {
				log.Printf("error reading results: %s", err)
				continue
			}

			log.Printf("evaluating file %s", fi.Name())

			changed := false

			for i, result := range report.Results {
				if rank, ok := RunRules(result); ok {
					log.Printf("rule matched for result %d", i+1) // One base

					if result.Rank == rank {
						log.Printf("rank already set to %s; nothing to do", rank)
					} else {
						log.Printf("set rank from %s to %s", result.Rank, rank)
						result.Rank = rank
						changed = true
					}
				}
			}

			if changed {
				// Open the save file for writing.
				if f, err = os.Create(path); err != nil {
					log.Printf("error opening file: %s", err)
					continue
				}

				rw := NewResultsWriter(f)

				for _, result := range report.Results {
					if err = rw.Write(result); err != nil {
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
	},
}

func RunRules(r *Result) (Rank, bool) {
	for _, rule := range Rules {
		if rank, ok := rule.Matches(r); ok {
			return rank, ok
		}
	}

	return 0, false
}

func inSlice(s string, a []string) bool {
	for _, x := range a {
		if x == s {
			return true
		}
	}

	return false
}

type Rule struct {
	Tables []string
	Field  func(t, f string) bool
	Map    map[[2]string]Rank
}

func (r *Rule) Matches(s *Result) (Rank, bool) {
	if inSlice(s.Table, r.Tables) && r.Field(s.Table, s.Field) {
		rank, ok := r.Map[[2]string{strings.ToLower(s.IssueCode), strings.ToLower(s.Prevalence)}]
		return rank, ok
	}

	return 0, false
}

func isPrimaryKey(t, f string) bool {
	return f == fmt.Sprintf("%s_id", t)
}

func isSourceValue(t, f string) bool {
	return strings.HasSuffix(f, "_source_value")
}

func isConceptId(t, f string) bool {
	return strings.HasSuffix(f, "_concept_id")
}

func isForeignKey(t, f string) bool {
	return !isPrimaryKey(t, f) && strings.HasSuffix(f, "_id") && !isConceptId(t, f)
}

func isOther(t, f string) bool {
	return !isPrimaryKey(t, f) && !isForeignKey(t, f) && !isSourceValue(t, f) && !isConceptId(t, f)
}

var defaultTables = []string{
	"care_site",
	"location",
	"provider",
}

var Rules = []*Rule{
	{
		Tables: defaultTables,
		Field:  isPrimaryKey,
		Map: map[[2]string]Rank{
			{"g2-013", "high"}:   MediumRank,
			{"g2-013", "medium"}: LowRank,
			{"g2-013", "low"}:    LowRank,
		},
	},

	{
		Tables: defaultTables,
		Field:  isSourceValue,
		Map: map[[2]string]Rank{
			{"g2-011", "full"}:   MediumRank,
			{"g2-011", "medium"}: LowRank,
			{"g4-002", "full"}:   MediumRank,
			{"g4-002", "high"}:   MediumRank,
			{"g4-002", "medium"}: MediumRank,
			{"g4-002", "low"}:    LowRank,
		},
	},

	{
		Tables: defaultTables,
		Field:  isConceptId,
		Map: map[[2]string]Rank{
			{"g1-002", "high"}:   HighRank,
			{"g1-002", "medium"}: HighRank,
		},
	},

	{
		Tables: defaultTables,
		Field:  isForeignKey,
		Map: map[[2]string]Rank{
			{"g2-013", "high"}:   MediumRank,
			{"g2-013", "medium"}: LowRank,
			{"g2-013", "low"}:    LowRank,
			{"g4-002", "full"}:   MediumRank,
		},
	},

	{
		Tables: defaultTables,
		Field:  isOther,
		Map: map[[2]string]Rank{
			{"g2-011", "low"}:    LowRank,
			{"g4-002", "full"}:   MediumRank,
			{"g4-002", "high"}:   MediumRank,
			{"g4-002", "medium"}: MediumRank,
			{"g4-002", "low"}:    LowRank,
		},
	},
}
