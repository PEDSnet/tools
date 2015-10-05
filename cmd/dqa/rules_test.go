package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

var (
	testRules = `
table,field,issue code,prevalence,rank
"in (condition_occurrence, visit_payer)",is primary key,G4-001,full,High
visit_payer,is source value,G2-013,"in (medium, high, low)",High
"visit_payer",is date/year,G2-002,unknown,High
"visit_payer","in (provider_id, care_site_id)",G2-013,"in (high, low)",Medium
"visit_payer","is concept id",G3-002,-,Medium
"visit_payer","is other",G3-002,-,Medium
`
	parsedRules = []struct {
		Rule      Rule
		TestField string
	}{
		// Line 1
		{
			Rule{
				Table:      "condition_occurrence",
				Condition:  isPrimaryKey,
				IssueCode:  "g4-001",
				Prevalence: "full",
				Rank:       HighRank,
			},
			"condition_occurrence_id",
		},
		{
			Rule{
				Table:      "visit_payer",
				Condition:  isPrimaryKey,
				IssueCode:  "g4-001",
				Prevalence: "full",
				Rank:       HighRank,
			},
			"visit_payer_id",
		},

		// Line 2
		{
			Rule{
				Table:      "visit_payer",
				Condition:  isSourceValue,
				IssueCode:  "g2-013",
				Prevalence: "medium",
				Rank:       HighRank,
			},
			"visit_payer_source_value",
		},
		{
			Rule{
				Table:      "visit_payer",
				Condition:  isSourceValue,
				IssueCode:  "g2-013",
				Prevalence: "high",
				Rank:       HighRank,
			},
			"visit_payer_source_value",
		},
		{
			Rule{
				Table:      "visit_payer",
				Condition:  isSourceValue,
				IssueCode:  "g2-013",
				Prevalence: "low",
				Rank:       HighRank,
			},
			"visit_payer_source_value",
		},

		// Line 3
		{
			Rule{
				Table:      "visit_payer",
				Condition:  isDateYear,
				IssueCode:  "g2-002",
				Prevalence: "unknown",
				Rank:       HighRank,
			},
			"visit_payer_date",
		},

		// Line 4
		{
			Rule{
				Table: "visit_payer",
				Condition: func(r *Result) bool {
					switch r.Field {
					case "provider_id", "care_site_id":
						return true
					}
					return false
				},
				IssueCode:  "g2-013",
				Prevalence: "high",
				Rank:       MediumRank,
			},
			"provider_id",
		},
		{
			Rule{
				Table: "visit_payer",
				Condition: func(r *Result) bool {
					switch r.Field {
					case "provider_id", "care_site_id":
						return true
					}
					return false
				},
				IssueCode:  "g2-013",
				Prevalence: "low",
				Rank:       MediumRank,
			},
			"care_site_id",
		},

		// Line 5
		{
			Rule{
				Table:      "visit_payer",
				Condition:  isConceptId,
				IssueCode:  "g3-002",
				Prevalence: "unknown",
				Rank:       MediumRank,
			},
			"visit_payer_concept_id",
		},

		// Line 6
		{
			Rule{
				Table:      "visit_payer",
				Condition:  isOther,
				IssueCode:  "g3-002",
				Prevalence: "unknown",
				Rank:       MediumRank,
			},
			"some_field",
		},
	}
)

func TestRulesParser(t *testing.T) {
	r := strings.NewReader(testRules)

	p, err := NewRulesParser(r)

	if err != nil {
		t.Fatal(err)
	}

	rules, err := p.ParseAll()

	if err != nil {
		t.Error(err)
	}

	if len(rules) != len(parsedRules) {
		t.Errorf("expected %d rules, got %d", len(parsedRules), len(rules))
		t.Fatal(rules)
	}

	for i, act := range rules {
		exp := parsedRules[i]

		if act.Table != exp.Rule.Table {
			t.Errorf("[%d] expected %s, got %s", i, act.Table, exp.Rule.Table)
		}

		if act.IssueCode != exp.Rule.IssueCode {
			t.Errorf("[%d] expected %s, got %s", i, act.IssueCode, exp.Rule.IssueCode)
		}

		if act.Prevalence != exp.Rule.Prevalence {
			t.Errorf("[%d] expected %s, got %s", i, act.Prevalence, exp.Rule.Prevalence)
		}

		if act.Rank != exp.Rule.Rank {
			t.Errorf("[%d] expected %s, got %s", i, act.Rank, exp.Rule.Rank)
		}

		res := &Result{
			Table:      act.Table,
			Field:      exp.TestField,
			IssueCode:  act.IssueCode,
			Prevalence: act.Prevalence,
		}

		if !exp.Rule.Condition(res) {
			panic(fmt.Sprintf("[%d] expected condition failed", i))
		}

		if !act.Condition(res) {
			t.Errorf("[%d] condition function doesn't match", i)
		}

		if _, ok := act.Matches(res); !ok {
			t.Errorf("[%d] rule does not match result", i)
		}
	}
}

func TestFetchRules(t *testing.T) {
	token := os.Getenv("GITHUB_AUTH_TOKEN")

	if token == "" {
		t.Skip()
	}

	sets, err := FetchRules(token)

	if err != nil {
		t.Fatal(err)
	}

	if len(sets) != 3 {
		t.Fatalf("expected 3 rule sets, got %d", len(sets))
	}

	for _, set := range sets {
		if len(set.Rules) == 0 {
			t.Errorf("[%s] no rules parsed", set)
		} else {
			t.Logf("[%s] contains %d rules", set, len(set.Rules))
		}
	}
}
