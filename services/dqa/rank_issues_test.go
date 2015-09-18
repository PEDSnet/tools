package main

import "testing"

var ruleMatchTests = map[Result]Rank{
	// Primary key
	{
		Table:      "location",
		Field:      "location_id",
		IssueCode:  "g2-013",
		Prevalence: "high",
	}: MediumRank,

	{
		Table:      "care_site",
		Field:      "care_site_id",
		IssueCode:  "g2-013",
		Prevalence: "medium",
	}: LowRank,

	{
		Table:      "provider",
		Field:      "provider_id",
		IssueCode:  "g2-013",
		Prevalence: "low",
	}: LowRank,

	// Source value
	{
		Table:      "location",
		Field:      "location_source_value",
		IssueCode:  "g2-011",
		Prevalence: "full",
	}: MediumRank,

	{
		Table:      "care_site",
		Field:      "care_site_source_value",
		IssueCode:  "g4-002",
		Prevalence: "full",
	}: MediumRank,

	{
		Table:      "provider",
		Field:      "provider_source_value",
		IssueCode:  "g4-002",
		Prevalence: "low",
	}: LowRank,

	// Concept id
	{
		Table:      "location",
		Field:      "location_concept_id",
		IssueCode:  "g1-002",
		Prevalence: "high",
	}: HighRank,

	// Foreign key
	{
		Table:      "care_site",
		Field:      "location_id",
		IssueCode:  "g2-013",
		Prevalence: "high",
	}: MediumRank,

	{
		Table:      "provider",
		Field:      "care_site_id",
		IssueCode:  "g4-002",
		Prevalence: "full",
	}: MediumRank,

	// Other
	{
		Table:      "location",
		Field:      "city",
		IssueCode:  "g2-011",
		Prevalence: "low",
	}: LowRank,

	{
		Table:      "location",
		Field:      "county",
		IssueCode:  "g4-002",
		Prevalence: "high",
	}: MediumRank,
}

var ruleNonMatchTests = []Result{
	// Other table
	{
		Table:      "person",
		Field:      "person_id",
		IssueCode:  "g2-013",
		Prevalence: "high",
	},

	// Other code
	{
		Table:      "location",
		Field:      "location_id",
		IssueCode:  "g2-999",
		Prevalence: "high",
	},
}

func TestRuleMatches(t *testing.T) {
	for res, erank := range ruleMatchTests {
		_, arank, ok := RunRules(&res)

		if !ok {
			t.Errorf("expected to match: %s", res)
		} else if arank != erank {
			t.Errorf("expected %s, but matched %s", erank, arank)
		}
	}
}

func TestRuleNonMatches(t *testing.T) {
	for _, res := range ruleNonMatchTests {
		_, _, ok := RunRules(&res)

		if ok {
			t.Errorf("did not expect to match: %s", res)
		}
	}
}
