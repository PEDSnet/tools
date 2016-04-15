// The scope of this module is to create an issue on GitHub for each
// issue found in a Secondary Report analysis.
package feedback

import (
	"bytes"
	"fmt"

	"golang.org/x/oauth2"

	"github.com/PEDSnet/tools/cmd/dqa/results"
	"github.com/google/go-github/github"
)

const repoOwner = "PEDSnet"

type GithubReport struct {
	Site       string
	ETLVersion string
	DataCycle  string

	// Keep track of all results that were included in this report
	// for the summary.
	results results.Results

	client *github.Client
}

func (gr *GithubReport) Len() int {
	return len(gr.results)
}

func (gr *GithubReport) BuildSummaryIssue() (*github.IssueRequest, error) {
	f := &results.File{
		Results: gr.results,
	}

	r := results.NewMarkdownReport(f)
	buf := bytes.NewBuffer(nil)

	if err := r.Render(buf); err != nil {
		return nil, err
	}

	res := f.Results[0]

	title := fmt.Sprintf("DQA: %s (%s) for PEDSnet CDM v%s", gr.DataCycle, gr.ETLVersion, res.ModelVersion)
	body := buf.String()
	labels := []string{
		"Data Quality",
		fmt.Sprintf("Data Cycle: %s", gr.DataCycle),
	}

	ir := github.IssueRequest{
		Title:  &title,
		Body:   &body,
		Labels: &labels,
	}

	return &ir, nil
}

func (gr *GithubReport) NewIssue(r *results.Result) (*github.IssueRequest, error) {
	if r.SiteName() != gr.Site || r.ETLVersion() != gr.ETLVersion {
		return nil, fmt.Errorf("Result site or ETL version does not match reports")
	}

	title := fmt.Sprintf("DQA: %s (%s): %s/%s", gr.DataCycle, gr.ETLVersion, r.Table, r.Field)
	body := fmt.Sprintf("**Description**: %s\n**Finding**: %s", r.IssueDescription, r.Finding)

	labels := []string{
		"Data Quality",
		fmt.Sprintf("Data Cycle: %s", gr.DataCycle),
		fmt.Sprintf("Table: %s", r.Table),
	}

	if r.Rank > 0 {
		labels = append(labels, fmt.Sprintf("Rank: %s", r.Rank))
	}

	if r.Cause != "" {
		labels = append(labels, fmt.Sprintf("Cause: %s", r.Cause))
	}

	if r.Status != "" {
		labels = append(labels, fmt.Sprintf("Status: %s", r.Status))
	}

	// All fields are pointers.
	ir := github.IssueRequest{
		Title:  &title,
		Body:   &body,
		Labels: &labels,
	}

	gr.results = append(gr.results, r)

	return &ir, nil
}

// PostIssue sends a request to the GitHub API to create an issue.
// Upon success, a concrete issue is returned with the ID.
func (gr *GithubReport) PostIssue(ir *github.IssueRequest) (*github.Issue, error) {
	issue, _, err := gr.client.Issues.Create(repoOwner, gr.Site, ir)

	if err != nil {
		return nil, err
	}

	return issue, nil
}

// NewGitHubReport initializes a new report for posting to GitHub.
func NewGitHubReport(site, etl, cycle, token string) *GithubReport {
	tk := &oauth2.Token{
		AccessToken: token,
	}
	ts := oauth2.StaticTokenSource(tk)
	tc := oauth2.NewClient(oauth2.NoContext, ts)

	client := github.NewClient(tc)

	return &GithubReport{
		Site:       site,
		ETLVersion: etl,
		DataCycle:  cycle,
		client:     client,
	}
}
