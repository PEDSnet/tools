// The scope of this module is to create an issue on GitHub for each
// issue found in a Secondary Report analysis.
package feedback

import (
	"bytes"
	"fmt"
	"strings"

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

// Fetch a DQA summary issue using labels.
func (gr *GithubReport) FetchSummaryIssue(ir *github.IssueRequest) (*github.Issue, error) {
	opts := &github.IssueListByRepoOptions{
		State:  "all",
		Labels: *ir.Labels,
	}

	issues, _, err := gr.client.Issues.ListByRepo(repoOwner, gr.Site, opts)
	if err != nil {
		return nil, err
	}

	if len(issues) == 1 {
		return &issues[0], nil
	}

	if len(issues) > 1 {
		// List of URLs to inspect.
		urls := make([]string, len(issues))

		for i, issue := range issues {
			urls[i] = fmt.Sprintf("- %s", issue.HTMLURL)
		}

		return nil, fmt.Errorf("Multiple issues match:\n%s", strings.Join(urls, "\n"))
	}

	return nil, nil
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

	title := fmt.Sprintf("DQA Summary: %s (%s) for PEDSnet CDM v%s", gr.DataCycle, gr.ETLVersion, res.ModelVersion)
	body := buf.String()
	labels := []string{
		"Data Quality",
		fmt.Sprintf("Data Cycle: %s", gr.DataCycle),
		"Data Quality Summary",
	}

	ir := github.IssueRequest{
		Title:  &title,
		Body:   &body,
		Labels: &labels,
	}

	return &ir, nil
}

func (gr *GithubReport) BuildIssue(r *results.Result) (*github.IssueRequest, error) {
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

// Ensure the minimum labels are set on the issue.
func (gr *GithubReport) EnsureLabels(num int, labels []string) ([]github.Label, error) {
	allLabels, _, err := gr.client.Issues.AddLabelsToIssue(repoOwner, gr.Site, num, labels)

	if err != nil {
		return nil, err
	}

	return allLabels, nil
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
