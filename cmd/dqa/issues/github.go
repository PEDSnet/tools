package issues

import (
	"bytes"
	"encoding/csv"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/PEDSnet/tools/cmd/dqa/uni"
	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

const (
	owner = "PEDSnet"

	catalogRepo = "Data-Quality-Analysis"
	catalogPath = "DQA_Catalog/"

	conflictRepo             = "Data-Quality-Results"
	conflictAssociationsPath = "SecondaryReports/ConflictResolution/conflict_associations.csv"
)

var checkCodeRe = regexp.MustCompile(`^([A-C][A-C]-\d{3})_`)

type Threshold struct {
	Lower int
	Upper int
}

type Catalog map[string]map[[2]string]*Threshold

// NewGitHubReport initializes a new report for posting to GitHub.
func GetCatalog(token string) (Catalog, error) {
	tk := &oauth2.Token{
		AccessToken: token,
	}

	context := oauth2.NoContext
	ts := oauth2.StaticTokenSource(tk)
	tc := oauth2.NewClient(context, ts)

	client := github.NewClient(tc)

	// Get conflict associations
	fileContent, _, _, err := client.Repositories.GetContents(context, owner, conflictRepo, conflictAssociationsPath, nil)
	if err != nil {
		return nil, err
	}
	content, err := fileContent.GetContent()
	if err != nil {
		return nil, err
	}

	buf := uni.New(bytes.NewBufferString(content))
	cr := csv.NewReader(buf)
	if _, err := cr.Read(); err != nil {
		return nil, err
	}

	checkIssuesCodes := make(map[string]string)

	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		checkIssuesCodes[row[1]] = row[0]
	}

	// Fetch thresholds from conflict check mappings.
	_, dirContent, _, err := client.Repositories.GetContents(context, owner, catalogRepo, catalogPath, nil)
	if err != nil {
		return nil, err
	}

	catalog := make(Catalog)

	for _, file := range dirContent {
		if *file.Type == "dir" {
			continue
		}

		if !checkCodeRe.MatchString(*file.Name) {
			continue
		}

		// Only process codes that are relevant.
		checkCode := checkCodeRe.FindStringSubmatch(*file.Name)[1]

		var targetCode string
		if _, ok := checkIssuesCodes[checkCode]; ok {
			targetCode = checkIssuesCodes[checkCode]
		} else {
			targetCode = checkCode
		}

		// Fetch to get contents.
		file, _, _, err = client.Repositories.GetContents(context, owner, catalogRepo, *file.Path, nil)
		if err != nil {
			return nil, err
		}
		content, err := file.GetContent()
		if err != nil {
			return nil, err
		}

		buf := uni.New(bytes.NewBufferString(content))
		cr := csv.NewReader(buf)
		if _, err := cr.Read(); err != nil {
			continue
		}

		if _, ok := catalog[targetCode]; !ok {
			catalog[targetCode] = make(map[[2]string]*Threshold)
		}

		for {
			row, err := cr.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}

			lower, err := strconv.Atoi(row[3])
			if err != nil {
				lower = 0
			}
			upper, err := strconv.Atoi(row[4])
			if err != nil {
				upper = 0
			}

			table := strings.ToLower(row[1])
			field := strings.ToLower(row[2])

			catalog[targetCode][[2]string{table, field}] = &Threshold{
				Lower: lower,
				Upper: upper,
			}
		}
	}

	return catalog, nil
}
