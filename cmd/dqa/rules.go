package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"

	dms "github.com/chop-dbhi/data-models-service/client"
)

var (
	dataQualityContentsURI = "https://api.github.com/repos/PEDSnet/Data-Quality/contents/%s"

	ruleSetFiles = map[string]string{
		"Admin":       "SecondaryReports/Ranking/RuleSet1_Admin.csv",
		"Demographic": "SecondaryReports/Ranking/RuleSet2_Demographic.csv",
		"Fact":        "SecondaryReports/Ranking/RuleSet3_Fact.csv",
	}
)

func fetchRules(name, path string, token string, model *dms.Model) (*RuleSet, error) {
	url := fmt.Sprintf(dataQualityContentsURI, path)
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/vnd.github.v3.raw")
	req.Header.Set("Authorization", "token "+token)

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	p, err := NewRulesParser(resp.Body, model)

	if err != nil {
		return nil, err
	}

	rules, err := p.ParseAll()

	if err != nil {
		return nil, err
	}

	return &RuleSet{
		Name:   name,
		Parser: p,
		Rules:  rules,
	}, nil
}

func FetchRules(token string, model *dms.Model) ([]*RuleSet, error) {
	size := len(ruleSetFiles)

	sets := make([]*RuleSet, size)
	errs := make([]error, size)

	wg := sync.WaitGroup{}
	wg.Add(size)

	i := 0

	for n, p := range ruleSetFiles {
		go func(index int, name, path string) {
			if rs, err := fetchRules(name, path, token, model); err != nil {
				errs[index] = err
			} else {
				sets[index] = rs
			}

			wg.Done()
		}(i, n, p)

		i++
	}

	wg.Wait()

	if errs != nil {
		for _, err := range errs {
			if err != nil {
				return nil, err
			}
		}
	}

	return sets, nil
}

// RunRules iterates through all rules for the result until a match is found.
func RunRules(sets []*RuleSet, r *Result) (*RuleSet, Rank, bool) {
	var (
		rs    *RuleSet
		rule  *Rule
		match bool
		rank  Rank
	)

	for _, rs = range sets {
		for _, rule = range rs.Rules {
			if rank, match = rule.Matches(r); match {
				return rs, rank, true
			}
		}
	}

	return nil, 0, false
}

func inSlice(s string, a []string) bool {
	for _, x := range a {
		if x == s {
			return true
		}
	}

	return false
}

type Matcher interface {
	Matches(r *Result) (Rank, bool)
}

type Condition func(r *Result) bool

// Rule defines a mapping from a table, field condition, issue code, and
// prevalence to a specific rank.
type Rule struct {
	Table      string
	Condition  Condition
	IssueCode  string
	Prevalence string
	Rank       Rank
}

// Matches takes a result and determines if the result matches the rule.
func (r *Rule) Matches(s *Result) (Rank, bool) {
	if strings.ToLower(s.Table) != r.Table {
		return 0, false
	}

	if !r.Condition(s) {
		return 0, false
	}

	if strings.ToLower(s.IssueCode) != r.IssueCode {
		return 0, false
	}

	if strings.ToLower(s.Prevalence) != r.Prevalence {
		return 0, false
	}

	return r.Rank, true
}

// Field conditionals.
func isPersistent(r *Result) bool {
	return strings.ToLower(r.Status) == "persistent"
}

func isPrimaryKey(r *Result) bool {
	return r.Field == fmt.Sprintf("%s_id", r.Table)
}

func isSourceValue(r *Result) bool {
	return strings.HasSuffix(r.Field, "_source_value")
}

func isConceptId(r *Result) bool {
	return strings.HasSuffix(r.Field, "_concept_id")
}

func isForeignKey(r *Result) bool {
	return !isPrimaryKey(r) && strings.HasSuffix(r.Field, "_id") && !isConceptId(r)
}

func isDateYear(r *Result) bool {
	return strings.Contains(r.Field, "date") || strings.Contains(r.Field, "year")
}

func isOther(r *Result) bool {
	return !isPrimaryKey(r) && !isForeignKey(r) && !isSourceValue(r) && !isConceptId(r) && !isDateYear(r)
}

type RuleSet struct {
	Name   string
	Parser *RulesParser
	Rules  []*Rule
}

func (rs *RuleSet) String() string {
	return rs.Name
}

func (rs *RuleSet) Matches(r *Result) (Rank, bool) {
	// Global rule.
	if isPersistent(r) {
		return 0, false
	}

	for _, rule := range rs.Rules {
		if rank, ok := rule.Matches(r); ok {
			return rank, true
		}
	}

	return 0, false
}

// Header of a valid rules file.
var (
	// Matches the contents of `in (string1, string2, ...)`
	inStmtRe = regexp.MustCompile(`^in\s*\(([^\)]+)\)$`)

	// Matches a standard identifier, including field and table names
	// and prevalence. This is used to validate the value.
	identRe = regexp.MustCompile(`^(?i:[a-z0-9_]+)$`)

	rulesHeader = []string{
		"Table",
		"Field",
		"Issue Code",
		"Prevalence",
		"Rank",
	}

	ruleFieldTypes = []string{
		"is primary key",
		"is source value",
		"is date/year",
		"is concept id",
		"is other",
	}
)

type RuleParseError struct {
	line int
	err  error
}

func (e *RuleParseError) Error() string {
	return fmt.Sprintf("line %d: %s", e.line, e.err)
}

func NewRuleParseError(line int, err error) error {
	return &RuleParseError{line, err}
}

type RulesParser struct {
	model *dms.Model
	cr    *csv.Reader
	line  int
	// Set of validation errors found as rules are parsed.
	verrs []error
}

func (*RulesParser) isIdent(v string) bool {
	return identRe.MatchString(v)
}

func (p *RulesParser) parseInSet(v string) ([]string, error) {
	var l []string
	m := inStmtRe.FindAllStringSubmatch(v, 1)

	if len(m) > 0 {
		// Split tokens in submatch and trim the space.
		l = strings.Split(m[0][1], ",")
	} else {
		l = []string{v}
	}

	for i, x := range l {
		x = strings.TrimSpace(x)

		if !p.isIdent(x) {
			return nil, fmt.Errorf("'%s' is not a valid identifier", x)
		}

		l[i] = x
	}

	return l, nil
}

func (p *RulesParser) parseTable(v string) ([]string, error) {
	tables, err := p.parseInSet(v)
	if err != nil {
		return nil, err
	}

	// Validate tables.
	for _, t := range tables {
		if tbl := p.model.Tables.Get(t); tbl == nil {
			err = NewRuleParseError(p.line, fmt.Errorf("table '%s' is not defined", t))
			p.verrs = append(p.verrs, err)
		}
	}

	return tables, nil
}

func (p *RulesParser) parseField(v string, tables []string) (Condition, error) {
	// Check for type.
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "is primary key":
		return isPrimaryKey, nil

	case "is source value":
		return isSourceValue, nil

	case "is date/year":
		return isDateYear, nil

	case "is foreign key":
		return isForeignKey, nil

	case "is concept id":
		return isConceptId, nil

	case "is other":
		return isOther, nil
	}

	// Assume in(..) or single value.
	fields, err := p.parseInSet(v)

	if err != nil {
		return nil, err
	}

	// Validate all fields are defined in all tables for this rule.
	for _, f := range fields {
		for _, t := range tables {
			// Tables already validated.
			tbl := p.model.Tables.Get(t)

			if fld := tbl.Fields.Get(f); fld == nil {
				err := NewRuleParseError(p.line, fmt.Errorf("field '%s' is not defined for table '%s'", f, t))
				p.verrs = append(p.verrs, err)
			}
		}
	}

	return func(r *Result) bool {
		return inSlice(r.Field, fields)
	}, nil
}

func (*RulesParser) parseIssueCode(v string) (string, error) {
	return strings.ToLower(v), nil
}

func (p *RulesParser) parsePrevalence(v string) ([]string, error) {
	if v == "-" {
		return []string{"unknown"}, nil
	}

	if v == "in (*)" {
		return Prevalences, nil
	}

	return p.parseInSet(v)
}

func (p *RulesParser) parseRank(v string) (Rank, error) {
	switch strings.ToLower(v) {
	case "high":
		return HighRank, nil

	case "medium":
		return MediumRank, nil

	case "low":
		return LowRank, nil
	}

	err := NewRuleParseError(p.line, fmt.Errorf("'%s' is not a valid rank", v))
	p.verrs = append(p.verrs, err)

	return 0, nil
}

func (p *RulesParser) Parse() ([]*Rule, error) {
	row, err := p.cr.Read()

	if err == io.EOF {
		return nil, io.EOF
	}

	if err != nil {
		return nil, err
	}

	p.line++

	var (
		tables      []string
		condition   Condition
		issueCode   string
		prevalences []string
		rank        Rank
	)

	if tables, err = p.parseTable(row[0]); err != nil {
		return nil, NewRuleParseError(p.line, err)
	}

	if condition, err = p.parseField(row[1], tables); err != nil {
		return nil, NewRuleParseError(p.line, err)
	}

	if issueCode, err = p.parseIssueCode(row[2]); err != nil {
		return nil, NewRuleParseError(p.line, err)
	}

	if prevalences, err = p.parsePrevalence(row[3]); err != nil {
		return nil, NewRuleParseError(p.line, err)
	}

	if rank, err = p.parseRank(row[4]); err != nil {
		return nil, NewRuleParseError(p.line, err)
	}

	var rules []*Rule

	for _, t := range tables {
		for _, p := range prevalences {
			rules = append(rules, &Rule{
				Table:      t,
				Condition:  condition,
				Prevalence: p,
				IssueCode:  issueCode,
				Rank:       rank,
			})
		}
	}

	return rules, nil
}

func (p *RulesParser) ParseAll() ([]*Rule, error) {
	var (
		err         error
		line, rules []*Rule
	)

	for {
		line, err = p.Parse()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		rules = append(rules, line...)
	}

	return rules, nil
}

func (p *RulesParser) ValidationErrors() []error {
	if len(p.verrs) > 0 {
		return p.verrs
	}

	return nil
}

func NewRulesParser(r io.Reader, m *dms.Model) (*RulesParser, error) {
	cr := csv.NewReader(&UniversalReader{r})

	cr.FieldsPerRecord = len(rulesHeader)
	cr.TrimLeadingSpace = true
	cr.Comment = '#'
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true

	_, err := cr.Read()

	if err != nil {
		return nil, NewRuleParseError(1, fmt.Errorf("invalid header: %s", err))
	}

	return &RulesParser{
		model: m,
		line:  1,
		cr:    cr,
	}, nil
}
