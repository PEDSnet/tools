package rules

import (
	"fmt"
	"strings"

	"github.com/PEDSnet/tools/cmd/dqa/results"
)

type Condition struct {
	Name string
	Test func(r *results.Result) bool
}

// Field conditionals.
var isPersistent = &Condition{
	Test: func(r *results.Result) bool {
		return strings.ToLower(r.Status) == "persistent"
	},
}

var isPrimaryKey = &Condition{
	Test: func(r *results.Result) bool {
		return r.Field == fmt.Sprintf("%s_id", r.Table)
	},
}

var isSourceValue = &Condition{
	Test: func(r *results.Result) bool {
		return strings.HasSuffix(r.Field, "_source_value")
	},
}

var isConceptId = &Condition{
	Test: func(r *results.Result) bool {
		return strings.HasSuffix(r.Field, "_concept_id")
	},
}

var isForeignKey = &Condition{
	Test: func(r *results.Result) bool {
		return !isPrimaryKey.Test(r) && strings.HasSuffix(r.Field, "_id") && !isConceptId.Test(r)
	},
}

var isDateYear = &Condition{
	Test: func(r *results.Result) bool {
		return strings.Contains(r.Field, "date") || strings.Contains(r.Field, "year")
	},
}

var isDateYearTime = &Condition{
	Test: func(r *results.Result) bool {
		return strings.HasSuffix(r.Field, "_date") || strings.HasSuffix(r.Field, "_year") || strings.HasSuffix(r.Field, "_time")
	},
}

var isOther = &Condition{
	Test: func(r *results.Result) bool {
		return !isPrimaryKey.Test(r) && !isForeignKey.Test(r) && !isSourceValue.Test(r) && !isConceptId.Test(r) && !isDateYear.Test(r)
	},
}
