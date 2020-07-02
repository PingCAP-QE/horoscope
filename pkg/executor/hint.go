package executor

import (
	"reflect"
	"strings"
)

type Hints map[string]bool

func NewHints(raw string) Hints {
	hints := make(Hints)
	for _, hint := range strings.Split(raw, ",") {
		hints[strings.Trim(hint, " ")] = true
	}
	return hints
}

func (h Hints) Equal(other Hints) bool {
	return reflect.DeepEqual(h, other)
}

func (h Hints) RemoveNTHPlan() {
	for hint := range h {
		if strings.Contains(hint, "nth_plan") {
			delete(h, hint)
		}
	}
}
