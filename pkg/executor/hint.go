// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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

func (h Hints) String() string {
	slice := make([]string, 0, len(h))
	for hint := range h {
		slice = append(slice, hint)
	}
	return strings.Join(slice, ", ")
}
