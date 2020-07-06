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

type Hints struct {
	segments map[string]bool
	raw      string
}

func NewHints(raw string) Hints {
	hints := Hints{
		segments: make(map[string]bool),
		raw:      raw,
	}
	for _, hint := range strings.Split(raw, ",") {
		segment := strings.Trim(hint, " ")
		if !strings.Contains(segment, "nth_plan") {
			hints.segments[segment] = true
		}
	}
	return hints
}

func (h Hints) Equal(other Hints) bool {
	return reflect.DeepEqual(h.segments, other.segments)
}

func (h Hints) String() string {
	return h.raw
}
