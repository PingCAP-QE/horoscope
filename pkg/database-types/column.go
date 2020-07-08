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

package types

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/pingcap/parser/model"

	"github.com/chaos-mesh/horoscope/pkg/executor"
)

var (
	typePattern = regexp.MustCompile(`([a-z]*)\(?(.*)\)?`)
)

// Column defines database column
type Column struct {
	Table *Table
	Name  model.CIStr
	Type  string
	Args  string
	Null  bool
	Key   string
}

func (c Column) String() string {
	return fmt.Sprintf("%s.%s", c.Table.Name, c.Name)
}

func LoadColumn(table *Table, row executor.Row) (column *Column, err error) {
	tp, args, err := parseType(row[1])
	if err != nil {
		return
	}

	column = &Column{
		Table: table,
		Name:  model.NewCIStr(row[0]),
		Type:  tp,
		Args:  args,
		Null:  ifNull(row[2]),
		Key:   row[3],
	}
	return
}

func ifNull(null string) bool {
	return null == "YES"
}

// ParseType parse types and data length
func parseType(rawType string) (tp string, args string, err error) {
	matches := typePattern.FindStringSubmatch(rawType)
	if len(matches) != 3 {
		err = errors.New(fmt.Sprintf("Invalid column type: %s", rawType))
		return
	}
	tp, args = matches[1], matches[2]
	return
}
