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

package database

import (
	"fmt"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/types"

	"github.com/chaos-mesh/horoscope/pkg/executor"
)

// Column defines database column
type Column struct {
	Table *Table
	Name  model.CIStr
	Type  *types.FieldType
	Null  bool
	Key   string
}

func (c Column) String() string {
	return fmt.Sprintf("%s.%s", c.Table.Name, c.Name)
}

func (c Column) FullType() string {
	return c.Type.String()
}

func LoadColumn(table *Table, row executor.Row) (column *Column, err error) {
	tp, err := NewParser().ParseFieldType(string(row[1]))
	if err != nil {
		return
	}

	column = &Column{
		Table: table,
		Name:  model.NewCIStr(string(row[0])),
		Type:  tp,
		Null:  ifNull(string(row[2])),
		Key:   string(row[3]),
	}
	return
}

func ifNull(null string) bool {
	return null == "YES"
}
