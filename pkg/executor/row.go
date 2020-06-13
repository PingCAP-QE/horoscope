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

import "database/sql"

type (
	Row  []string
	Rows []Row
)

func NewRows(rows *sql.Rows) (ret Rows, err error) {
	ret = make(Rows, 0)
	columns, err := rows.Columns()
	if err != nil {
		return
	}
	for rows.Next() {
		dataSet := make([]interface{}, 0, len(columns))
		row := make(Row, 0, len(columns))
		for range columns {
			dataSet = append(dataSet, new(string))
		}
		err = rows.Scan(dataSet...)
		if err != nil {
			return
		}

		for _, data := range dataSet {
			row = append(row, *data.(*string))
		}
		ret = append(ret, row)
	}
	return
}

func (r Row) Equal(other Row) bool {
	if len(r) != len(other) {
		return false
	}
	for i, column := range r {
		if column != other[i] {
			return false
		}
	}
	return true
}

func (r Rows) Equal(other Rows) bool {
	if len(r) != len(other) {
		return false
	}
	for i, column := range r {
		if !column.Equal(other[i]) {
			return false
		}
	}
	return true
}
