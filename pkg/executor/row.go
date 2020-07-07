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
	"database/sql"

	"github.com/jedib0t/go-pretty/table"
)

type (
	Row  []string
	Rows struct {
		columns Row
		data    []Row
	}
)

func NewRows(rows *sql.Rows) (ret Rows, err error) {
	data := make([]Row, 0)
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
		data = append(data, row)
	}
	ret = Rows{data: data, columns: columns}
	return
}

func (r Rows) Rows() int {
	return len(r.data)
}

func (r Rows) Columns() int {
	return len(r.columns)
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

func (r Row) ToTableRow() table.Row {
	tableRow := make(table.Row, 0, len(r))
	for _, column := range r {
		tableRow = append(tableRow, column)
	}
	return tableRow
}

func (r Rows) Equal(other Comparable) bool {
	otherRows, ok := other.(Rows)

	if !ok {
		return false
	}

	if r.Rows() != otherRows.Rows() || r.Columns() != otherRows.Columns() {
		return false
	}

	for i, column := range r.columns {
		if column != otherRows.columns[i] {
			return false
		}
	}
	for i, row := range r.data {
		if !row.Equal(otherRows.data[i]) {
			return false
		}
	}
	return true
}

func (r Rows) String() string {
	t := table.NewWriter()
	t.AppendHeader(r.columns.ToTableRow())
	for _, row := range r.data {
		t.AppendRow(row.ToTableRow())
	}
	return t.Render()
}
