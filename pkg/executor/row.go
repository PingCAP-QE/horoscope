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
	Row [][]byte

	Rows struct {
		ColumnMap map[string]int
		Columns   Row
		Data      []Row
	}

	RowStream struct {
		ColumnsMap map[string]int
		Columns    Row
		rawStream  *sql.Rows
	}
)

func NewRowStream(rows *sql.Rows) (ret RowStream, err error) {
	ret.rawStream = rows
	ret.ColumnsMap = make(map[string]int)
	columns, err := rows.Columns()
	if err != nil {
		return
	}
	for i, column := range columns {
		ret.Columns = append(ret.Columns, []byte(column))
		ret.ColumnsMap[column] = i
	}
	return
}

func (s *RowStream) Next() (row Row, err error) {
	if !s.rawStream.Next() {
		return
	}

	dataSet := make([]interface{}, 0, len(s.Columns))
	row = make(Row, 0, len(s.Columns))

	for range s.Columns {
		dataSet = append(dataSet, &[]byte{})
	}

	err = s.rawStream.Scan(dataSet...)
	if err != nil {
		return
	}

	for _, data := range dataSet {
		row = append(row, *data.(*[]byte))
	}
	return
}

func NewRows(rows *sql.Rows) (ret Rows, err error) {
	data := make([]Row, 0)
	stream, err := NewRowStream(rows)
	if err != nil {
		return
	}

	for {
		var row Row
		row, err = stream.Next()
		if err != nil {
			return
		}

		if row == nil {
			ret = Rows{Data: data, Columns: stream.Columns, ColumnMap: stream.ColumnsMap}
			return
		}

		data = append(data, row)
	}
}

func (r Rows) RowCount() int {
	return len(r.Data)
}

func (r Rows) ColumnNums() int {
	return len(r.Columns)
}

func (r Row) Equal(other Row) bool {
	if len(r) != len(other) {
		return false
	}
	for i, column := range r {
		if len(column) != len(other[i]) || string(column) != string(other[i]) {
			return false
		}
	}
	return true
}

func (r Row) ToTableRow() table.Row {
	tableRow := make(table.Row, 0, len(r))
	for _, column := range r {
		tableRow = append(tableRow, string(column))
	}
	return tableRow
}

func (r Rows) Equal(other Comparable) bool {
	otherRows, ok := other.(Rows)

	if !ok {
		return false
	}

	if r.RowCount() != otherRows.RowCount() || r.ColumnNums() != otherRows.ColumnNums() {
		return false
	}

	for i, column := range r.Columns {
		if len(column) != len(otherRows.Columns[i]) && string(column) != string(otherRows.Columns[i]) {
			return false
		}
	}
	for i, row := range r.Data {
		if !row.Equal(otherRows.Data[i]) {
			return false
		}
	}
	return true
}

func (r Rows) String() string {
	t := table.NewWriter()
	t.AppendHeader(r.Columns.ToTableRow())
	for _, row := range r.Data {
		t.AppendRow(row.ToTableRow())
	}
	return t.Render()
}
