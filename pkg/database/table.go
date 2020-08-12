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
	"errors"
	"fmt"

	"github.com/pingcap/parser/model"

	"github.com/chaos-mesh/horoscope/pkg/executor"
)

var descriptionColumns = executor.Row{[]byte("Field"), []byte("Type"), []byte("Null"), []byte("Key"), []byte("Default"), []byte("Extra")}

// Table defines database table
type Table struct {
	DBName     model.CIStr
	Name       model.CIStr
	PrimaryKey *Column
	Columns    []*Column
	ColumnsMap map[string]*Column
}

func PrepareTable(dbName, name string) *Table {
	return &Table{
		DBName:     model.NewCIStr(dbName),
		Name:       model.NewCIStr(name),
		Columns:    make([]*Column, 0),
		ColumnsMap: make(map[string]*Column),
	}
}

func (t *Table) LoadColumns(data executor.Rows) error {
	if !data.Columns.Equal(descriptionColumns) {
		return errors.New(fmt.Sprintf("Invalid columns\n%s", data.String()))
	}
	for _, row := range data.Data {
		column, err := LoadColumn(t, row)
		if err != nil {
			return err
		}

		t.Columns = append(t.Columns, column)
		t.ColumnsMap[column.Name.String()] = column
		if column.Key == "PRI" {
			t.PrimaryKey = column
		}
	}
	return nil
}

func (t *Table) Keys() []*Column {
	keys := make([]*Column, 0)
	for _, column := range t.Columns {
		if column.Key != "" {
			keys = append(keys, column)
		}
	}
	return keys
}
