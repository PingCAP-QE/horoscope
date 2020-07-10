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

	"github.com/chaos-mesh/horoscope/pkg/executor"
)

// Database defines database database
type Database struct {
	Name       string
	BaseTables map[string]*Table
}

func LoadDatabase(rawName, tables executor.Rows) (db *Database, err error) {
	if rawName.ColumnNums() != 1 || rawName.RowCount() != 1 {
		err = errors.New(fmt.Sprintf("Invalid database\n%s", rawName.String()))
		return
	}
	if tables.ColumnNums() != 2 {
		err = errors.New(fmt.Sprintf("Invalid tables\n%s", tables.String()))
		return
	}
	db = &Database{
		Name:       rawName.Data[0][0],
		BaseTables: make(map[string]*Table),
	}
	for _, row := range tables.Data {
		db.BaseTables[row[0]] = PrepareTable(row[0])
	}
	return
}
