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
	"testing"

	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"

	"github.com/chaos-mesh/horoscope/pkg/executor"
)

var (
	database = executor.Rows{
		Columns: executor.Row{[]byte("DATABASE()")},
		Data:    []executor.Row{{[]byte("test")}},
	}

	tables = executor.Rows{
		Columns: executor.Row{[]byte("tables_in_test"), []byte("table_type")},
		Data: []executor.Row{
			{[]byte("customer"), []byte("BASE TABLE")},
			{[]byte("lineitem"), []byte("BASE TABLE")},
			{[]byte("nation"), []byte("BASE TABLE")},
			{[]byte("orders"), []byte("BASE TABLE")},
			{[]byte("part"), []byte("BASE TABLE")},
			{[]byte("partsupp"), []byte("BASE TABLE")},
			{[]byte("region"), []byte("BASE TABLE")},
			{[]byte("supplier"), []byte("BASE TABLE")},
		},
	}
	lineitem = executor.Rows{
		Columns: executor.Row{[]byte("Field"), []byte("Type"), []byte("Null"), []byte("Key"), []byte("Default"), []byte("Extra")},
		Data: []executor.Row{
			{[]byte("L_ORDERKEY"), []byte("bigint(20)"), []byte("NO"), []byte("PRI"), []byte("NULL"), []byte("")},
			{[]byte("L_PARTKEY"), []byte("bigint(20)"), []byte("NO"), []byte(""), []byte("NULL"), []byte("")},
			{[]byte("L_SUPPKEY"), []byte("bigint(20)"), []byte("NO"), []byte("MUL"), []byte("NULL"), []byte("")},
			{[]byte("L_LINENUMBER"), []byte("bigint(20)"), []byte("NO"), []byte("PRI"), []byte("NULL"), []byte("")},
			{[]byte("L_QUANTITY"), []byte("decimal(15,2)"), []byte("NO"), []byte(""), []byte("NULL"), []byte("")},
			{[]byte("L_SHIPDATE"), []byte("date"), []byte("NO"), []byte(""), []byte("NULL"), []byte("")},
		},
	}
)

func TestLoadDatabase(t *testing.T) {
	db, err := LoadDatabase(database, tables)
	assert.Nil(t, err)
	err = db.BaseTables["lineitem"].LoadColumns(lineitem)
	assert.Nil(t, err)

	assert.Equal(t, "test", db.Name)
	assert.Equal(t, 8, len(db.BaseTables))

	lineitem := db.BaseTables["lineitem"]

	assert.Equal(t, "lineitem", lineitem.Name.String())
	assert.Equal(t, 6, len(lineitem.Columns))
}
