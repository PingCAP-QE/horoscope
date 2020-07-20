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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/chaos-mesh/horoscope/pkg/executor"
)

var (
	database = executor.Rows{
		Columns: executor.NullableRow{"DATABASE()"},
		Data:    []executor.NullableRow{{"test"}},
	}

	tables = executor.Rows{
		Columns: executor.NullableRow{"tables_in_test", "table_type"},
		Data: []executor.NullableRow{
			{"customer", "BASE TABLE"},
			{"lineitem", "BASE TABLE"},
			{"nation", "BASE TABLE"},
			{"orders", "BASE TABLE"},
			{"part", "BASE TABLE"},
			{"partsupp", "BASE TABLE"},
			{"region", "BASE TABLE"},
			{"supplier", "BASE TABLE"},
		},
	}
	lineitem = executor.Rows{
		Columns: executor.NullableRow{"Field", "Type", "Null", "Key", "Default", "Extra"},
		Data: []executor.NullableRow{
			{"L_ORDERKEY", "bigint(20)", "NO", "PRI", "NULL", ""},
			{"L_PARTKEY", "bigint(20)", "NO", "", "NULL", ""},
			{"L_SUPPKEY", "bigint(20)", "NO", "MUL", "NULL", ""},
			{"L_LINENUMBER", "bigint(20)", "NO", "PRI", "NULL", ""},
			{"L_QUANTITY", "decimal(15,2)", "NO", "", "NULL", ""},
			{"L_SHIPDATE", "date", "NO", "", "NULL", ""},
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
