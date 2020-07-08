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
		Columns: executor.Row{"DATABASE()"},
		Data:    []executor.Row{{"test"}},
	}

	tables = executor.Rows{
		Columns: executor.Row{"Tables_in_test"},
		Data: []executor.Row{
			{"customer"},
			{"lineitem"},
			{"nation"},
			{"orders"},
			{"part"},
			{"partsupp"},
			{"region"},
			{"supplier"},
		},
	}
	lineitem = executor.Rows{
		Columns: executor.Row{"Field", "Type", "Null", "Key", "Default", "Extra"},
		Data: []executor.Row{
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
	err = db.Tables["lineitem"].LoadColumns(lineitem)
	assert.Nil(t, err)

	assert.Equal(t, "test", db.Name)
	assert.Equal(t, 8, len(db.Tables))

	lineitem := db.Tables["lineitem"]

	assert.Equal(t, "lineitem", lineitem.Name.String())
	assert.Equal(t, 6, len(lineitem.Columns))
}
