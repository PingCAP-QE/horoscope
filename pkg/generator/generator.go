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

package generator

import (
	"fmt"
	"strings"

	"github.com/chaos-mesh/horoscope/pkg/database-types"
)

type Generator struct {
	db *types.Database
}

func (g *Generator) SelectStmt() string {
	tables, columns := g.RdTablesAndColumns()
	selectStmt := fmt.Sprintf("SELECT * FROM %s", strings.Join(tables, ","))
	if len(columns) > 0 {
		whereExpr := ""
		for _, column := range columns {
			if whereExpr != "" {
				whereExpr += fmt.Sprintf(" %s ", RdLogicOp())
			}
			whereExpr += fmt.Sprintf("%s %s %s", column.String(), RdComparisionOp(), RdSQLValue(column.Type))
		}
		selectStmt += fmt.Sprintf("WHERE %s", whereExpr)
	}
	return selectStmt
}

func (g *Generator) RdTablesAndColumns() ([]string, []*types.Column) {
	tableNums := Rd(len(g.db.BaseTables) + 1)
	columns := make([]*types.Column, 0)
	tables := make([]string, tableNums)
	for tableName, table := range g.db.BaseTables {
		tableNums--
		if tableNums < 0 {
			break
		}
		tables = append(tables, tableName)
		for _, column := range table.Columns {
			if column.Key != "" {
				columns = append(columns, column)
			}
		}
	}
	return tables, columns
}
