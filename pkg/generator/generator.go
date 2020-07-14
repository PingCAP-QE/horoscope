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
	"github.com/chaos-mesh/horoscope/pkg/executor"
)

type (
	Generator struct {
		db   *types.Database
		exec executor.Executor
	}

	Options struct {
		MaxTables int
		Limit     int
	}
)

func NewGenerator(db *types.Database, exec executor.Executor) *Generator {
	return &Generator{
		db:   db,
		exec: exec,
	}
}

func (g *Generator) SelectStmt(options Options) (string, error) {
	tables, columns := g.RdTablesAndColumns(options.MaxTables)
	selectStmt := fmt.Sprintf("SELECT * FROM %s", strings.Join(tables, ","))
	if len(columns) > 0 {
		whereExpr := ""
		for _, column := range columns {
			if whereExpr != "" {
				whereExpr += fmt.Sprintf(" %s ", RdLogicOp())
			}
			value, err := g.RdValue(column)
			if err != nil {
				return "", err
			}
			whereExpr += fmt.Sprintf("%s %s %s", column.String(), RdComparisionOp(), value)
		}
		selectStmt += fmt.Sprintf(" WHERE %s", whereExpr)
	}

	if options.Limit != 0 {
		selectStmt += fmt.Sprintf(" LIMIT %d", options.Limit)
	}

	return selectStmt, nil
}

func (g *Generator) RdTablesAndColumns(maxTables int) ([]string, []*types.Column) {
	tableNums := Rd(maxTables) + 1
	columns := make([]*types.Column, 0)
	tables := make([]string, 0, tableNums)
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

func (g *Generator) RdValue(column *types.Column) (value string, err error) {
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", column.Name.String(), column.Table.Name.String())
	rows, err := g.exec.Query(query)
	if err != nil {
		return
	}
	value = FormatValue(column.Type, rows.Data[0][0])
	return
}
