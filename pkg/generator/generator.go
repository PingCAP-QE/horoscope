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
		MaxTables          int
		Limit              int
		DisableRandLogicOp bool
	}
)

func NewGenerator(db *types.Database, exec executor.Executor) *Generator {
	return &Generator{
		db:   db,
		exec: exec,
	}
}

func (g *Generator) SelectStmt(options Options) (string, error) {
	tables, columnsList := g.RdTablesAndColumns(options.MaxTables)
	selectStmt := fmt.Sprintf("SELECT * FROM %s", strings.Join(tables, ","))
	if len(columnsList) > 0 {
		exprGroup := make([]string, 0)
		for i, columns := range columnsList {
			if len(columns) > 0 {
				values, err := g.RdValues(tables[i], columns)
				if err != nil {
					return "", err
				}
				expr := ""
				for j, column := range columns {
					if expr != "" {
						logicOp := "AND"
						if !options.DisableRandLogicOp {
							logicOp = RdLogicOp()
						}
						expr += fmt.Sprintf(" %s ", logicOp)
					}
					expr += fmt.Sprintf("(%s <=> %s OR %s %s %s)", column.String(), values[j], column.String(), RdComparisionOp(), values[j])
				}
				exprGroup = append(exprGroup, expr)
			}
		}
		selectStmt += fmt.Sprintf(" WHERE (%s)", strings.Join(exprGroup, ") AND ("))
	}

	if options.Limit != 0 {
		selectStmt += fmt.Sprintf(" LIMIT %d", options.Limit)
	}

	return selectStmt, nil
}

func (g *Generator) RdTablesAndColumns(maxTables int) ([]string, [][]*types.Column) {
	tableNums := Rd(maxTables) + 1
	columnsList := make([][]*types.Column, 0)
	tables := make([]string, 0, tableNums)
	for tableName, table := range g.db.BaseTables {
		columns := make([]*types.Column, 0)
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
		columnsList = append(columnsList, columns)
	}
	return tables, columnsList
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

func (g *Generator) RdValues(table string, columns []*types.Column) (values []string, err error) {
	columnNames := make([]string, 0, len(columns))
	values = make([]string, 0, len(columns))
	for _, column := range columns {
		columnNames = append(columnNames, column.Name.String())
	}
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", strings.Join(columnNames, ","), table)
	rows, err := g.exec.Query(query)
	if err != nil {
		return
	}
	for i, value := range rows.Data[0] {
		values = append(values, FormatValue(columns[i].Type, value))
	}
	return
}
