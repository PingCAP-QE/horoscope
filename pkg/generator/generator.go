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
	"math"
	"math/rand"
	"strings"

	"github.com/chaos-mesh/horoscope/pkg/database"
	"github.com/chaos-mesh/horoscope/pkg/executor"
)

type (
	Generator struct {
		db   *database.Database
		exec executor.Executor
	}

	Options struct {
		MaxTables int
		Limit     int
	}
)

func NewGenerator(db *database.Database, exec executor.Executor) *Generator {
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
						expr += fmt.Sprintf(" %s ", RdLogicOp())
					}
					expr += RdExpr(g.exec, column, values[j])
				}
				exprGroup = append(exprGroup, expr)
			}
		}
		selectStmt += fmt.Sprintf(" WHERE (%s)", strings.Join(exprGroup, ") AND ("))
	}

	// control the max count of order by clause
	orderBy := g.RdOrderBy(columnsList, 2)

	if len(orderBy) != 0 {
		selectStmt += fmt.Sprintf(" ORDER BY %s", strings.Join(orderBy, ", "))
	}
	if options.Limit != 0 {
		selectStmt += fmt.Sprintf(" LIMIT %d", options.Limit)
	}

	return selectStmt, nil
}

func (g *Generator) RdTablesAndColumns(maxTables int) ([]string, [][]*database.Column) {
	tableNums := Rd(maxTables) + 1
	columnsList := make([][]*database.Column, 0)
	tables := make([]string, 0, tableNums)
	for tableName, table := range g.db.BaseTables {
		columns := make([]*database.Column, 0)
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

func (g *Generator) RdOrderBy(tableColumns [][]*database.Column, count uint) []string {
	var cols []string
	for _, columns := range tableColumns {
		for _, column := range columns {
			cols = append(cols, column.String())
		}
	}
	rand.Shuffle(len(cols), func(i, j int) {
		cols[i], cols[j] = cols[j], cols[i]
	})
	min := int(math.Min(float64(count), float64(len(cols)))) + 1
	elemLen := Rd(min)
	return cols[:elemLen]
}

func (g *Generator) RdValues(table string, columns []*database.Column) (values [][]byte, err error) {
	columnNames := make([]string, 0, len(columns))
	values = make([][]byte, 0, len(columns))
	for _, column := range columns {
		columnNames = append(columnNames, column.Name.String())
	}
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", strings.Join(columnNames, ","), table)
	rows, err := g.exec.Query(query)
	if err != nil {
		return
	}
	for _, value := range rows.Data[0] {
		values = append(values, value)
	}
	return
}
