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
	"math/rand"
	"strings"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/opcode"

	"github.com/chaos-mesh/horoscope/pkg/database"
	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/keymap"
	"github.com/chaos-mesh/horoscope/pkg/utils"
)

type (
	Generator struct {
		db         *database.Database
		exec       executor.Executor
		keyMatcher *keymap.KeyMatcher
	}

	Options struct {
		MaxTables            int
		MinDurationThreshold time.Duration
		Limit                int

		// control order by
		StableOrderBy     bool
		MaxOrderByColumns int

		EnableKeyMap bool
	}
)

func NewGenerator(db *database.Database, exec executor.Executor, keymaps []keymap.KeyMap) *Generator {
	g := &Generator{
		db:   db,
		exec: exec,
	}

	if keymaps != nil {
		g.keyMatcher = keymap.NewKeyMatcher(keymaps)
	}

	return g
}

func (g *Generator) SelectStmt(options Options) (string, error) {
	tables, columnsList := g.RdTablesAndKeys(options.MaxTables)
	selectStmt, err := g.PrepareSelect(tables, columnsList)
	if err != nil {
		return "", err
	}

	// control the max count of order by clause
	orderBy := g.RdOrderBy(options, tables, columnsList)

	if len(orderBy) != 0 {
		selectStmt.OrderBy = &ast.OrderByClause{Items: orderBy}
	}
	if options.Limit != 0 {
		selectStmt.Limit = &ast.Limit{Count: utils.NewValueExpr(options.Limit)}
	}

	return utils.BufferOut(selectStmt)
}

func (g *Generator) PrepareSelect(tables []string, columnsList [][]*database.Column) (*ast.SelectStmt, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("database `%s` is empty", g.db.Name)
	}

	tableRefs := g.TableRefsClause(tables)

	valuesList, err := g.RdValuesList(tableRefs, columnsList)
	if err != nil {
		return nil, err
	}

	stmt := &ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{
			Fields: []*ast.SelectField{
				{
					WildCard: &ast.WildCardField{},
				},
			},
		},
		From:  tableRefs,
		Where: g.WhereExpr(columnsList, valuesList),
	}

	return stmt, nil
}

func (g *Generator) TableRefsClause(tables []string) *ast.TableRefsClause {
	clause := &ast.TableRefsClause{
		TableRefs: &ast.Join{},
	}
	clause.TableRefs.Left = &ast.TableName{Name: model.NewCIStr(tables[0])}
	for _, table := range tables[1:] {
		clause.TableRefs.Right = &ast.TableName{Name: model.NewCIStr(table)}
		if g.keyMatcher != nil {
			if keyPair := g.keyMatcher.MatchRandom(tables[0], table); keyPair != nil {
				clause.TableRefs.On = &ast.OnCondition{
					Expr: &ast.BinaryOperationExpr{
						L:  &ast.ColumnNameExpr{Name: keyPair.K1.ColumnName()},
						R:  &ast.ColumnNameExpr{Name: keyPair.K2.ColumnName()},
						Op: opcode.EQ,
					},
				}
			}
		}
		clause.TableRefs = &ast.Join{Left: clause.TableRefs}
	}

	return clause
}

func (g *Generator) WhereExpr(columnsList [][]*database.Column, valuesList [][][]byte) ast.ExprNode {
	var whereExpr ast.ExprNode
	for i, columns := range columnsList {
		if len(columns) > 0 {
			values := valuesList[i]
			var expr ast.ExprNode
			for j, column := range columns {
				subExpr := RdExpr(g.exec, column, values[j])
				if expr == nil {
					expr = subExpr
				} else {
					expr = &ast.BinaryOperationExpr{
						L:  expr,
						R:  subExpr,
						Op: RdLogicOp(),
					}
				}
			}

			if whereExpr == nil {
				whereExpr = expr
			} else {
				whereExpr = &ast.BinaryOperationExpr{
					L:  whereExpr,
					R:  expr,
					Op: opcode.LogicAnd,
				}
			}
		}
	}
	return whereExpr
}

func (g *Generator) RdTablesAndKeys(maxTables int) ([]string, [][]*database.Column) {
	tableNums := Rd(maxTables) + 1
	keysList := make([][]*database.Column, 0, tableNums)
	tables := make([]string, 0, tableNums)

	var mainTable *database.Table

	for _, table := range g.db.BaseTables {
		mainTable = table
		break
	}

	if mainTable == nil {
		return tables, keysList
	}

	tables = append(tables, mainTable.Name.String())
	keysList = append(keysList, mainTable.Keys())

	for tableName, table := range g.db.BaseTables {
		if len(tables) >= tableNums {
			break
		}

		if g.keyMatcher != nil && len(g.keyMatcher.Match(mainTable.Name.String(), tableName)) == 0 {
			continue
		}
		if tableName == mainTable.Name.String() {
			continue
		}
		tables = append(tables, tableName)
		keysList = append(keysList, table.Keys())
	}
	return tables, keysList
}

func (g *Generator) RdOrderBy(options Options, tables []string, tableColumns [][]*database.Column) []*ast.ByItem {
	var cols []*ast.ByItem
	if options.StableOrderBy {
		allHavePK := true
		var allColumnFields []*ast.ByItem
		var pkColumnFields []*ast.ByItem
		for _, tableName := range tables {
			table := g.db.BaseTables[tableName]
			if table.PrimaryKey != nil {
				pkColumnFields = append(pkColumnFields, &ast.ByItem{Expr: &ast.ColumnNameExpr{Name: table.PrimaryKey.ColumnName()}})
			} else {
				allHavePK = false
			}
			for _, column := range table.Columns {
				allColumnFields = append(allColumnFields, &ast.ByItem{Expr: &ast.ColumnNameExpr{Name: column.ColumnName()}})
			}
		}
		if allHavePK {
			cols = pkColumnFields
		} else {
			cols = allColumnFields
		}
		return cols
	}

	for _, columns := range tableColumns {
		for _, column := range columns {
			cols = append(cols, &ast.ByItem{Expr: &ast.ColumnNameExpr{Name: column.ColumnName()}})
		}
	}
	rand.Shuffle(len(cols), func(i, j int) {
		cols[i], cols[j] = cols[j], cols[i]
	})

	elemLen := Rd(utils.MinInt(options.MaxOrderByColumns, len(cols)) + 1)
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

func (g *Generator) RdValuesList(tableRefs *ast.TableRefsClause, columnsList [][]*database.Column) (valuesList [][][]byte, err error) {
	valuesList = make([][][]byte, 0, len(columnsList))

	fields := &ast.FieldList{}

	for _, columns := range columnsList {
		for _, column := range columns {
			fields.Fields = append(fields.Fields, &ast.SelectField{
				Expr: &ast.ColumnNameExpr{
					Name: column.ColumnName(),
				},
			})
		}
	}

	stmt := ast.SelectStmt{
		Fields: fields,
		From:   tableRefs,
		OrderBy: &ast.OrderByClause{
			Items: []*ast.ByItem{
				{
					Expr: &ast.FuncCallExpr{
						FnName: model.NewCIStr("RAND"),
					},
				},
			},
		},
		Limit: &ast.Limit{
			Count: utils.NewValueExpr(1),
		},
	}

	query, err := utils.BufferOut(&stmt)
	if err != nil {
		return
	}

	rows, err := g.exec.Query(query)
	if err != nil {
		return
	}

	if rows.RowCount() == 0 {
		err = fmt.Errorf("query got empty set: %s", query)
	}

	values := rows.Data[0]

	for _, columns := range columnsList {
		newValues := make([][]byte, len(columns))
		copy(newValues, values)
		valuesList = append(valuesList, newValues)
		values = values[len(columns):]
	}

	return
}
