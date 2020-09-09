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

var (
	ComposeTmpTable = model.NewCIStr("tmp")

	ComposeCountAsName = model.NewCIStr("val")

	ComposeSumExpr = &ast.AggregateFuncExpr{
		F: ast.AggFuncSum,
		Args: []ast.ExprNode{
			&ast.ColumnNameExpr{
				Name: &ast.ColumnName{
					Name:  ComposeCountAsName,
					Table: ComposeTmpTable,
				},
			},
		},
	}
)

type (
	Generator struct {
		db         *database.Database
		exec       executor.Executor
		keyMatcher *keymap.KeyMatcher
	}

	Options struct {
		MaxTables            int           `json:"max_tables"`
		MinDurationThreshold time.Duration `json:"min_duration_threshold"`
		Limit                int           `json:"limit"`
		KeyOnly              bool          `json:"key_only"`

		// control order by
		UnstableOrderBy bool `json:"unstable_order_by"`
		MaxByItems      int  `json:"max_by_items"`

		EnableKeyMap bool `json:"enable_key_map"`

		AggregateWeight float64 `json:"aggregate_weight"`
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

func (g *Generator) ComposeStmt(options Options) (query string, err error) {
	tables, columnsList := g.RdTablesAndKeys(&options)
	var stmt *ast.SelectStmt
	for i := 0; i < len(tables); i++ {
		start := i
		if i != len(tables)-1 {
			i += Rd(2)
		}
		// TODO: control random by options
		if RdBool() {
			stmt, err = g.ComposeSelect(options, tables[start:i+1], columnsList[start:], stmt)
		} else {
			stmt, err = g.ComposeUnion(options, tables[start:i+1], columnsList[start:], stmt)
		}
		if err != nil {
			return
		}
	}

	// TODO: control random by options
	if RdBool() {
		stmt.TableHints = []*ast.TableOptimizerHint{
			{
				HintName: model.NewCIStr("MERGE_JOIN"),
				Tables:   []ast.HintTable{{TableName: model.NewCIStr(tables[0])}},
			},
		}
	}

	return utils.BufferOut(stmt)
}

func (g *Generator) ComposeSelect(options Options, tables []string, columnsList [][]*database.Column, composedStmt *ast.SelectStmt) (*ast.SelectStmt, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("database `%s` is empty", g.db.Name)
	}

	tableRefs := g.TableRefsClause(tables)

	stmt := &ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{},
		From:   tableRefs,
	}

	if twoColumns := RdTowColumns(columnsList); twoColumns != nil {
		stmt.Where = &ast.BinaryOperationExpr{
			Op: opcode.NE,
			L:  &ast.ColumnNameExpr{Name: twoColumns[0].ColumnName()},
			R:  &ast.ColumnNameExpr{Name: twoColumns[1].ColumnName()},
		}
	}

	if composedStmt != nil {
		composedExpr := &ast.BinaryOperationExpr{
			Op: opcode.NE,
			L:  &ast.SubqueryExpr{Query: composedStmt},
			R:  utils.NewValueExpr(RdInt64()),
		}

		if stmt.Where == nil {
			stmt.Where = composedExpr
		} else {
			stmt.Where = &ast.BinaryOperationExpr{
				Op: opcode.LogicOr,
				L:  stmt.Where,
				R:  composedExpr,
			}
		}
	}

	composeCountExpr := &ast.AggregateFuncExpr{
		F:    ast.AggFuncCount,
		Args: []ast.ExprNode{utils.NewValueExpr(1)},
	}

	stmt.Fields.Fields = []*ast.SelectField{
		{
			AsName: ComposeCountAsName,
			Expr:   composeCountExpr,
		},
	}

	// TODO: control random by options
	if RdBool() {
		if rdColumn := RdColumns(columnsList); rdColumn != nil {
			byItems := []*ast.ByItem{{Expr: &ast.ColumnNameExpr{
				Name: rdColumn.ColumnName(),
			}}}
			stmt.GroupBy = &ast.GroupByClause{Items: byItems}
			stmt.OrderBy = &ast.OrderByClause{Items: byItems}
			stmt.IsInBraces = true
		}
	}

	// TODO: control random by options
	if RdBool() {
		if stmt.OrderBy == nil {
			stmt.OrderBy = &ast.OrderByClause{Items: make([]*ast.ByItem, 0)}
			stmt.IsInBraces = true
		}
		stmt.OrderBy.Items = append(stmt.OrderBy.Items, &ast.ByItem{Expr: &ast.ColumnNameExpr{
			Name: &ast.ColumnName{
				Name: ComposeCountAsName,
			},
		}})
	}

	// TODO: control random by options
	if RdBool() {
		stmt.Limit = &ast.Limit{Count: utils.NewValueExpr(Rd(10) + 1)}
		stmt.IsInBraces = true
	}

	return stmt, nil
}

func (g *Generator) ComposeUnion(options Options, tables []string, columnsList [][]*database.Column, composedStmt *ast.SelectStmt) (stmt *ast.SelectStmt, err error) {
	selectList := []*ast.SelectStmt{nil, nil}
	selectList[0], err = g.ComposeSelect(options, tables, columnsList, composedStmt)
	if err != nil {
		return
	}
	selectList[1], err = g.ComposeSelect(options, tables, columnsList, composedStmt)
	if err != nil {
		return
	}
	stmt = &ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{},
		From: &ast.TableRefsClause{TableRefs: &ast.Join{
			Left: &ast.TableSource{
				AsName: ComposeTmpTable,
				Source: &ast.UnionStmt{SelectList: &ast.UnionSelectList{Selects: selectList}},
			},
		}},
	}
	stmt.Fields.Fields = []*ast.SelectField{
		{
			Expr: ComposeSumExpr,
		},
	}
	return
}

func (g *Generator) BenchStmt(options Options) (query string, err error) {
	tables, columnsList := g.RdTablesAndKeys(&options)
	stmt, err := g.BenchSelect(options, tables, columnsList)
	if err != nil {
		return
	}
	return utils.BufferOut(stmt)
}

func (g *Generator) BenchSelect(options Options, tables []string, columnsList [][]*database.Column) (*ast.SelectStmt, error) {
	if RdFloat64() < options.AggregateWeight {
		return g.AggregateSelect(options, tables, columnsList)
	} else {
		return g.NormalSelect(options, tables, columnsList)
	}
}

func (g *Generator) NormalSelect(options Options, tables []string, columnsList [][]*database.Column) (stmt *ast.SelectStmt, err error) {
	stmt, err = g.PrepareSelect(tables, columnsList)
	if err != nil {
		return
	}

	// control the max count of order by clause
	orderBy := g.RdOrderBy(options, tables, columnsList)

	if len(orderBy) != 0 {
		stmt.OrderBy = &ast.OrderByClause{Items: orderBy}
	}
	if options.Limit != 0 {
		stmt.Limit = &ast.Limit{Count: utils.NewValueExpr(options.Limit)}
	}

	return
}

func (g *Generator) AggregateSelect(options Options, tables []string, columnsList [][]*database.Column) (stmt *ast.SelectStmt, err error) {
	stmt, err = g.PrepareSelect(tables, columnsList)
	if err != nil {
		return
	}

	items, groupColumns := g.RdGroupBy(options.MaxByItems, columnsList)
	fields := make([]*ast.SelectField, 0)

	for _, item := range items {
		fields = append(fields, &ast.SelectField{Expr: item.Expr})
	}

	for _, columns := range columnsList {
		for _, column := range columns {
			if !groupColumns[column] {
				fields = append(fields, &ast.SelectField{Expr: RdAggregateExpr(column)})
			}
		}
	}

	if len(items) > 0 {
		stmt.GroupBy = &ast.GroupByClause{Items: items}
		stmt.OrderBy = &ast.OrderByClause{Items: items}
	}
	stmt.Fields.Fields = fields

	if options.Limit != 0 {
		stmt.Limit = &ast.Limit{Count: utils.NewValueExpr(options.Limit)}
	}

	return
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
				subExpr := RdRangeConditionExpr(g.exec, column, values[j])
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

func (g *Generator) RdTablesAndKeys(option *Options) ([]string, [][]*database.Column) {
	tableNums := Rd(option.MaxTables) + 1
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

		if option.KeyOnly {
			keysList = append(keysList, table.Keys())
		} else {
			keysList = append(keysList, table.Columns)
		}
	}
	return tables, keysList
}

func (g *Generator) RdGroupBy(maxByItems int, tableColumns [][]*database.Column) ([]*ast.ByItem, map[*database.Column]bool) {
	itemNums := Rd(maxByItems + 1)
	items := make([]*ast.ByItem, 0, itemNums)
	columnSet := make(map[*database.Column]bool)
	for i := 0; i < itemNums; i++ {
		columns := tableColumns[Rd(len(tableColumns))]
		column := columns[Rd(len(columns))]
		if !columnSet[column] {
			items = append(items, &ast.ByItem{Expr: &ast.ColumnNameExpr{Name: column.ColumnName()}})
			columnSet[column] = true
		}
	}
	return items, columnSet
}

func (g *Generator) RdOrderBy(options Options, tables []string, tableColumns [][]*database.Column) []*ast.ByItem {
	var cols []*ast.ByItem
	if !options.UnstableOrderBy {
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

	elemLen := Rd(utils.MinInt(options.MaxByItems, len(cols)) + 1)
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
		SelectStmtOpts: &ast.SelectStmtOpts{},
		Fields:         fields,
		From:           tableRefs,
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
		return
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
