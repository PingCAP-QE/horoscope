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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/types"

	"github.com/chaos-mesh/horoscope/pkg/database"
)

const (
	Count  AggregateFunc = ast.AggFuncCount
	Sum                  = ast.AggFuncSum
	Avg                  = ast.AggFuncAvg
	Max                  = ast.AggFuncMax
	Min                  = ast.AggFuncMin
	VarPop               = ast.AggFuncVarPop
)

var (
	aggregates = []AggregateFunc{
		Count,
		Sum,
		Avg,
		Max,
		Min,
		VarPop,
	}
)

type (
	AggregateFunc string
)

func (fn AggregateFunc) Suit(tp *types.FieldType) bool {
	switch fn {
	case Count, Max, Min:
		return true
	case Sum, Avg, VarPop:
		switch tp.EvalType() {
		case types.ETInt, types.ETReal, types.ETDecimal, types.ETDuration:
			return true
		}
	}
	return false
}

/// RdExpr is a function generating random expression.
func (fn AggregateFunc) RdExpr(column *database.Column) ast.ExprNode {
	columnExpr := &ast.ColumnNameExpr{Name: column.ColumnName()}
	return &ast.AggregateFuncExpr{F: string(fn), Args: []ast.ExprNode{columnExpr}}
}
