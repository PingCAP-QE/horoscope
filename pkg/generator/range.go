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
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/types"
	log "github.com/sirupsen/logrus"

	util "github.com/chaos-mesh/horoscope/pkg"
	"github.com/chaos-mesh/horoscope/pkg/database"
	"github.com/chaos-mesh/horoscope/pkg/executor"
)

const (
	/// =
	Equal = RangeCondition(opcode.EQ)

	/// <=>
	NullSafeEqual = RangeCondition(opcode.NullEQ)

	/// IN()
	In = RangeCondition(opcode.In)

	/// IS NULL
	IsNull = RangeCondition(opcode.IsNull)

	/// >
	Greater = RangeCondition(opcode.GT)

	/// <
	Less = RangeCondition(opcode.LT)

	///>=
	GreaterEqual = RangeCondition(opcode.GE)

	/// <=
	LessEqual = RangeCondition(opcode.LE)

	/// !=
	NotEqual = RangeCondition(opcode.NE)

	/// LIKE
	Like = RangeCondition(opcode.Like)

	/// BETWEEN ... AND
	Between = RangeCondition(iota + opcode.IsFalsity + 1)

	IsNotNull
)

var (
	conditions = []RangeCondition{
		Equal,
		NullSafeEqual,
		In,
		IsNull,
		IsNotNull,
		Greater,
		Less,
		GreaterEqual,
		LessEqual,
		Between,
		NotEqual,
		Like,
	}
)

type (
	RangeCondition int
)

func (condition RangeCondition) Suit(tp *types.FieldType, value []byte) bool {
	switch condition {
	case NullSafeEqual:
		return true
	case IsNull:
		return value == nil
	case Equal, IsNotNull, In, Greater, Less, GreaterEqual, LessEqual, Between, NotEqual:
		return value != nil
	case Like:
		return value != nil && tp.EvalType() == types.ETString
	default:
		return false
	}
}

/// RdExpr is a function generating random expression.
/// empty expression represents for failure
func (condition RangeCondition) RdExpr(column *database.Column, value []byte, exec executor.Executor) (expr ast.ExprNode) {
	if !condition.Suit(column.Type, value) {
		return
	}

	var err error

	var genCmpValue = func(condition RangeCondition) ([]byte, error) {
		switch condition {
		case Greater:
			return RdGreaterValue(exec, column, value)
		case Less:
			return RdLessValue(exec, column, value)
		case NotEqual:
			return RdNotEqualValue(exec, column, value)
		case Like:
			return append(value[:Rd(len(value))+1], '%'), nil
		default:
			return value, nil
		}
	}

	columnNameExpr := &ast.ColumnNameExpr{Name: column.ColumnName()}

	switch condition {
	case Equal, NullSafeEqual, GreaterEqual, LessEqual, Less, Greater, NotEqual, Like:
		var cmpValue []byte
		cmpValue, err = genCmpValue(condition)
		if err != nil {
			break
		}
		expr = &ast.BinaryOperationExpr{
			L:  columnNameExpr,
			Op: opcode.Op(condition),
			R:  util.NewValueExpr(cmpValue),
		}
	case In:
		var rg []ast.ExprNode
		rg, err = RdInRange(column, value, exec)
		if err != nil {
			break
		}
		expr = &ast.PatternInExpr{
			Expr: columnNameExpr,
			List: rg,
		}
	case IsNull:
		expr = &ast.IsNullExpr{Expr: columnNameExpr}
	case IsNotNull:
		expr = &ast.IsNullExpr{Expr: columnNameExpr, Not: true}
	case Between:
		var lessValue, greaterValue []byte
		lessValue, err = RdLessValue(exec, column, value)
		if err != nil {
			break
		}
		greaterValue, err = RdGreaterValue(exec, column, value)
		if err != nil {
			break
		}

		expr = &ast.BetweenExpr{
			Expr:  columnNameExpr,
			Left:  util.NewValueExpr(lessValue),
			Right: util.NewValueExpr(greaterValue),
		}
	}

	if err != nil {
		log.Warnf("err in RdInRange: %s", err.Error())
	}
	return
}
