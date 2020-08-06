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

	"github.com/pingcap/parser/types"
	log "github.com/sirupsen/logrus"

	"github.com/chaos-mesh/horoscope/pkg/database"
	"github.com/chaos-mesh/horoscope/pkg/executor"
)

const (
	/// =
	Equal CmpOperator = iota

	/// <=>
	NullSafeEqual

	/// IN()
	In

	/// IS NULL
	IsNull

	/// IS NOT NULL
	IsNotNull

	/// >
	Greater

	/// <
	Less

	///>=
	GreaterEqual

	/// <=
	LessEqual

	/// BETWEEN ... AND
	Between

	/// !=
	NotEqual

	/// <>
	NotEqual2

	/// LIKE
	Like
)

var (
	Ops = []CmpOperator{
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
		NotEqual2,
		Like,
	}
)

type (
	CmpOperator uint8
)

func (op CmpOperator) Suit(tp *types.FieldType, value []byte) bool {
	switch op {
	case NullSafeEqual:
		return true
	case IsNull:
		return value == nil
	case Equal, In, IsNotNull, Greater, Less, GreaterEqual, LessEqual, Between, NotEqual, NotEqual2:
		return value != nil
	case Like:
		return value != nil && tp.EvalType() == types.ETString
	default:
		return false
	}
}

/// RdExpr is a function generating random expression.
/// empty expression represents for failure
func (op CmpOperator) RdExpr(column *database.Column, value []byte, exec executor.Executor) (expr string) {
	if !op.Suit(column.Type, value) {
		return
	}

	var err error

	switch op {
	case Equal:
		expr = fmt.Sprintf("%s = %s", column, FormatValue(column.Type, value))
	case NullSafeEqual:
		expr = fmt.Sprintf("%s <=> %s", column, FormatValue(column.Type, value))
	case In:
		var rg string
		rg, err = RdInRange(column, value, exec)
		if err != nil {
			break
		}
		expr = fmt.Sprintf("%s IN (%s)", column, rg)
	case IsNull:
		expr = fmt.Sprintf("%s IS NULL", column)
	case IsNotNull:
		expr = fmt.Sprintf("%s IS NOT NULL", column)
	case Greater:
		var cmpValue string
		cmpValue, err = RdLessValue(exec, column, value)
		if err != nil {
			break
		}
		expr = fmt.Sprintf("%s > %s", column, cmpValue)
	case Less:
		var cmpValue string
		cmpValue, err = RdGreaterValue(exec, column, value)
		if err != nil {
			break
		}
		expr = fmt.Sprintf("%s < %s", column, cmpValue)
	case GreaterEqual:
		expr = fmt.Sprintf("%s >= %s", column, FormatValue(column.Type, value))
	case LessEqual:
		expr = fmt.Sprintf("%s <= %s", column, FormatValue(column.Type, value))
	case Between:
		var lessValue string
		var greaterValue string
		lessValue, err = RdLessValue(exec, column, value)
		if err != nil {
			break
		}
		greaterValue, err = RdGreaterValue(exec, column, value)
		if err != nil {
			break
		}
		expr = fmt.Sprintf("%s BETWEEN %s AND %s", column, lessValue, greaterValue)
	case NotEqual:
		var cmpValue string
		cmpValue, err = RdNotEqualValue(exec, column, value)
		if err != nil {
			break
		}
		expr = fmt.Sprintf("%s != %s", column, cmpValue)
	case NotEqual2:
		var cmpValue string
		cmpValue, err = RdNotEqualValue(exec, column, value)
		if err != nil {
			break
		}
		expr = fmt.Sprintf("%s <> %s", column, cmpValue)
	case Like:
		pattern := append(value[:Rd(len(value)-1)+1], '%')
		expr = fmt.Sprintf("%s LIKE %s", column, FormatValue(column.Type, pattern))
	}

	if err != nil {
		log.Warnf("err in RdInRange: %s", err.Error())
	}
	return
}
