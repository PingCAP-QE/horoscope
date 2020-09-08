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
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/types"

	"github.com/chaos-mesh/horoscope/pkg/database"
	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/utils"
)

var (
	logicOperators = []opcode.Op{opcode.LogicOr}
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Set the weight of "AND" operator
func SetAndOpWeight(weight int) {
	if weight > 0 {
		logicOperators = make([]opcode.Op, 0, weight+1)
		logicOperators = append(logicOperators, opcode.LogicOr)
		for ; weight > 0; weight-- {
			logicOperators = append(logicOperators, opcode.LogicAnd)
		}
	}
}

// Rd same to rand.Intn
func Rd(n int) int {
	return rand.Intn(n)
}

func RdBool() bool {
	return Rd(2) == 1
}

func RdInt63(n int64) int64 {
	return rand.Int63n(n)
}

// RdRange rand int in range
func RdRange(n, m int64) int64 {
	if n == m {
		return n
	}
	if m < n {
		n, m = m, n
	}
	return n + rand.Int63n(m-n)
}

func RdInt64() int64 {
	if Rd(2) == 1 {
		return rand.Int63()
	}
	return -rand.Int63() - 1
}

// RdFloat64 rand float64
func RdFloat64() float64 {
	return rand.Float64()
}

// RdDate rand date
func RdDate() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 1, 0, time.UTC).Unix()
	max := time.Date(2100, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// RdTimestamp return same format as RdDate except rand range
// TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07'
func RdTimestamp() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

func RdDuration() time.Duration {
	return time.Duration(RdInt64())
}

func FormatStringLiteral(value string) string {
	value = strings.Replace(value, `\`, `\\`, -1)
	value = strings.Replace(value, `'`, `\'`, -1)
	return fmt.Sprintf("'%s'", value)
}

func FormatTime(t time.Time) string {
	timeString := t.Format("2006-01-02 15:04:05")
	return FormatStringLiteral(timeString)
}

// TODO: Implement `FormatDuration`
func FormatDuration(dur time.Duration) string {
	panic("FormatDuration is unimplemented")
}

// RdString rand string with given length
func RdString(length int) string {
	res := ""
	for i := 0; i < length; i++ {
		charCode := RdRange(33, 127)
		// char '\' and '\'' should be escaped
		if charCode == 92 || charCode == 39 {
			charCode++
			// res = fmt.Sprintf("%s%s", res, "\\")
		}
		res += string(rune(charCode))
	}
	return res
}

// RdStringChar rand string with given length, letter chars only
func RdStringChar(length int) string {
	res := ""
	for i := 0; i < length; i++ {
		charCode := RdRange(97, 123)
		res += string(rune(charCode))
	}
	return res
}

func RdSQLValue(tp *types.FieldType) string {
	const MAX_STRING_LEN = 256
	switch tp.EvalType() {
	case types.ETInt:
		return fmt.Sprintf("%d", RdInt64())
	case types.ETReal, types.ETDecimal:
		return fmt.Sprintf("%f", RdFloat64())
	case types.ETDatetime:
		return FormatTime(RdDate())
	case types.ETTimestamp:
		return FormatTime(RdTimestamp())
	case types.ETDuration:
		return FormatDuration(RdDuration())
	case types.ETString:
		return FormatStringLiteral(RdString(Rd(MAX_STRING_LEN)))
	default:
		panic(fmt.Sprintf("unsupported field type: %s", tp.String()))
	}
}

func RdBinaryOperator(ops []opcode.Op) opcode.Op {
	return ops[Rd(len(ops))]
}

func RdLogicOp() opcode.Op {
	return RdBinaryOperator(logicOperators)
}

func FormatValue(tp *types.FieldType, value []byte) string {
	if value == nil {
		return "NULL"
	}
	switch tp.EvalType() {
	case types.ETInt, types.ETReal, types.ETDecimal:
		return string(value)
	default:
		return FormatStringLiteral(string(value))
	}
}

func RdValue(exec executor.Executor, column *database.Column) (value []byte, err error) {
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", column.Name.String(), column.Table.Name.String())
	rows, err := exec.Query(query)
	if err != nil {
		return
	}

	if rows.RowCount() == 0 {
		err = fmt.Errorf("table %s is empty", column.Table)
		return
	}

	value = rows.Data[0][0]
	return
}

func RdGreaterValue(exec executor.Executor, column *database.Column, less []byte) (value []byte, err error) {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s > %s ORDER BY RAND() LIMIT 1",
		column.Name.String(), column.Table.Name.String(), column.Name.String(), FormatValue(column.Type, less),
	)
	rows, err := exec.Query(query)
	if err != nil {
		return
	}

	if rows.RowCount() == 0 {
		err = fmt.Errorf("there is no %s greater than %s", column, FormatValue(column.Type, less))
		return
	}
	value = rows.Data[0][0]
	return
}

func RdLessValue(exec executor.Executor, column *database.Column, less []byte) (value []byte, err error) {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s < %s ORDER BY RAND() LIMIT 1",
		column.Name.String(), column.Table.Name.String(), column.Name.String(), FormatValue(column.Type, less),
	)
	rows, err := exec.Query(query)
	if err != nil {
		return
	}

	if rows.RowCount() == 0 {
		err = fmt.Errorf("there is no %s less than %s", column, FormatValue(column.Type, less))
		return
	}
	value = rows.Data[0][0]
	return
}

func RdNotEqualValue(exec executor.Executor, column *database.Column, value []byte) (other []byte, err error) {
	other, err = RdLessValue(exec, column, value)
	if err == nil {
		return
	}
	other, err = RdGreaterValue(exec, column, value)
	if err == nil {
		return
	}
	err = fmt.Errorf("there is no value in %s other than %s", column, FormatValue(column.Type, value))
	return
}

func RdInRange(column *database.Column, value []byte, exec executor.Executor) (rg []ast.ExprNode, err error) {
	const MaxAdditional = 10
	rg = []ast.ExprNode{utils.NewValueExpr(value)}

	for i := 0; i < Rd(MaxAdditional); i++ {
		var val []byte
		val, err = RdValue(exec, column)
		if err != nil {
			return
		}
		rg = append(rg, utils.NewValueExpr(val))
	}

	return
}

func RdRangeConditionExpr(exec executor.Executor, column *database.Column, value []byte) ast.ExprNode {
	opList := make([]RangeCondition, 0)
	for _, op := range conditions {
		if op.Suit(column.Type, value) {
			opList = append(opList, op)
		}
	}

	for {
		randOp := opList[Rd(len(opList))]
		if expr := randOp.RdExpr(column, value, exec); expr != nil {
			return expr
		}
	}
}

func RdAggregateExpr(column *database.Column) ast.ExprNode {
	fnList := make([]AggregateFunc, 0)
	for _, fn := range aggregates {
		if fn.Suit(column.Type) {
			fnList = append(fnList, fn)
		}
	}
	return fnList[Rd(len(fnList))].RdExpr(column)
}

func RdTowColumns(columnsList [][]*database.Column) []*database.Column {
	ret := []*database.Column{nil, nil}
	ret[0] = RdColumns(columnsList)
	if ret[0] == nil {
		return nil
	}

	columnSlice := make([]*database.Column, 0)
	for _, columns := range columnsList {
		for _, column := range columns {
			if column != ret[0] && column.Type.EvalType() == ret[0].Type.EvalType() {
				columnSlice = append(columnSlice, column)
			}
		}
	}

	if len(columnSlice) < 1 {
		return nil
	}

	ret[1] = columnSlice[Rd(len(columnSlice))]

	return ret
}

func RdColumns(columnsList [][]*database.Column) *database.Column {
	columnSlice := make([]*database.Column, 0)
	for _, columns := range columnsList {
		for _, column := range columns {
			columnSlice = append(columnSlice, column)
		}
	}
	if len(columnSlice) < 1 {
		return nil
	}

	return columnSlice[Rd(len(columnSlice))]
}
