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
	"time"

	"github.com/pingcap/tidb/types"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Rd same to rand.Intn
func Rd(n int) int {
	return rand.Intn(n)
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

func RdBinaryOperator(ops []string) string {
	return ops[Rd(len(ops))]
}

func RdComparisionOp() string {
	return RdBinaryOperator([]string{"<=", ">="})
}

func RdLogicOp() string {
	return RdBinaryOperator([]string{"AND", "OR"})
}

func FormatValue(tp *types.FieldType, value *string) string {
	if value == nil {
		return "NULL"
	}
	switch tp.EvalType() {
	case types.ETInt, types.ETReal, types.ETDecimal:
		return *value
	default:
		return FormatStringLiteral(*value)
	}
}
