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

package horoscope

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/generator"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/errno"
	log "github.com/sirupsen/logrus"
)

var (
	PlanHint = model.NewCIStr("NTH_PLAN")
)

type (
	Horoscope struct {
		exec executor.Executor
		gen  generator.Generator
	}

	BenchResult struct {
		Round uint
		Sql   string
		Cost  time.Duration
	}

	BenchResults struct {
		Origin BenchResult
		Plans  []BenchResult
	}
)

func NewHoroscope(exec executor.Executor, gen generator.Generator) *Horoscope {
	return &Horoscope{exec: exec, gen: gen}
}

func (h *Horoscope) Plan(node ast.StmtNode, planId int64) (string, error) {
	switch stmt := node.(type) {
	case *ast.SelectStmt:
		if planHint := findPlanHint(stmt.TableHints); planHint != nil {
			planHint.HintData = planId
		} else {
			stmt.TableHints = []*ast.TableOptimizerHint{
				{HintName: PlanHint, HintData: planId},
			}
		}
	default:
		return "", errors.New("unsupported statement")
	}
	return bufferOut(node)
}

func (h *Horoscope) QueryWithTime(round uint, query string) (dur time.Duration, list []executor.Rows, err error) {
	log.WithFields(log.Fields{
		"query": query,
		"round": round,
	}).Debug("query with time")
	start := time.Now()
	list, err = h.exec.Query(query, round)
	dur = time.Since(start)
	return
}

func (h *Horoscope) Step(round uint) (results *BenchResults, err error) {
	query := h.gen.Query()
	if query == nil {
		return
	}

	originQuery, err := bufferOut(query)
	if err != nil {
		return
	}

	originDur, originList, err := h.QueryWithTime(round, originQuery)
	if err != nil {
		return
	}

	log.WithFields(log.Fields{
		"query": originQuery,
		"cost":  fmt.Sprintf("%dms", originDur.Milliseconds()),
	}).Info("complete origin query")

	results = &BenchResults{
		Origin: BenchResult{Round: round, Cost: originDur, Sql: originQuery},
		Plans:  make([]BenchResult, 0),
	}

	rowsSet := make([][]executor.Rows, 0)

	var id int64 = 1
	for ; ; id++ {
		var plan string
		var dur time.Duration
		var rows []executor.Rows

		plan, err = h.Plan(query, id)
		if err != nil {
			return
		}

		dur, rows, err = h.QueryWithTime(round, plan)
		log.WithFields(log.Fields{
			"query": plan,
			"cost":  fmt.Sprintf("%dms", dur.Milliseconds()),
		}).Infof("complete execution plan%d", id)

		if err != nil {
			if PlanOutOfRange(err) {
				err = verifyQueryResult(originList, rowsSet)
			}
			return
		}

		rowsSet = append(rowsSet, rows)
		results.Plans = append(results.Plans, BenchResult{Round: round, Sql: plan, Cost: dur})
	}
}

func bufferOut(node ast.Node) (string, error) {
	out := new(bytes.Buffer)
	err := node.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

func findPlanHint(hints []*ast.TableOptimizerHint) *ast.TableOptimizerHint {
	if len(hints) > 0 {
		for _, hint := range hints {
			if hint.HintName.L == PlanHint.L {
				return hint
			}
		}
	}
	return nil
}

func PlanOutOfRange(err error) bool {
	mysqlErr, ok := err.(*mysql.MySQLError)
	return ok && mysqlErr.Number == errno.ErrUnknown && strings.Contains(mysqlErr.Message, "nth_plan")
}

func verifyQueryResult(origin []executor.Rows, lists [][]executor.Rows) (err error) {
	for _, list := range lists {
		if !verifyList(origin, list) {
			return errors.New(fmt.Sprintf("query results verification fails: origin(%#v), result(%#v", origin, list))
		}
	}
	return
}

func verifyList(one, other []executor.Rows) bool {
	if len(one) != len(other) {
		return false
	}
	for i, column := range one {
		if !column.Equal(other[i]) {
			return false
		}
	}
	return true
}
