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
	"time"

	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/generator"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	log "github.com/sirupsen/logrus"
)

const (
	DQL QueryType = iota
	DML
)

var (
	PlanHint = model.NewCIStr("NTH_PLAN")
)

type (
	Horoscope struct {
		exec executor.Executor
		gen  generator.Generator
	}

	Bench struct {
		Plan        int64
		SQL         string
		Hints       executor.Hints
		Explanation executor.Rows
		Cost        time.Duration
	}

	Benches struct {
		QueryID     string
		SQL         string
		Query       ast.StmtNode
		Type        QueryType
		Round       uint
		Hints       executor.Hints
		Cost        time.Duration
		DefaultPlan int64
		Explanation executor.Rows
		Plans       []*Bench
	}

	QueryType uint8
)

func NewHoroscope(exec executor.Executor, gen generator.Generator) *Horoscope {
	return &Horoscope{exec: exec, gen: gen}
}

func (h *Horoscope) InitBenches(query ast.StmtNode, round uint, queryID string) (benches *Benches, err error) {
	sql, err := BufferOut(query)
	if err != nil {
		return
	}

	hints, _, err := h.exec.GetHints(sql)
	if err != nil {
		return
	}

	explanation, err := h.exec.Explain(sql)
	if err != nil {
		return
	}

	benches = &Benches{
		QueryID:     queryID,
		Round:       round,
		SQL:         sql,
		Query:       query,
		Hints:       hints,
		Explanation: explanation,
		Plans:       make([]*Bench, 0),
	}

	var optHints *[]*ast.TableOptimizerHint
	benches.Type, optHints, err = AnalyzeQuery(query, sql)
	if err != nil {
		return
	}

	var id int64 = 1
	for ; ; id++ {
		var plan string
		var warnings []error

		plan, err = Plan(query, optHints, id)
		if err != nil {
			return
		}

		hints, warnings, err = h.exec.GetHints(plan)

		if err != nil {
			return
		}

		for _, warning := range warnings {
			if executor.PlanOutOfRange(warning) {
				return
			}
		}

		explanation, err = h.exec.Explain(plan)
		if err != nil {
			return
		}

		if benches.Explanation.Equal(explanation) {
			benches.DefaultPlan = id
		}

		benches.Plans = append(benches.Plans, &Bench{
			Hints:       hints,
			Explanation: explanation,
			Plan:        id,
			SQL:         plan,
		})
	}
}

func (h *Horoscope) QueryWithTime(round uint, query string, tp QueryType) (dur time.Duration, list []executor.Comparable, err error) {
	log.WithFields(log.Fields{
		"query": query,
		"round": round,
	}).Debug("query with time")
	start := time.Now()
	switch tp {
	case DQL:
		list, err = h.exec.Query(query, round)
	case DML:
		list, err = h.exec.Exec(query, round)
	default:
		panic("Query type should be checked in `InitBenches`")
	}
	dur = time.Since(start)
	return
}

func (h *Horoscope) Step(round uint) (benches *Benches, err error) {
	qID, query := h.gen.Query()
	if query == nil {
		return
	}

	benches, err = h.InitBenches(query, round, qID)

	if err != nil {
		return
	}

	originDur, originList, err := h.QueryWithTime(round, benches.SQL, benches.Type)
	if err != nil {
		return
	}

	benches.Cost = originDur

	log.WithFields(log.Fields{
		"query id": qID,
		"query":    benches.SQL,
		"cost":     fmt.Sprintf("%dms", originDur.Milliseconds()),
	}).Info("complete origin query")

	rowsSet := make([][]executor.Comparable, 0)

	for _, plan := range benches.Plans {
		var dur time.Duration
		var list []executor.Comparable

		dur, list, err = h.QueryWithTime(round, plan.SQL, benches.Type)
		if err != nil {
			return
		}

		log.WithFields(log.Fields{
			"query id": qID,
			"query":    plan.SQL,
			"cost":     fmt.Sprintf("%dms", dur.Milliseconds()),
		}).Infof("complete execution plan%d", plan.Plan)

		rowsSet = append(rowsSet, list)
		plan.Cost = dur
	}

	err = verifyQueryResult(originList, rowsSet)
	return
}

func BufferOut(node ast.Node) (string, error) {
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

func verifyQueryResult(origin []executor.Comparable, lists [][]executor.Comparable) (err error) {
	for _, list := range lists {
		if !verifyList(origin, list) {
			return errors.New(fmt.Sprintf("query results verification fails: origin(%#v), result(%#v", origin, list))
		}
	}
	return
}

func verifyList(one, other []executor.Comparable) bool {
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

func Plan(node ast.StmtNode, hints *[]*ast.TableOptimizerHint, planId int64) (string, error) {
	if planId > 0 {
		if planHint := findPlanHint(*hints); planHint != nil {
			planHint.HintData = planId
		} else {
			*hints = append(*hints, &ast.TableOptimizerHint{
				HintName: PlanHint, HintData: planId,
			})
		}
	}
	return BufferOut(node)
}

func AnalyzeQuery(query ast.StmtNode, sql string) (tp QueryType, hints *[]*ast.TableOptimizerHint, err error) {
	switch stmt := query.(type) {
	case *ast.SelectStmt:
		tp = DQL
		hints = &stmt.TableHints
	case *ast.InsertStmt:
		tp = DML
		hints = &stmt.TableHints
	case *ast.UpdateStmt:
		tp = DML
		hints = &stmt.TableHints
	case *ast.DeleteStmt:
		tp = DML
		hints = &stmt.TableHints
	default:
		err = errors.New(fmt.Sprintf("Unsupported query: %s", sql))
	}
	return
}
