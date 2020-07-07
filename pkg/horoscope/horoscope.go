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

var (
	PlanHint = model.NewCIStr("NTH_PLAN")
)

type Horoscope struct {
	exec executor.Executor
	gen  generator.Generator
}

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
	return BufferOut(node)
}

func (h *Horoscope) QueryWithTime(round uint, query string) (durs []time.Duration, list []executor.Rows, err error) {
	log.WithFields(log.Fields{
		"query": query,
		"round": round,
	}).Debug("query with time")
	for i := 0; i < int(round); i++ {
		start := time.Now()
		rows, err := h.exec.Query(query)
		if err != nil {
			return nil, nil, err
		}
		durs = append(durs, time.Since(start))
		list = append(list, rows)
	}
	return
}

func (h *Horoscope) Step(round uint) (results *BenchResults, err error) {
	qID, query := h.gen.Query()
	if query == nil {
		return
	}

	originQuery, err := BufferOut(query)
	if err != nil {
		return
	}

	defaultPlanDurs, originList, err := h.QueryWithTime(round, originQuery)
	if err != nil {
		return
	}

	log.WithFields(log.Fields{
		"query id": qID,
		"query":    originQuery,
		"cost":     fmt.Sprintf("%v", defaultPlanDurs),
	}).Info("complete origin query")

	results = &BenchResults{
		QueryID: qID,
		Origin:  NewBenchResult(originQuery, round, defaultPlanDurs),
		Plans:   make([]BenchResult, 0),
	}

	rowsSet := make([][]executor.Rows, 0)

	var id int64 = 1
	for ; ; id++ {
		var plan string
		var durs []time.Duration
		var rows []executor.Rows

		plan, err = h.Plan(query, id)
		if err != nil {
			return
		}

		durs, rows, err = h.QueryWithTime(round, plan)

		if err != nil {
			if executor.PlanOutOfRange(err) {
				err = verifyQueryResult(originList, rowsSet)
			} else {
				log.WithFields(log.Fields{
					"query id": qID,
					"query":    plan,
					"error":    err,
				}).Errorf("executing query failed")
			}
			return
		}
		log.WithFields(log.Fields{
			"query id": qID,
			"query":    plan,
			"cost":     fmt.Sprintf("%v", durs),
		}).Infof("complete execution plan%d", id)

		rowsSet = append(rowsSet, rows)
		results.Plans = append(results.Plans, NewBenchResult(plan, round, durs))
	}
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
