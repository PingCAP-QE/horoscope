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
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	log "github.com/sirupsen/logrus"
	"golang.org/x/perf/benchstat"

	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/loader"
	"github.com/chaos-mesh/horoscope/pkg/utils"
)

const (
	DQL QueryType = iota
	DML
)

var (
	PlanHint = model.NewCIStr("NTH_PLAN")
)

// ServerError wraps server error
type ServerError struct {
	error
}

type (
	Horoscope struct {
		exec                   executor.Pool
		differentialExecs      []executor.Pool
		loader                 loader.QueryLoader
		enableCollectCardError bool
		explicitTxn            bool
	}
	QueryType uint8
)

func NewHoroscope(exec executor.Pool, differentialExecs []executor.Pool, loader loader.QueryLoader, enableCollectCardError bool) *Horoscope {
	return &Horoscope{exec: exec, differentialExecs: differentialExecs, loader: loader, enableCollectCardError: enableCollectCardError}
}

func (h *Horoscope) Next(round uint, maxPlans uint64, verify bool, ignoreServerError bool) (benches *Benches, err error) {
	exec, closeFunc, err := newExecutorFromPool(h.exec, h.explicitTxn)
	if err != nil {
		return nil, fmt.Errorf("create an executor failed: %v", err)
	}
	defer closeFunc()

	qID, query := h.loader.Next()
	if query == nil {
		return
	}

	benches, err = h.collectPlans(qID, query, maxPlans)
	if err != nil {
		return
	}
	log.WithFields(log.Fields{
		"query id":        qID,
		"query":           benches.DefaultPlan.SQL,
		"plan space size": len(benches.Plans),
	}).Info("complete plan collection")

	benches.Round = round

	cost, originResultSets, err := RunSQLWithTime(exec, benches.Round, benches.DefaultPlan.SQL, benches.Type)
	if err != nil {
		return
	}
	testOracle := originResultSets[0]

	benches.DefaultPlan.Cost = cost
	if h.enableCollectCardError {
		b, j, e := h.CollectCardinalityEstimationError(benches.DefaultPlan.SQL)
		if e != nil {
			return nil, err
		}
		benches.DefaultPlan.BaseTableCardInfo, benches.DefaultPlan.JoinTableCardInfo = b, j
	}
	log.WithFields(log.Fields{
		"query id": qID,
		"query":    benches.DefaultPlan.SQL,
		"cost":     fmt.Sprintf("%vms", cost.Values),
		"hints":    benches.DefaultPlan.Hints,
	}).Info("complete origin query")

	for _, plan := range benches.Plans {
		var sets []executor.Comparable
		cost, sets, err = RunSQLWithTime(exec, round, plan.SQL, benches.Type)
		if err != nil {
			if _, serverError := err.(ServerError); serverError && ignoreServerError {
				continue
			}
			return nil, err
		}
		plan.Cost = cost

		if h.enableCollectCardError {
			b, j, e := h.CollectCardinalityEstimationError(plan.SQL)
			if e != nil {
				return nil, err
			}
			plan.BaseTableCardInfo, plan.JoinTableCardInfo = b, j
			var baseTableQErrorStats [][]interface{}
			var joinTableQErrorStats [][]interface{}
			for _, c := range plan.BaseTableCardInfo {
				baseTableQErrorStats = append(baseTableQErrorStats, []interface{}{c.QError, c.OpInfo})
			}
			for _, c := range plan.JoinTableCardInfo {
				joinTableQErrorStats = append(joinTableQErrorStats, []interface{}{c.QError, c.OpInfo})
			}
			log.WithFields(log.Fields{
				"#base table": len(plan.BaseTableCardInfo),
				"base table":  baseTableQErrorStats,
				"#join table": len(plan.JoinTableCardInfo),
				"join table":  joinTableQErrorStats,
			}).Infof("cardinality estimation error for query %s, plan%d", benches.QueryID, plan.Plan)
		}

		log.WithFields(log.Fields{
			"query id": qID,
			"query":    plan.SQL,
			"cost":     fmt.Sprintf("%vms", cost.Values),
			"hints":    plan.Hints,
		}).Infof("complete execution plan%d", plan.Plan)

		if verify {
			for _, set := range sets {
				if !testOracle.Equal(set) {
					benches.VerifiedFail = true
					err = fmt.Errorf("results mismatch in plan(%d)", plan.Plan)
					return
				}
			}
		}
	}

	if verify {
		for _, pool := range h.differentialExecs {
			var results []executor.Comparable
			var dExec executor.Executor
			var dCloseFunc func()
			dExec, dCloseFunc, err = newExecutorFromPool(pool, h.explicitTxn)
			if err != nil {
				return
			}
			defer dCloseFunc()
			_, results, err = RunSQLWithTime(dExec, benches.Round, benches.DefaultPlan.SQL, benches.Type)
			if err != nil {
				return
			}

			for _, result := range results {
				if !testOracle.Equal(result) {
					benches.VerifiedFail = true
					err = fmt.Errorf("results mismatch in different DSN: %s <=> %s", exec.Dsn(), dExec.Dsn())
					return
				}
			}
		}
	}
	return
}

func RunSQLWithTime(exec executor.Executor, round uint, query string, tp QueryType) (*Metrics, []executor.Comparable, error) {
	var (
		costs = Metrics(benchstat.Metrics{
			Unit: "ms",
		})
		list []executor.Comparable
		err  error
	)

	log.WithFields(log.Fields{
		"query": query,
		"round": round,
	}).Debug("query with time")

	for i := 0; i < int(round); i++ {
		start := time.Now()
		var rows executor.Comparable
		switch tp {
		case DQL:
			rows, err = exec.Query(query)
		case DML:
			rows, err = exec.Exec(query)
		default:
			panic("Next type should be checked in `collectPlans`")
		}
		if err != nil {
			return nil, nil, ServerError{err}
		}
		costs.Values = append(costs.Values, float64(time.Since(start).Microseconds()/1000))
		list = append(list, rows)
	}

	costs.computeStats()
	return &costs, list, nil
}

func (h *Horoscope) CollectCardinalityEstimationError(query string) (baseTable []*executor.CardinalityInfo, join []*executor.CardinalityInfo, err error) {
	rows, _, err := h.exec.Executor().ExplainAnalyze(query)
	if err != nil {
		return nil, nil, fmt.Errorf("explain analyze error: %v", err)
	}
	cis := executor.CollectEstAndActRows(executor.NewExplainAnalyzeInfo(rows))
	for _, ci := range cis {
		if ci.Op == "Selection" {
			baseTable = append(baseTable, ci)
		} else if strings.Contains(ci.Op, "Join") {
			join = append(join, ci)
		}
	}
	return
}

func (h *Horoscope) collectPlans(queryID string, query ast.StmtNode, maxPlans uint64) (benches *Benches, err error) {
	sql, err := utils.BufferOut(query)
	if err != nil {
		return
	}

	hints, err := h.exec.Executor().GetHints(sql)
	if err != nil {
		return
	}

	explanation, _, err := h.exec.Executor().Explain(sql)
	if err != nil {
		return
	}
	log.Infof("query explain start %s:\n%s\nquery explain end\n", queryID, explanation.String())

	benches = &Benches{
		QueryID: queryID,
		DefaultPlan: Bench{
			Plan:        0,
			SQL:         sql,
			Hints:       hints,
			Explanation: explanation,
		},
		Query: query,
		Plans: make([]*Bench, 0),
	}

	var optHints *[]*ast.TableOptimizerHint
	benches.Type, optHints, err = AnalyzeQuery(query, sql)
	if err != nil {
		return
	}

	var id uint64 = 1
	for ; id <= maxPlans; id++ {
		var plan string
		var warnings []error

		plan, err = Plan(query, optHints, int64(id))
		if err != nil {
			return
		}

		explanation, warnings, err = h.exec.Executor().Explain(plan)
		if err != nil {
			return
		}

		for _, warning := range warnings {
			if executor.PlanOutOfRange(warning) {
				return
			}
		}

		hints, err = h.exec.Executor().GetHints(plan)
		if err != nil {
			return
		}

		if benches.DefaultPlan.Explanation.Equal(explanation) {
			benches.DefaultPlan.Plan = id
		}

		benches.Plans = append(benches.Plans,
			&Bench{
				Hints:       hints,
				Explanation: explanation,
				Plan:        id,
				SQL:         plan,
			})
	}
	return
}

func newExecutorFromPool(pool executor.Pool, explicitTxn bool) (exec executor.Executor, closeFunc func(), err error) {
	if explicitTxn {
		exec, err := pool.Transaction()
		return exec, func() {
			exec.Rollback()
		}, err
	}
	return pool.Executor(), func() {}, nil
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
	return utils.BufferOut(node)
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
		err = fmt.Errorf("unsupported query: %s", sql)
	}
	return
}

func IsSubOptimal(defPlan *Bench, plan *Bench) bool {
	const alpha, thresholdPct = 0.05, 0.9
	// Some plans failed because of server errors, ignore this case
	if plan.Cost == nil {
		return false
	}
	if plan.Cost.Mean >= thresholdPct*defPlan.Cost.Mean {
		return false
	}
	defaultPlanCost, currentPlanCost := defPlan.Cost, plan.Cost
	pVal, testErr := benchstat.TTest(&benchstat.Metrics{
		Unit:    defaultPlanCost.Unit,
		Values:  defaultPlanCost.Values,
		RValues: defaultPlanCost.RValues,
		Min:     defaultPlanCost.Min,
		Mean:    defaultPlanCost.Mean,
		Max:     defaultPlanCost.Max,
	}, &benchstat.Metrics{
		Unit:    currentPlanCost.Unit,
		Values:  currentPlanCost.Values,
		RValues: currentPlanCost.RValues,
		Min:     currentPlanCost.Min,
		Mean:    currentPlanCost.Mean,
		Max:     currentPlanCost.Max,
	})
	if testErr != nil || pVal < alpha {
		return true
	}
	return false
}
