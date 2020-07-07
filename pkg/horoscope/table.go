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
	"github.com/jedib0t/go-pretty/table"
	"golang.org/x/perf/benchstat"
	"strings"
)

// Table is used for displaying in output
type Table struct {
	Metric  string
	Headers []string
	Rows    []*Row
}

type Row struct {
	QueryId        string
	Query          string
	PlanSpaceCount int
	DefaultPlanDur string
	BestPlanDur    string
	OptimalPlan    []string
	Effectiveness  float64
}

func (r *Row) toTableRows() table.Row {
	var row table.Row
	row = append(row, r.QueryId, r.PlanSpaceCount, r.DefaultPlanDur, r.BestPlanDur,
		fmt.Sprintf("%.1f%%", r.Effectiveness*100), strings.Join(r.OptimalPlan, ","), r.Query)
	return row
}

type BenchCollection []*Benches

func (c *BenchCollection) Table() Table {
	alpha := 0.05
	table := Table{Metric: "execution time", Headers: []string{"id", "#plan space", "default execution time", "best plan execution time", "effectiveness", "better optimal plans", "query"}}
	for _, b := range *c {
		defaultPlanDurs, bestPlanDurs, betterPlanCount, optimalPlan := b.Cost, b.Cost, 0, make([]string, 0)

		for _, p := range b.Plans {
			if p.Cost.Mean < defaultPlanDurs.Mean {
				currentPlanDurs := p.Cost
				pval, testerr := benchstat.TTest(&benchstat.Metrics{
					Unit:    defaultPlanDurs.Unit,
					Values:  defaultPlanDurs.Values,
					RValues: defaultPlanDurs.RValues,
					Min:     defaultPlanDurs.Min,
					Mean:    defaultPlanDurs.Mean,
					Max:     defaultPlanDurs.Max,
				}, &benchstat.Metrics{
					Unit:    currentPlanDurs.Unit,
					Values:  currentPlanDurs.Values,
					RValues: currentPlanDurs.RValues,
					Min:     currentPlanDurs.Min,
					Mean:    currentPlanDurs.Mean,
					Max:     currentPlanDurs.Max,
				})
				if testerr != nil || testerr == nil && pval < alpha {
					betterPlanCount += 1
					optimalPlan = append(optimalPlan, fmt.Sprintf("#%d(%0.1f%%)", p.Plan, 100*currentPlanDurs.Mean/defaultPlanDurs.Mean))
					if currentPlanDurs.Mean < bestPlanDurs.Mean {
						bestPlanDurs = currentPlanDurs
					}
				}
			}
		}
		planSpaceCount := len(b.Plans)
		row := Row{
			QueryId:        b.QueryID,
			Query:          b.SQL,
			PlanSpaceCount: len(b.Plans),
			DefaultPlanDur: b.Cost.format(),
			BestPlanDur:    bestPlanDurs.format(),
			OptimalPlan:    optimalPlan,
			Effectiveness:  float64(planSpaceCount-betterPlanCount) / float64(planSpaceCount),
		}
		table.Rows = append(table.Rows, &row)
	}
	return table
}

func (t Table) String() string {
	w := table.NewWriter()
	var headers table.Row
	for _, h := range t.Headers {
		headers = append(headers, h)
	}
	w.AppendHeader(headers)
	for _, row := range t.Rows {
		w.AppendRow(row.toTableRows())
	}
	return w.Render()
}
