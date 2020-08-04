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

	"github.com/jedib0t/go-pretty/table"
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
	DefaultPlanId  int
	DefaultPlanDur string
	BestPlanDur    string
	OptimalPlan    []string
	Effectiveness  float64
	EstRowsQError  []string
}

func (r *Row) toTableRows() table.Row {
	var row table.Row
	row = append(row, r.QueryId, r.PlanSpaceCount, fmt.Sprintf("%2d: %s", r.DefaultPlanId, r.DefaultPlanDur), r.BestPlanDur,
		fmt.Sprintf("%.1f%%", r.Effectiveness*100), strings.Join(r.OptimalPlan, ","), strings.Join(r.EstRowsQError, " "), r.Query)
	return row
}

type BenchCollection []*Benches

func (c *BenchCollection) Table() Table {
	table := Table{Metric: "execution time", Headers: []string{"id", "#plan space", "default execution time", "best plan execution time", "effectiveness", "better optimal plans", "estRow q-error", "query"}}
	for _, b := range *c {
		defaultPlan, bestPlan, betterPlanCount, optimalPlan := &b.DefaultPlan, &b.DefaultPlan, 0, make([]string, 0)
		baseTableBookMap, baseTableMetrics := make(map[string]struct{}), Metrics{}
		var estRowsQError []string
		for _, p := range b.Plans {
			if IsSubOptimal(defaultPlan, p) {
				betterPlanCount += 1
				optimalPlan = append(optimalPlan, fmt.Sprintf("#%d(%0.1f%%)", p.Plan, 100*p.Cost.Mean/defaultPlan.Cost.Mean))
				if p.Cost.Mean < bestPlan.Cost.Mean {
					bestPlan = p
				}
			}
			for _, c := range p.BaseTableCardInfo {
				if _, ok := baseTableBookMap[c.OpInfo]; !ok {
					baseTableBookMap[c.OpInfo] = struct{}{}
					baseTableMetrics.Values = append(baseTableMetrics.Values, c.QError)
				}
			}
		}
		estRowsQError = append(estRowsQError, fmt.Sprintf("count:%d, median:%.1f, 90th:%.1f, 95th:%.1f, max:%.1f",
			len(baseTableMetrics.Values),
			baseTableMetrics.quantile(0.5),
			baseTableMetrics.quantile(0.9),
			baseTableMetrics.quantile(0.95),
			baseTableMetrics.quantile(1)))
		planSpaceCount := len(b.Plans)
		row := Row{
			QueryId:        b.QueryID,
			Query:          b.DefaultPlan.SQL,
			PlanSpaceCount: len(b.Plans),
			DefaultPlanId:  int(b.DefaultPlan.Plan),
			DefaultPlanDur: b.DefaultPlan.Cost.format(),
			BestPlanDur:    bestPlan.Cost.format(),
			OptimalPlan:    optimalPlan,
			Effectiveness:  float64(planSpaceCount-betterPlanCount) / float64(planSpaceCount),
			EstRowsQError:  estRowsQError,
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
