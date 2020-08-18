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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jedib0t/go-pretty/table"
)

// Table is used for displaying in output
type Table struct {
	Metric  string   `json:"metric"`
	Headers []string `json:"-"`
	Rows    []*Row   `json:"data"`
}

type Row struct {
	QueryId           string             `json:"queryID"`
	Query             string             `json:"query"`
	PlanSpaceCount    int                `json:"planSpaceSize"`
	DefaultPlanId     int                `json:"defaultPlanID"`
	DefaultPlanDur    float64            `json:"defaultPlanDur"`
	DefaultPlanDurDev float64            `json:"defaultPlanDurDev"`
	BestPlanDur       float64            `json:"bestPlanDur"`
	BestPlanDurDev    float64            `json:"bestPlanDurDev"`
	OptimalPlan       []string           `json:"optimalPlan"`
	Effectiveness     float64            `json:"effectiveness"`
	EstRowsQError     map[string]float64 `json:"estRowsQError"`
}

func (r *Row) toTableRows() table.Row {
	var row table.Row
	row = append(row, r.QueryId, r.PlanSpaceCount, fmt.Sprintf("%2d: %.1f ± %.1f%%", r.DefaultPlanId, r.DefaultPlanDur, r.DefaultPlanDurDev),
		fmt.Sprintf("%.1f ± %.1f%%", r.BestPlanDur, r.BestPlanDurDev),
		fmt.Sprintf("%.1f%%", r.Effectiveness*100), strings.Join(r.OptimalPlan, ","),
		fmt.Sprintf("count: %d, median: %.1f, 90th:%.1f, 95th:%.1f, max:%.1f", int(r.EstRowsQError["count"]), r.EstRowsQError["median"],
			r.EstRowsQError["90th"], r.EstRowsQError["95th"], r.EstRowsQError["max"]),
		r.Query)
	return row
}

type BenchCollection []*Benches

func (c *BenchCollection) Output(format string) error {
	switch format {
	case "table":
		fmt.Println(c.Table().String())
		return nil
	case "json":
		data, err := json.Marshal(c.Table())
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	default:
		return fmt.Errorf("unknown output format %s", format)
	}
}

func (c *BenchCollection) Table() Table {
	table := Table{Metric: "execution time", Headers: []string{"id", "#plan space", "default execution time", "best plan execution time", "effectiveness", "better optimal plans", "estRow q-error", "query"}}
	for _, b := range *c {
		defaultPlan, bestPlan, betterPlanCount, optimalPlan := &b.DefaultPlan, &b.DefaultPlan, 0, make([]string, 0)
		baseTableBookMap, baseTableMetrics := make(map[string]struct{}), Metrics{}
		for _, p := range b.Plans {
			if IsSubOptimal(defaultPlan, p) {
				betterPlanCount += 1
				optimalPlan = append(optimalPlan, fmt.Sprintf("#%d(%.1f%%)", p.Plan, 100*p.Cost.Mean/defaultPlan.Cost.Mean))
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
		planSpaceCount := len(b.Plans)
		row := Row{
			QueryId:           b.QueryID,
			Query:             b.DefaultPlan.SQL,
			PlanSpaceCount:    len(b.Plans),
			DefaultPlanId:     int(b.DefaultPlan.Plan),
			DefaultPlanDur:    b.DefaultPlan.Cost.Mean,
			DefaultPlanDurDev: b.DefaultPlan.Cost.Diff(),
			BestPlanDur:       bestPlan.Cost.Mean,
			BestPlanDurDev:    bestPlan.Cost.Diff(),
			OptimalPlan:       optimalPlan,
			Effectiveness:     float64(planSpaceCount-betterPlanCount) / float64(planSpaceCount),
			EstRowsQError: map[string]float64{
				"count": float64(len(baseTableMetrics.Values)),
				"50th":  baseTableMetrics.quantile(0.5),
				"90th":  baseTableMetrics.quantile(0.5),
				"95th":  baseTableMetrics.quantile(0.5),
				"max":   baseTableMetrics.quantile(1),
			},
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
