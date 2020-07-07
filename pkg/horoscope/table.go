package horoscope

import (
	"fmt"
	"github.com/jedib0t/go-pretty/table"
	"golang.org/x/perf/benchstat"
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
	Effectiveness  float64
}

func (r *Row) toTableRows() table.Row {
	var row table.Row
	row = append(row, r.QueryId, r.PlanSpaceCount, r.DefaultPlanDur, r.BestPlanDur, fmt.Sprintf("%.1f%%", r.Effectiveness*100), r.Query)
	return row
}

type Collection []*BenchResults

func (c *Collection) Table() Table {
	alpha := 0.05
	table := Table{Metric: "execution time", Headers: []string{"id", "#plan space", "default execution time", "best plan execution time", "effectiveness", "query"}}
	for _, b := range *c {
		defaultPlanDurs := b.Origin.Durations
		bestPlan := b.Origin
		betterPlanCount := 0
		for _, p := range b.Plans {
			if p.Durations.Mean < defaultPlanDurs.Mean {
				currentPlanDurs := p.Durations
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
					if currentPlanDurs.Mean < bestPlan.Durations.Mean {
						bestPlan = p
					}
				}
			}
		}
		planSpaceCount := len(b.Plans)
		row := Row{
			QueryId:        b.QueryID,
			Query:          b.Origin.Sql,
			PlanSpaceCount: len(b.Plans),
			DefaultPlanDur: b.Origin.Durations.format(),
			BestPlanDur:    bestPlan.Durations.format(),
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
