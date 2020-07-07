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
	"math"

	"github.com/aclements/go-moremath/stats"
	"github.com/pingcap/parser/ast"
	"golang.org/x/perf/benchstat"

	"github.com/chaos-mesh/horoscope/pkg/executor"
)

type Benches struct {
	QueryID     string
	SQL         string
	Query       ast.StmtNode
	Type        QueryType
	Round       uint
	Hints       executor.Hints
	Cost        *Durations
	DefaultPlan int64
	Explanation executor.Rows
	Plans       []*Bench
}

type Bench struct {
	Plan        int64
	SQL         string
	Hints       executor.Hints
	Explanation executor.Rows
	Cost        *Durations
}

type Durations benchstat.Metrics

func (d *Durations) format() string {
	mean, diff := d.formatMean(), d.formatDiff()
	return fmt.Sprintf("%s ±%3s", mean, diff)
}

func (d *Durations) formatMean() string {
	mean := d.Mean
	return fmt.Sprintf("%.1fms", mean)
}

func (d *Durations) formatDiff() string {
	if d.Mean == 0 || d.Max == 0 {
		return ""
	}
	diff := math.Max(1-d.Min/d.Max,
		d.Max/d.Min-1)
	return fmt.Sprintf("%.0f%%", diff*100)
}

// computeStats updates the derived statistics in d from the raw
// samples in d.Values.
func (d *Durations) computeStats() {
	var value []float64
	var rValue []float64
	for _, v := range d.Values {
		value = append(value, v)
	}
	values := stats.Sample{Xs: value}
	q1, q3 := values.Quantile(0.25), values.Quantile(0.75)
	lo, hi := q1-1.5*(q3-q1), q3+1.5*(q3-q1)
	for _, value := range value {
		if lo <= value && value <= hi {
			rValue = append(rValue, value)
			d.RValues = append(d.RValues, value)
		}
	}
	d.Min, d.Max = stats.Bounds(value)
	d.Mean = stats.Mean(rValue)
}
