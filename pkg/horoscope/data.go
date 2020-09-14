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
	Query       ast.StmtNode
	Type        QueryType
	Round       uint
	DefaultPlan Bench
	Plans       []*Bench
}

type Bench struct {
	Plan        uint64
	SQL         string
	Hints       executor.Hints
	Explanation executor.Rows
	Cost        *Metrics
	// use q-error to calc the cardinality error
	BaseTableCardInfo []*executor.CardinalityInfo
	JoinTableCardInfo []*executor.CardinalityInfo
}

type Metrics benchstat.Metrics

func (m *Metrics) format() string {
	mean, diff := m.Mean, m.Diff()
	return fmt.Sprintf("%.1fms ± %.1f%%", mean, diff*100)
}

func (m *Metrics) Diff() float64 {
	if m.Mean == 0 || m.Max == 0 {
		return 0
	}
	diff := math.Max(1-m.Min/m.Max,
		m.Max/m.Min-1)
	return diff * 100
}

// computeStats updates the derived statistics in d from the raw
// samples in d.Values.
func (m *Metrics) computeStats() {
	var value []float64
	var rValue []float64
	for _, v := range m.Values {
		value = append(value, v)
	}
	values := stats.Sample{Xs: value}
	q1, q3 := values.Quantile(0.25), values.Quantile(0.75)
	lo, hi := q1-1.5*(q3-q1), q3+1.5*(q3-q1)
	for _, value := range value {
		if lo <= value && value <= hi {
			rValue = append(rValue, value)
			m.RValues = append(m.RValues, value)
		}
	}
	m.Min, m.Max = stats.Bounds(value)
	m.Mean = stats.Mean(rValue)
}

func (m *Metrics) quantile(q float64) float64 {
	values := stats.Sample{Xs: m.Values}
	return values.Quantile(q)
}
