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

package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aclements/go-moremath/stats"
	"github.com/jedib0t/go-pretty/table"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/horoscope"
)

var (
	cardinalitor *horoscope.Cardinalitor
	columns      string
	typ          string
	timeout      time.Duration
	cardCommand  = &cli.Command{
		Name:   "card",
		Usage:  "test the cardinality estimations",
		Action: testCard,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "columns",
				Usage:       "collect cardinality estimation error, format of 't1:c1,t1:c2,t2:c1...'",
				Destination: &columns,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "type",
				Aliases:     []string{"t"},
				Usage:       "emq means exact match queries(A = x); rge means range(lb <= A < ub)",
				Value:       "emq",
				Destination: &typ,
			},
			&cli.DurationFlag{
				Name:        "timeout",
				Usage:       "the timeout of testing",
				Required:    false,
				Destination: &timeout,
			},
		},
	}
)

func testCard(*cli.Context) error {
	tableColumns := make(map[string][]string)
	for _, pair := range strings.Split(columns, ",") {
		values := strings.Split(pair, ".")
		table := values[0]
		column := values[1]
		if _, e := tableColumns[table]; !e {
			tableColumns[table] = make([]string, 0)
		}
		tableColumns[table] = append(tableColumns[table], column)
	}
	cardinalitor = horoscope.NewCardinalitor(Exec, tableColumns, horoscope.CardinalityQueryType(typ), timeout)
	result, err := cardinalitor.Test()
	if err != nil {
		return err
	}
	fmt.Print(renderCardTable(result))
	return nil
}

func renderCardTable(coll map[string]map[string]*horoscope.Metrics) string {
	t := table.NewWriter()
	t.AppendHeader(table.Row{"Table", "Column", "<= 2", "<= 3", "<= 4", "> 4", "max q-error"})
	for tableName, tbl := range coll {
		for columnName, m := range tbl {
			s := &stats.Sample{Xs: m.Values}
			s.Sort()
			c2, c3, c4 := countOf(s, 2), countOf(s, 3), countOf(s, 4)

			cb4, max := len(m.Values)-c4, s.Quantile(1)
			t.AppendRow(table.Row{tableName, columnName, c2, c3 - c2, c4 - c3, cb4, max})
		}
	}
	return t.Render()
}

func countOf(s *stats.Sample, f float64) int {
	return sort.Search(len(s.Xs), func(i int) bool {
		return s.Xs[i] > f
	})
}
