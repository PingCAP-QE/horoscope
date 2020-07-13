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
	"github.com/urfave/cli/v2"
	"strings"

	"github.com/chaos-mesh/horoscope/pkg/horoscope"
)

var (
	cardinalitor *horoscope.Cardinalitor
	columns      string
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
		},
	}
)

func testCard(*cli.Context) error {
	tableColumns := make(map[string][]string)
	for _, pair := range strings.Split(columns, ",") {
		values := strings.Split(pair, ":")
		table := values[0]
		column := values[1]
		if _, e := tableColumns[table]; !e {
			tableColumns[table] = make([]string, 0)
		}
		tableColumns[table] = append(tableColumns[table], column)
	}
	cardinalitor = horoscope.NewCardinalitor(Exec, 0, tableColumns)
	_, err := cardinalitor.Test()
	return err
}
