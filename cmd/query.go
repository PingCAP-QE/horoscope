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
	"bufio"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/generator"
	"github.com/chaos-mesh/horoscope/pkg/horoscope"
)

var (
	planID int64 = 0

	queryCommand = &cli.Command{
		Name:    "query",
		Aliases: []string{"q"},
		Usage:   "Execute a query",
		Flags: []cli.Flag{
			&cli.Int64Flag{
				Name:        "plan",
				Aliases:     []string{"p"},
				Usage:       "Use plan by `ID`",
				Destination: &planID,
			},
		},
		Action: func(context *cli.Context) error {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("tidb> ")
			sql, err := reader.ReadString('\n')
			if err != nil {
				return err
			}

			query, err := Parse(sql)
			if err != nil {
				return err
			}

			tp, hints, err := horoscope.AnalyzeQuery(query, sql)
			if err != nil {
				return err
			}

			plan, err := horoscope.Plan(query, hints, planID)
			if err != nil {
				return err
			}

			scope := horoscope.NewHoroscope(Exec, generator.BlankGenerator)
			dur, rows, err := scope.RunSQLWithTime(round, plan, tp)
			if err != nil {
				return err
			}

			log.WithField("query", plan).Debug("Complete query")
			fmt.Printf("%s\nComplete in %vms", rows[0].String(), dur.Values)
			return nil
		},
	}
)
