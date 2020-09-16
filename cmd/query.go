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

	"github.com/chaos-mesh/horoscope/pkg/horoscope"
	"github.com/chaos-mesh/horoscope/pkg/loader"
)

func queryCommand() *cli.Command {
	return &cli.Command{
		Name:    "query",
		Aliases: []string{"q"},
		Usage:   "Execute a query",
		Flags: []cli.Flag{
			&cli.Uint64Flag{
				Name:        "plan",
				Aliases:     []string{"p"},
				Usage:       "use plan by `ID`",
				Value:       options.Query.PlanID,
				Destination: &options.Query.PlanID,
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

			plan, err := horoscope.Plan(query, hints, int64(options.Query.PlanID))
			if err != nil {
				return err
			}

			horo := horoscope.NewHoroscope(Pool.Executor(), nil, loader.NoopLoader{}, true)
			dur, rows, err := horo.RunSQLWithTime(1, plan, tp)
			if err != nil {
				return err
			}

			log.WithField("query", plan).Debug("Complete query")
			fmt.Printf("%s\nComplete in %vms", rows[0].String(), dur.Values)
			return nil
		},
	}
}
