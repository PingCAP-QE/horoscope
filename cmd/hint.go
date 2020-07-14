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

	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/horoscope"
)

var hintCommand = &cli.Command{
	Name:    "hint",
	Aliases: []string{"H"},
	Usage:   "Explain hint of a query",
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:        "plan",
			Aliases:     []string{"p"},
			Usage:       "use plan by `ID`",
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

		_, hints, err := horoscope.AnalyzeQuery(query, sql)
		if err != nil {
			return err
		}

		plan, err := horoscope.Plan(query, hints, planID)
		if err != nil {
			return err
		}

		explainHints, err := Exec.GetHints(plan)
		if err != nil {
			return err
		}
		fmt.Println(explainHints.String())
		return nil
	},
}
