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
	"io/ioutil"
	"os"
	"path"

	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/generator"
)

var (
	planNums       int
	andOpWeight    int
	genOptions     generator.Options
	outputWorkload = "benchmark/dyn"
	prepareFile    = path.Join(outputWorkload, "prepare.sql")
	queriesDir     = path.Join(outputWorkload, "queries")

	genCommand = &cli.Command{
		Name:    "gen",
		Aliases: []string{"g"},
		Usage:   "Generate a dynamic bench scheme",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:        "plans",
				Aliases:     []string{"p"},
				Usage:       "the `numbers` of plans",
				Value:       20,
				Destination: &planNums,
			},
			&cli.IntFlag{
				Name:        "tables",
				Aliases:     []string{"t"},
				Usage:       "the max `numbers` of tables",
				Value:       3,
				Destination: &genOptions.MaxTables,
			},
			&cli.IntFlag{
				Name:        "limit",
				Aliases:     []string{"l"},
				Usage:       "`limit` of each query",
				Value:       100,
				Destination: &genOptions.Limit,
			},
			&cli.IntFlag{
				Name:        "weight",
				Aliases:     []string{"w"},
				Usage:       "weight of 'AND' operator in random",
				Value:       3,
				Destination: &andOpWeight,
			},
		},
		Before: func(*cli.Context) error {
			generator.SetAndOpWeight(andOpWeight)
			return nil
		},
		Action: func(context *cli.Context) error {
			gen := generator.NewGenerator(Database, Exec)
			plans := make([]string, 0, planNums)
			for i := 0; i < planNums; i++ {
				stmt, err := gen.SelectStmt(genOptions)
				if err != nil {
					return err
				}
				plans = append(plans, stmt)
			}
			err := ioutil.WriteFile(prepareFile, []byte(genPrepare(plans)), 0644)
			if err != nil {
				return err
			}
			err = os.RemoveAll(queriesDir)
			if err != nil {
				return err
			}
			err = os.Mkdir(queriesDir, 0755)
			if err != nil {
				return err
			}
			for i, plan := range plans {
				err := ioutil.WriteFile(
					path.Join(queriesDir, fmt.Sprintf("%d.sql", i+1)),
					[]byte(fmt.Sprintf("%s;\n", plan)),
					0644,
				)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
)

func genPrepare(plans []string) string {
	prepare := ""
	for tableName := range Database.BaseTables {
		prepare += fmt.Sprintf("ANALYZE TABLE %s;\n", tableName)
	}
	for _, plan := range plans {
		prepare += fmt.Sprintf("EXPLAIN %s;\n", plan)
	}
	return prepare
}
