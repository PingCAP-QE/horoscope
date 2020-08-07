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
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/generator"
)

var (
	queryNums   int
	andOpWeight int
	genOptions  generator.Options
	prepareFile = path.Join(dynWorkload, "prepare.sql")
	queriesDir  = path.Join(dynWorkload, "queries")

	genCommand = &cli.Command{
		Name:    "gen",
		Aliases: []string{"g"},
		Usage:   "Generate a dynamic bench scheme",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:        "queries",
				Aliases:     []string{"q"},
				Usage:       "the number of queries",
				Value:       20,
				Destination: &queryNums,
			},
			&cli.IntFlag{
				Name:        "table-count",
				Aliases:     []string{"c"},
				Usage:       "the max number of tables",
				Value:       1,
				Destination: &genOptions.MaxTables,
			},
			&cli.DurationFlag{
				Name:        "threshold",
				Aliases:     []string{"d"},
				Usage:       "minimum query execution `duration` threshold",
				Value:       10 * time.Millisecond,
				Destination: &genOptions.MinDurationThreshold,
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
		Before: func(ctx *cli.Context) error {
			generator.SetAndOpWeight(andOpWeight)
			return initTx(ctx)
		},
		Action: func(context *cli.Context) error {
			gen := generator.NewGenerator(Database, Pool.Executor())
			plans := make([]string, 0, queryNums)
			for len(plans) < queryNums {
				stmt, err := gen.SelectStmt(genOptions)
				if err != nil {
					return err
				}
				dur, err := getQueryRunDuration(Tx, stmt)
				if err != nil {
					return err
				}
				if dur < genOptions.MinDurationThreshold {
					log.Infof("query duration %v is less than %v, so ignore it", dur, genOptions.MinDurationThreshold)
					continue
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

func getQueryRunDuration(tx executor.Executor, query string) (time.Duration, error) {
	start := time.Now()
	_, err := tx.Query(query)
	if err != nil {
		return 0, err
	}
	return time.Now().Sub(start), nil
}
