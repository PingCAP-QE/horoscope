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
	"github.com/chaos-mesh/horoscope/pkg/keymap"
)

var (
	generateOptions = &options.Generate
	genOptions      = &generateOptions.Generator
)

func genCommand() *cli.Command {
	return &cli.Command{
		Name:    "gen",
		Aliases: []string{"g"},
		Usage:   "Generate a dynamic bench scheme",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "mode",
				Aliases:     []string{"m"},
				Usage:       "mode of generator, `< bench | op-compose >`",
				Value:       generateOptions.Mode,
				Destination: &generateOptions.Mode,
			},
			&cli.IntFlag{
				Name:        "queries",
				Aliases:     []string{"q"},
				Usage:       "the number of queries",
				Value:       generateOptions.Queries,
				Destination: &generateOptions.Queries,
			},
			&cli.BoolFlag{
				Name:        "keymap",
				Aliases:     []string{"k"},
				Usage:       "enable keymap",
				Value:       genOptions.EnableKeyMap,
				Destination: &genOptions.EnableKeyMap,
			},
			&cli.BoolFlag{
				Name:        "key-only",
				Usage:       "only use key columns",
				Value:       genOptions.KeyOnly,
				Destination: &genOptions.KeyOnly,
			},
			&cli.IntFlag{
				Name:        "table-count",
				Aliases:     []string{"c"},
				Usage:       "the max number of tables",
				Value:       genOptions.MaxTables,
				Destination: &genOptions.MaxTables,
			},
			&cli.BoolFlag{
				Name:        "unstable-order",
				Usage:       "enable unstable order queries",
				Value:       genOptions.UnstableOrderBy,
				Destination: &genOptions.UnstableOrderBy,
			},
			&cli.IntFlag{
				Name:        "max-by-items",
				Usage:       "the max `number` of by-items, used by order-by and group-by",
				Value:       genOptions.MaxByItems,
				Destination: &genOptions.MaxByItems,
			},
			&cli.DurationFlag{
				Name:        "threshold",
				Aliases:     []string{"d"},
				Usage:       "minimum query execution `duration` threshold",
				Value:       genOptions.MinDurationThreshold,
				Destination: &genOptions.MinDurationThreshold,
			},
			&cli.IntFlag{
				Name:        "limit",
				Aliases:     []string{"l"},
				Usage:       "`limit` of each query",
				Value:       genOptions.Limit,
				Destination: &genOptions.Limit,
			},
			&cli.Float64Flag{
				Name:        "aggr-weight",
				Aliases:     []string{"aw"},
				Usage:       "`weight` of aggregate select statements; between 0.0 and 1.0",
				Value:       genOptions.AggregateWeight,
				Destination: &genOptions.AggregateWeight,
			},
			&cli.IntFlag{
				Name:        "weight",
				Aliases:     []string{"w"},
				Usage:       "weight of 'AND' operator in random",
				Value:       generateOptions.AndOpWeight,
				Destination: &generateOptions.AndOpWeight,
			},
		},
		Before: func(ctx *cli.Context) error {
			generator.SetAndOpWeight(generateOptions.AndOpWeight)
			return initTx(ctx)
		},
		Action: func(context *cli.Context) (err error) {
			var keymaps []keymap.KeyMap
			if genOptions.EnableKeyMap {
				keymaps, err = keymap.ParseFile(path.Join(mainOptions.Workload, KeymapFile))
				if err != nil {
					return
				}
			}
			gen := generator.NewGenerator(Database, Pool.Executor(), keymaps)
			plans := make([]string, 0, generateOptions.Queries)
			for len(plans) < generateOptions.Queries {
				var query string
				switch generateOptions.Mode {
				case "bench":
					query, err = gen.BenchStmt(*genOptions)
				case "op-compose":
					query, err = gen.ComposeStmt(*genOptions)
				default:
					err = fmt.Errorf("invalid mode: '%s'", generateOptions.Mode)
				}
				if err != nil {
					return err
				}
				log.WithField("query", query).Debug("new query generated, checking...")
				dur, err := getQueryRunDuration(Tx, query)
				if err != nil {
					return err
				}
				if dur < genOptions.MinDurationThreshold {
					log.Tracef("query duration %v is less than %v, so ignore it", dur, genOptions.MinDurationThreshold)
					continue
				}
				plans = append(plans, query)
			}

			err = ioutil.WriteFile(path.Join(mainOptions.Workload, PrepareFile), []byte(genPrepare(plans)), 0644)
			if err != nil {
				return
			}

			queriesDir := path.Join(mainOptions.Workload, QueriesDir)
			err = os.RemoveAll(queriesDir)
			if err != nil {
				return
			}
			err = os.Mkdir(queriesDir, 0755)
			if err != nil {
				return
			}
			for i, plan := range plans {
				err = ioutil.WriteFile(
					path.Join(queriesDir, fmt.Sprintf("%d.sql", i+1)),
					[]byte(fmt.Sprintf("%s;\n", plan)),
					0644,
				)
				if err != nil {
					return
				}
			}
			return
		},
	}
}

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
