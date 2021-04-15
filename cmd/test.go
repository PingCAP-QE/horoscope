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
	"path"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/horoscope"
	"github.com/chaos-mesh/horoscope/pkg/loader"
)

var (
	testOptions = &options.Test

	differentialPools = make([]executor.Pool, 0)
)

func testCommand() *cli.Command {
	differentialDsn := cli.NewStringSlice(testOptions.DifferentialDsn...)
	return &cli.Command{
		Name:   "test",
		Usage:  "test the optimizer",
		Action: test,
		Before: func(context *cli.Context) error {
			if err := testOptions.Validate(); err != nil {
				return err
			}
			if err := initDifferentialDsn(differentialDsn.Value()); err != nil {
				log.Warn(err.Error())
			}
			return initTx(context)
		},
		After: func(ctx *cli.Context) error {
			testOptions.DifferentialDsn = differentialDsn.Value()
			return rollback(ctx)
		},
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "prepare",
				Aliases:     []string{"p"},
				Usage:       "prepare before test",
				Value:       testOptions.NeedPrepare,
				Destination: &testOptions.NeedPrepare,
			},
			&cli.UintFlag{
				Name:        "round",
				Aliases:     []string{"r"},
				Usage:       "execution `ROUND` of each query",
				Value:       testOptions.Round,
				Destination: &testOptions.Round,
			},
			&cli.Uint64Flag{
				Name:        "max-plans",
				Usage:       "the max `numbers` of plans",
				Value:       testOptions.MaxPlans,
				Destination: &testOptions.MaxPlans,
			},
			&cli.StringFlag{
				Name:        "output-format",
				Aliases:     []string{"f"},
				Usage:       "specify the format of report, may be `table` or `json`",
				Value:       testOptions.ReportFmt,
				Destination: &testOptions.ReportFmt,
			},
			&cli.BoolFlag{
				Name:        "no-verify",
				Aliases:     []string{"nv"},
				Usage:       "don't perform results verification",
				Value:       testOptions.NoVerify,
				Destination: &testOptions.NoVerify,
			},
			&cli.BoolFlag{
				Name:        "ignore-server-error",
				Usage:       "ignore server error",
				Value:       testOptions.IgnoreServerError,
				Destination: &testOptions.IgnoreServerError,
			},
			&cli.StringSliceFlag{
				Name:        "differential-dsn",
				Aliases:     []string{"dd"},
				Usage:       "other `DSNs` for differential test",
				Value:       differentialDsn,
				Destination: differentialDsn,
			},
			&cli.BoolFlag{
				Name:        "no-bench",
				Aliases:     []string{"nb"},
				Usage:       "don't output benchmark report",
				Value:       testOptions.NoBench,
				Destination: &testOptions.NoBench,
			},
			&cli.BoolFlag{
				Name:        "no-cardinality-error",
				Usage:       "collect cardinality estimation error",
				Value:       testOptions.DisableCollectCardError,
				Destination: &testOptions.DisableCollectCardError,
			},
		},
	}
}

func test(*cli.Context) error {
	if testOptions.NeedPrepare {
		if err := prepare(mainOptions.Workload, Tx); err != nil {
			return err
		}
	}
	newLoader, err := loader.LoadDir(path.Join(mainOptions.Workload, QueriesDir))
	if err != nil {
		return err
	}

	horo := horoscope.NewHoroscope(Pool, differentialPools, newLoader, !testOptions.DisableCollectCardError)
	collection := make(horoscope.BenchCollection, 0)
	for {
		benches, err := horo.Next(testOptions.Round, testOptions.MaxPlans, !testOptions.NoVerify, testOptions.IgnoreServerError)
		if err != nil {
			if benches != nil {
				log.WithFields(log.Fields{
					"query id": benches.QueryID,
					"query":    benches.DefaultPlan.SQL,
					"err":      err.Error(),
				}).Warn("Occurs an error when testing the query")
			} else {
				log.WithFields(log.Fields{
					"err": err.Error(),
				}).Warn("Occurs an error when testing one query")
			}
			if strings.Contains(err.Error(), "connection refused") ||
				strings.Contains(err.Error(), "invalid connection") {
				time.Sleep(2 * time.Minute)
				continue
			}
			if _, serverError := err.(horoscope.ServerError); serverError && testOptions.IgnoreServerError {
				continue
			}
			return err
		}
		if benches == nil {
			break
		}
		log.WithFields(log.Fields{
			"query id":      benches.QueryID,
			"query":         benches.DefaultPlan.SQL,
			"default plan":  benches.DefaultPlan.Plan,
			"default hints": benches.DefaultPlan.Hints,
			"cost":          fmt.Sprintf("%v", benches.DefaultPlan.Cost.Values),
			"plan size":     len(benches.Plans),
		}).Info("Complete a step")
		log.WithFields(log.Fields{
			"query id":    benches.QueryID,
			"explanation": benches.DefaultPlan.Explanation.String(),
		}).Debug("Default explanation")
		collection = append(collection, benches)
		if !testOptions.NoBench {
			for _, plan := range benches.Plans {
				if horoscope.IsSubOptimal(&benches.DefaultPlan, plan) && plan.Plan != benches.DefaultPlan.Plan {
					log.WithFields(log.Fields{
						"query id":     benches.QueryID,
						"better plan":  plan.Plan,
						"better hints": plan.Hints,
					}).Errorf(
						"may choose a suboptimal plan(%0.2fms < %0.2fms)",
						plan.Cost.Mean,
						benches.DefaultPlan.Cost.Mean,
					)
					log.WithFields(log.Fields{
						"query id":           benches.QueryID,
						"better explanation": plan.Explanation.String(),
					}).Debug("Better explanation")
				}
			}
		}
	}

	if !testOptions.NoBench {
		return collection.Output(testOptions.ReportFmt)
	}

	return nil
}

func prepare(workloadDir string, exec executor.Executor) error {
	log.WithFields(log.Fields{
		"workload dir": workloadDir,
	}).Info("preparing...")

	file := path.Join(workloadDir, PrepareFile)
	sqls, err := ioutil.ReadFile(file)
	if err != nil {
		return fmt.Errorf("read file %s error: %v", file, err)
	}
	_, err = exec.Exec(string(sqls))
	if err != nil {
		return fmt.Errorf("exec prepare statements error: %v", err)
	}
	log.WithFields(log.Fields{
		"workload dir": workloadDir,
	}).Info("preparing finished")
	return nil
}

func initDifferentialDsn(dsns []string) error {
	for _, dsn := range dsns {
		pool, err := executor.NewPool(dsn, &mainOptions.Pool)
		if err != nil {
			return fmt.Errorf("fail to open pool on dsn '%s': %s", dsn, err.Error())
		}
		differentialPools = append(differentialPools, pool)
	}
	return nil
}
