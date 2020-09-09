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

	"github.com/chaos-mesh/horoscope/pkg/horoscope"
	"github.com/chaos-mesh/horoscope/pkg/loader"
)

var (
	benchOptions = &options.Bench
	benchCommand = &cli.Command{
		Name:   "bench",
		Usage:  "Bench the optimizer",
		Action: bench,
		Before: initTx,
		After:  rollback,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "prepare",
				Aliases:     []string{"p"},
				Usage:       "prepare before benching",
				Value:       benchOptions.NeedPrepare,
				Destination: &benchOptions.NeedPrepare,
			},
			&cli.UintFlag{
				Name:        "round",
				Aliases:     []string{"r"},
				Usage:       "execution `ROUND` of each query",
				Value:       benchOptions.Round,
				Destination: &benchOptions.Round,
			},
			&cli.StringFlag{
				Name:        "output-format",
				Aliases:     []string{"f"},
				Usage:       "specify the format of report, may be `table` or `json`",
				Value:       benchOptions.ReportFmt,
				Destination: &benchOptions.ReportFmt,
			},
			&cli.BoolFlag{
				Name:        "no-verify",
				Usage:       "dont't perform results verification",
				Value:       benchOptions.NoVerify,
				Destination: &benchOptions.NoVerify,
			},
			&cli.BoolFlag{
				Name:        "no-cardinality-error",
				Usage:       "collect cardinality estimation error",
				Value:       benchOptions.DisableCollectCardError,
				Destination: &benchOptions.DisableCollectCardError,
			},
		},
	}
)

func bench(*cli.Context) error {
	if benchOptions.NeedPrepare {
		if err := prepare(mainOptions.Workload); err != nil {
			return err
		}
	}
	newLoader, err := loader.LoadDir(mainOptions.Workload)
	if err != nil {
		return err
	}

	horo := horoscope.NewHoroscope(Tx, newLoader, !benchOptions.DisableCollectCardError)
	collection := make(horoscope.BenchCollection, 0)
	for {
		benches, err := horo.Next(benchOptions.Round, !benchOptions.NoVerify)
		if err != nil {
			if benches != nil {
				log.WithFields(log.Fields{
					"query id": benches.QueryID,
					"query":    benches.DefaultPlan.SQL,
					"err":      err.Error(),
				}).Warn("Occurs an error when benching the query")
			} else {
				log.WithFields(log.Fields{
					"err": err.Error(),
				}).Warn("Occurs an error when benching one query")
			}
			if strings.Contains(err.Error(), "connection refused") ||
				strings.Contains(err.Error(), "invalid connection") {
				time.Sleep(2 * time.Minute)
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
	return collection.Output(benchOptions.ReportFmt)
}

func prepare(workloadDir string) error {
	log.WithFields(log.Fields{
		"workload dir": workloadDir,
	}).Info("preparing...")

	file := path.Join(workloadDir, PrepareFile)
	sqls, err := ioutil.ReadFile(file)
	if err != nil {
		return fmt.Errorf("read file %s error: %v", file, err)
	}
	_, err = Pool.Executor().Exec(string(sqls))
	if err != nil {
		return fmt.Errorf("exec prepare statements error: %v", err)
	}
	log.WithFields(log.Fields{
		"workload dir": workloadDir,
	}).Info("preparing finished")
	return nil
}
