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

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/horoscope"
	"github.com/chaos-mesh/horoscope/pkg/loader"
)

var (
	needPrepare            bool
	enableCollectCardError bool
	needVerify             bool
	workloadDir            string
	benchCommand           = &cli.Command{
		Name:   "bench",
		Usage:  "Bench the optimizer",
		Action: bench,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "prepare",
				Aliases:     []string{"p"},
				Usage:       "prepare before benching",
				Value:       false,
				Destination: &needPrepare,
			},
			&cli.BoolFlag{
				Name:        "verify",
				Aliases:     []string{"v"},
				Usage:       "need results verification",
				Value:       false,
				Destination: &needVerify,
			},
			&cli.BoolFlag{
				Name:        "c",
				Usage:       "collect cardinality estimation error",
				Value:       true,
				Destination: &enableCollectCardError,
			},
			&cli.StringFlag{
				Name:        "workload",
				Aliases:     []string{"w"},
				Usage:       "specify the workload `DIR`",
				Value:       "benchmark/dyn",
				Destination: &workloadDir,
			},
		},
	}
)

func bench(*cli.Context) error {
	if needPrepare {
		if err := prepare(workloadDir); err != nil {
			return err
		}
	}
	newLoader, err := loader.LoadDir(workloadDir)
	if err != nil {
		return err
	}

	horo := horoscope.NewHoroscope(Exec, newLoader, enableCollectCardError)
	var collection horoscope.BenchCollection
	for {
		benches, err := horo.Next(round, needVerify)
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
			continue
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
	fmt.Print(collection.Table().String())
	return nil
}

func prepare(workloadDir string) error {
	log.WithFields(log.Fields{
		"workload dir": workloadDir,
	}).Info("preparing...")

	file := fmt.Sprintf("%s/prepare.sql", workloadDir)
	sqls, err := ioutil.ReadFile(file)
	if err != nil {
		return fmt.Errorf("read file %s error: %v", file, err)
	}
	_, err = Exec.Exec(string(sqls))
	if err != nil {
		return fmt.Errorf("exec prepare statements error: %v", err)
	}
	log.WithFields(log.Fields{
		"workload dir": workloadDir,
	}).Info("preparing finished")
	return nil
}
