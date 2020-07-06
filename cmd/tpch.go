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

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/generator"
	"github.com/chaos-mesh/horoscope/pkg/horoscope"
)

var (
	scope       *horoscope.Horoscope
	tables      = []string{"lineitem", "orders", "part", "partsupp", "supplier", "customer", "region", "nation"}
	needPrepare bool
	tpchCommand = &cli.Command{
		Name:   "tpch",
		Usage:  "Test DSN with TPCH",
		Action: tpch,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "prepare",
				Aliases:     []string{"p"},
				Usage:       "Analyze Table and load stats into memory",
				Value:       false,
				Destination: &needPrepare,
			},
		},
	}
)

func tpch(*cli.Context) error {
	if needPrepare {
		if err := tpchPrepare(); err != nil {
			return err
		}
	}
	gen := generator.NewTpcHGenerator()
	scope = horoscope.NewHoroscope(exec, gen)
	step := 0
	for {
		benches, err := scope.Step(round)
		if err != nil {
			return err
		}

		if benches == nil {
			break
		}
		step++

		log.WithFields(log.Fields{
			"query":         benches.SQL,
			"step":          step,
			"default plan":  benches.DefaultPlan,
			"default hints": benches.Hints,
			"cost":          fmt.Sprintf("%dms", benches.Cost.Milliseconds()),
			"plan size":     len(benches.Plans),
		}).Info("Complete a step")
		log.WithFields(log.Fields{
			"step":        step,
			"explanation": benches.Explanation.String(),
		}).Debug("Default explanation")
		for _, plan := range benches.Plans {
			if plan.Cost < benches.Cost && plan.Plan != benches.DefaultPlan {
				log.WithFields(log.Fields{
					"step":         step,
					"better plan":  plan.Plan,
					"better hints": plan.Hints,
				}).Errorf(
					"choose wrong plan(%dms < %dms)",
					plan.Cost.Milliseconds(),
					benches.Cost.Milliseconds(),
				)
				log.WithFields(log.Fields{
					"step":               step,
					"better explanation": plan.Explanation.String(),
				}).Debug("Better explanation")
			}
		}
	}
	return nil
}

func tpchPrepare() error {
	for _, table := range tables {
		log.Infof("Analyzing table %s...", table)
		_, err := exec.Exec(fmt.Sprintf("analyze table %s", table), 1)
		if err != nil {
			return err
		}
	}
	log.Infof("Warming up...")
	gen := generator.NewTpcHGenerator()
	for {
		queryNode := gen.Query()
		if queryNode == nil {
			break
		}

		query, err := horoscope.BufferOut(queryNode)
		log.WithField("query", query).Debug("warm up query")
		if err != nil {
			return err
		}
		_, err = exec.Query(query, 1)
		if err != nil {
			return err
		}
	}
	return nil
}
