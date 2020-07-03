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
		results, err := scope.Step(round)
		step++
		if err != nil {
			return err
		}

		if results == nil {
			break
		}
		for _, result := range results.Plans {
			if result.Cost < results.Origin.Cost {
				same, err := exec.IsSamePlan(results.Origin.Sql, result.Sql)
				if err != nil {
					return err
				}
				if !same {
					hints, err := exec.GetHints(result.Sql)
					if err != nil {
						return err
					}
					defaultHints, err := exec.GetHints(result.Sql)
					if err != nil {
						return err
					}

					log.WithFields(log.Fields{
						"query":        results.Origin.Sql,
						"step":         step,
						"default plan": defaultHints.String(),
						"better plan":  hints.String(),
					}).Errorf(
						"choose wrong plan(%dms < %dms)",
						result.Cost.Milliseconds(),
						results.Origin.Cost.Milliseconds(),
					)
				}
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
