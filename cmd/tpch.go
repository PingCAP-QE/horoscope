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
	var collection horoscope.Collection
	for {
		results, err := scope.Step(round)
		if err != nil {
			return err
		}
		if results == nil {
			break
		}
		collection = append(collection, results)
		for _, result := range results.Plans {
			if result.Durations.Mean < results.Origin.Durations.Mean {
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
						"query id":     results.QueryID,
						"query":        results.Origin.Sql,
						"default plan": defaultHints.String(),
						"better plan":  hints.String(),
					}).Errorf(
						"may choose a wrong default plan(%.2fms < %.2fms)",
						result.Durations.Mean,
						results.Origin.Durations.Mean,
					)
				}
			}
		}
	}
	log.Infof(collection.Table().String())
	return nil
}

func tpchPrepare() error {
	for _, table := range tables {
		log.Infof("Analyzing table %s...", table)
		_, err := exec.Exec(fmt.Sprintf("analyze table %s", table))
		if err != nil {
			return err
		}
	}
	log.Infof("Warming up...")
	gen := generator.NewTpcHGenerator()
	for {
		_, queryNode := gen.Query()
		if queryNode == nil {
			break
		}

		query, err := horoscope.BufferOut(queryNode)
		log.WithField("query", query).Debug("warm up query")
		if err != nil {
			return err
		}
		_, err = exec.Query(query)
		if err != nil {
			return err
		}
	}
	return nil
}
