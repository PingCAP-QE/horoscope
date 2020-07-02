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
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/generator"
	"github.com/chaos-mesh/horoscope/pkg/horoscope"
)

var queryCommand = &cli.Command{
	Name:    "query",
	Aliases: []string{"q"},
	Usage:   "Execute a query",
	Action: func(context *cli.Context) error {
		query := context.Args().Get(0)
		if query == "" {
			log.Fatal("Empty query")
		}
		scope := horoscope.NewHoroscope(exec, generator.BlankGenerator)
		dur, rows, err := scope.QueryWithTime(round, query)
		if err != nil {
			return err
		}

		log.WithField("rows", rows).Debugf("Complete query `%s`", query)
		log.WithField("duration", dur).Infof("Complete in %dms", dur.Milliseconds())
		return nil
	},
}
