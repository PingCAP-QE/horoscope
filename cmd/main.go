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
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/executor"
)

var (
	/// Config
	dsn           string
	round         uint
	jsonFormatter bool
	logFile       string
	verbose       string

	/// components
	exec executor.Executor
)

func main() {
	app := &cli.App{
		Name:  "horoscope",
		Usage: "An optimizer inspector for DBMS",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "dsn",
				Aliases:     []string{"d"},
				Value:       "root:@tcp(localhost:4000)/test?charset=utf8",
				Usage:       "`DSN` of target db",
				Destination: &dsn,
			},
			&cli.UintFlag{
				Name:        "round",
				Aliases:     []string{"r"},
				Value:       1,
				Usage:       "Execution `ROUND` of each query",
				Destination: &round,
			},
			&cli.BoolFlag{
				Name:        "json",
				Aliases:     []string{"j"},
				Value:       false,
				Usage:       "Format log with json formatter",
				Destination: &jsonFormatter,
			},
			&cli.StringFlag{
				Name:        "file",
				Aliases:     []string{"f"},
				Usage:       "`FILE` to store log",
				Destination: &logFile,
			},
			&cli.StringFlag{
				Name:        "verbose",
				Aliases:     []string{"v"},
				Value:       "info",
				Usage:       "`LEVEL` of log: trace|debug|info|warn|error|fatal|panic",
				Destination: &verbose,
			},
		},
		Before: func(context *cli.Context) (err error) {
			if err = setupLogger(); err != nil {
				return
			}
			exec, err = executor.NewExecutor(dsn)
			return
		},
		Commands: cli.Commands{
			tpchCommand,
			queryCommand,
			explainCommand,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func setupLogger() error {
	if jsonFormatter {
		log.SetFormatter(&log.JSONFormatter{})
	}

	level, err := log.ParseLevel(verbose)
	if err != nil {
		return err
	}
	log.SetLevel(level)

	if logFile != "" {
		file, err := os.Open(logFile)
		if err != nil {
			return err
		}
		log.SetOutput(file)
	}

	return nil
}
