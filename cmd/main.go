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
	"flag"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/generator"
	"github.com/chaos-mesh/horoscope/pkg/horoscope"
)

var (
	/// Config
	dsn           = flag.String("d", "root:@tcp(localhost:4000)/test?charset=utf8", "dsn of target db for testing")
	round         = flag.Uint("r", 1, "execution rounds of each query")
	jsonFormatter = flag.Bool("j", true, "format log json formatter")
	logFile       = flag.String("f", "", "path of file to store log")
	verbose       = flag.Bool("v", false, "set log level to info")
	verbosePlus   = flag.Bool("vv", false, "set log level to debug")
)

func main() {
	flag.Parse()
	setupLogger()

	exec, err := executor.NewExecutor(*dsn)
	if err != nil {
		panic(err.Error())
	}

	gen := generator.NewTpcHGenerator()
	scope := horoscope.NewHoroscope(exec, gen)

	for {
		results, err := scope.Step(*round)
		if err != nil {
			panic(err.Error())
		}

		if results == nil {
			break
		}
		for _, result := range results.Plans {
			if result.Cost < results.Origin.Cost {
				same, err := exec.IsSamePlan(results.Origin.Sql, result.Sql)
				if err != nil {
					panic(err.Error())
				}
				if !same {
					log.WithFields(log.Fields{
						"query":       results.Origin.Sql,
						"better plan": result.Sql,
					}).Errorf(
						"choose wrong plan(%dms < %dms)",
						result.Cost.Milliseconds(),
						results.Origin.Cost.Milliseconds(),
					)
				}
			}
		}
	}
}

func setupLogger() {
	log.SetLevel(log.WarnLevel)
	if *jsonFormatter {
		log.SetFormatter(&log.JSONFormatter{})
	}

	if *verbose {
		log.SetLevel(log.InfoLevel)
	}

	if *verbosePlus {
		log.SetLevel(log.DebugLevel)
	}

	if *logFile != "" {
		file, err := os.Open(*logFile)
		if err != nil {
			log.WithFields(log.Fields{
				"err":  err.Error(),
				"path": *logFile,
			}).Fatalln("fail to open file")
		}
		log.SetOutput(file)
	}
}
