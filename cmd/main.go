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
	"log"

	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/generator"
	"github.com/chaos-mesh/horoscope/pkg/horoscope"
)

var (
	/// Config
	Dsn        = "root:@tcp(localhost:4000)/test?charset=utf8"
	Round uint = 1
)

func main() {
	exec, err := executor.NewExecutor(Dsn)
	if err != nil {
		panic(err.Error())
	}

	gen := generator.NewTpcHGenerator()
	scope := horoscope.NewHoroscope(exec, gen)

	for {
		results, err := scope.Step(Round)
		if err != nil {
			panic(err.Error())
		}

		if results == nil {
			break
		}
		for _, result := range results.Plans {
			if result.Cost < results.Origin.Cost {
				log.Fatalf(
					"Bad plan %dms < %dms:\n`%s`\n runs slower than\n`%s`\n",
					result.Cost.Milliseconds(),
					results.Origin.Cost.Milliseconds(),
					result.Sql,
					results.Origin.Cost,
				)
			}
		}
	}
}
