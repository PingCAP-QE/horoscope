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
		for _, result := range append(results.Plans, results.Origin) {
			log.Printf("SQL(%s), Round: %d, Cost: %d us", result.Sql, result.Round, result.Cost.Microseconds())
		}
	}
}
