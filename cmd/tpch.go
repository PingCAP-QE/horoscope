package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/generator"
	"github.com/chaos-mesh/horoscope/pkg/horoscope"
)

var tpchCommand = &cli.Command{
	Name: "tpch",
	Action: func(context *cli.Context) error {
		gen = generator.NewTpcHGenerator()
		scope = horoscope.NewHoroscope(exec, gen)
		for {
			results, err := scope.Step(round)
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
		return nil
	},
}
