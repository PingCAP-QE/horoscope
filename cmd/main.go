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
	"os"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/database"
	"github.com/chaos-mesh/horoscope/pkg/executor"
)

const (
	dynWorkload = "benchmark/dyn"
)

var (
	/// Config
	dsn           string
	round         uint
	jsonFormatter bool
	logFile       string
	verbose       string
	poolOptions   executor.PoolOptions

	/// pre initialized components
	Pool     executor.Pool
	Database *database.Database

	/// needs initialized by subcommand
	Tx executor.Transaction
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
				Usage:       "set `DSN` of target db",
				Destination: &dsn,
			},
			&cli.UintFlag{
				Name:        "round",
				Aliases:     []string{"r"},
				Value:       1,
				Usage:       "execution `ROUND` of each query",
				Destination: &round,
			},
			&cli.BoolFlag{
				Name:        "json",
				Aliases:     []string{"j"},
				Value:       false,
				Usage:       "format log with json formatter",
				Destination: &jsonFormatter,
			},
			&cli.StringFlag{
				Name:        "file",
				Aliases:     []string{"f"},
				Usage:       "set `FILE` to store log",
				Destination: &logFile,
			},
			&cli.StringFlag{
				Name:        "verbose",
				Aliases:     []string{"v"},
				Value:       "info",
				Usage:       "set `LEVEL` of log: trace|debug|info|warn|error|fatal|panic",
				Destination: &verbose,
			},
			&cli.UintFlag{
				Name:        "max-open-conns",
				Value:       100,
				Usage:       "the max `numbers` of connections",
				Destination: &poolOptions.MaxOpenConns,
			},
			&cli.UintFlag{
				Name:        "max-idle-conns",
				Value:       20,
				Usage:       "the max `numbers` of idle connections",
				Destination: &poolOptions.MaxIdleConns,
			},
			&cli.UintFlag{
				Name:        "max-lifetime",
				Value:       10,
				Usage:       "the max `seconds` of connections lifetime",
				Destination: &poolOptions.MaxLifeSeconds,
			},
		},
		Before: func(context *cli.Context) (err error) {
			if err = setupLogger(); err != nil {
				return
			}
			Pool, err = executor.NewPool(dsn, &poolOptions)
			if err != nil {
				return
			}

			Database, err = InitDatabase(Pool.Executor())
			return
		},
		Commands: cli.Commands{
			benchCommand,
			genCommand,
			queryCommand,
			hintCommand,
			explainCommand,
			infoCommand,
			indexCommand,
			cardCommand,
			splitCommand,
			loadCommand,
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

func InitDatabase(exec executor.Executor) (db *database.Database, err error) {
	dbName, err := exec.Query("SELECT DATABASE()")
	if err != nil {
		return
	}
	tables, err := exec.Query("SHOW FULL TABLES WHERE TABLE_TYPE='BASE TABLE'")
	if err != nil {
		return
	}
	db, err = database.LoadDatabase(dbName, tables)
	if err != nil {
		return
	}
	for name, table := range db.BaseTables {
		var columns executor.Rows
		columns, err = exec.Query(fmt.Sprintf("DESC %s", name))
		if err != nil {
			return
		}
		err = table.LoadColumns(columns)
		if err != nil {
			return
		}
	}
	return
}

func initTx(*cli.Context) (err error) {
	Tx, err = Pool.Transaction()
	return err
}

func commit(*cli.Context) error {
	return Tx.Rollback()
}

func rollback(*cli.Context) error {
	return Tx.Rollback()
}
