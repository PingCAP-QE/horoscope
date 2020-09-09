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
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"os"

	"github.com/chaos-mesh/horoscope/pkg/database"
	"github.com/chaos-mesh/horoscope/pkg/executor"
)

const (
	PrepareFile = "prepare.sql"
	KeymapFile  = ".keymap"
	QueriesDir  = "queries"
	IndexesDir  = "indexes"
	SchemaFile  = "schema.sql"
	SliceDir    = "slices"
)

var (
	/// Config
	mainOptions = &options.Main

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
				Destination: &mainOptions.Dsn,
			},
			&cli.BoolFlag{
				Name:        "json",
				Aliases:     []string{"j"},
				Value:       false,
				Usage:       "format log with json formatter",
				Destination: &mainOptions.JsonFormatter,
			},
			&cli.StringFlag{
				Name:        "file",
				Aliases:     []string{"f"},
				Usage:       "set `FILE` to store log",
				Destination: &mainOptions.LogFile,
			},
			&cli.StringFlag{
				Name:        "verbose",
				Aliases:     []string{"v"},
				Value:       "info",
				Usage:       "set `LEVEL` of log: trace|debug|info|warn|error|fatal|panic",
				Destination: &mainOptions.Verbose,
			},
			&cli.UintFlag{
				Name:        "max-open-conns",
				Value:       100,
				Usage:       "the max `numbers` of connections",
				Destination: &mainOptions.Pool.MaxOpenConns,
			},
			&cli.UintFlag{
				Name:        "max-idle-conns",
				Value:       20,
				Usage:       "the max `numbers` of idle connections",
				Destination: &mainOptions.Pool.MaxIdleConns,
			},
			&cli.UintFlag{
				Name:        "max-lifetime",
				Value:       10,
				Usage:       "the max `seconds` of connections lifetime",
				Destination: &mainOptions.Pool.MaxLifeSeconds,
			},
		},
		Before: func(context *cli.Context) (err error) {
			if err = setupLogger(); err != nil {
				return
			}
			Pool, err = executor.NewPool(mainOptions.Dsn, &mainOptions.Pool)
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
	if mainOptions.JsonFormatter {
		log.SetFormatter(&log.JSONFormatter{})
	}

	level, err := log.ParseLevel(mainOptions.Verbose)
	if err != nil {
		return err
	}
	log.SetLevel(level)

	if mainOptions.LogFile != "" {
		file, err := os.Open(mainOptions.LogFile)
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
