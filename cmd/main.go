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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

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
	Config      = "horo.json"
)

var (
	/// Config
	mainOptions = &options.Main

	saveOptions bool

	/// pre initialized components
	Pool     executor.Pool
	Database *database.Database

	/// needs initialized by subcommand
	Tx executor.Transaction
)

func main() {
	if pathExist(Config) {
		config, err := ioutil.ReadFile(Config)
		if err != nil {
			log.Fatal("fail to read config file `%s`: err: %s", Config, err.Error())
		}

		err = json.Unmarshal(config, &options)
		if err != nil {
			log.Fatal("wrong config file `%s`, invalid format: %s", Config, err.Error())
		}
	}

	app := &cli.App{
		Name:  "horoscope",
		Usage: "An optimizer inspector for DBMS",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "dsn",
				Aliases:     []string{"d"},
				Usage:       "set `DSN` of target db",
				Value:       mainOptions.Dsn,
				Destination: &mainOptions.Dsn,
			},
			&cli.StringFlag{
				Name:        "workload",
				Aliases:     []string{"w"},
				Usage:       "workload `DIR` of horo",
				Value:       mainOptions.Workload,
				Destination: &mainOptions.Workload,
			},
			&cli.BoolFlag{
				Name:        "json",
				Aliases:     []string{"j"},
				Usage:       "format log with json formatter",
				Value:       mainOptions.JsonFormatter,
				Destination: &mainOptions.JsonFormatter,
			},
			&cli.StringFlag{
				Name:        "file",
				Aliases:     []string{"f"},
				Usage:       "set `FILE` to store log",
				Value:       mainOptions.LogFile,
				Destination: &mainOptions.LogFile,
			},
			&cli.StringFlag{
				Name:        "verbose",
				Aliases:     []string{"v"},
				Usage:       "set `LEVEL` of log: trace|debug|info|warn|error|fatal|panic",
				Value:       mainOptions.Verbose,
				Destination: &mainOptions.Verbose,
			},
			&cli.UintFlag{
				Name:        "max-open-conns",
				Usage:       "the max `numbers` of connections",
				Value:       mainOptions.Pool.MaxOpenConns,
				Destination: &mainOptions.Pool.MaxOpenConns,
			},
			&cli.UintFlag{
				Name:        "max-idle-conns",
				Usage:       "the max `numbers` of idle connections",
				Value:       mainOptions.Pool.MaxIdleConns,
				Destination: &mainOptions.Pool.MaxIdleConns,
			},
			&cli.UintFlag{
				Name:        "max-lifetime",
				Usage:       "the max `seconds` of connections lifetime",
				Value:       mainOptions.Pool.MaxLifeSeconds,
				Destination: &mainOptions.Pool.MaxLifeSeconds,
			},
			&cli.BoolFlag{
				Name:        "save-options",
				Aliases:     []string{"s"},
				Usage:       fmt.Sprintf("save options to %s", Config),
				Destination: &saveOptions,
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
		After: func(*cli.Context) error {
			if saveOptions {
				config, err := json.MarshalIndent(&options, "", "    ")
				if err != nil {
					return nil
				}

				return ioutil.WriteFile(Config, config, 0600)
			}
			return nil
		},
		Commands: cli.Commands{
			initCommand(),
			benchCommand(),
			genCommand(),
			queryCommand(),
			hintCommand(),
			explainCommand(),
			infoCommand(),
			indexCommand(),
			cardCommand(),
			splitCommand(),
			loadCommand(),
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
