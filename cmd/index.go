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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var (
	reserveIndexes bool
	dynWorkload    = "benchmark/dyn/indexes"
	addIndexes     = path.Join(dynWorkload, "add-indexes.sql")
	cleanIndexes   = path.Join(dynWorkload, "clean-indexes.sql")
	indexCommand   = &cli.Command{
		Name:  "index",
		Usage: "Add indexes for tables",
		Subcommands: cli.Commands{
			&cli.Command{
				Name:   "new",
				Usage:  "New indexes schemes",
				Action: newScheme,
			},
			&cli.Command{
				Name:  "add",
				Usage: "Apply scheme add-indexes",
				Action: func(context *cli.Context) error {
					return apply(addIndexes)
				},
			},
			&cli.Command{
				Name:  "clean",
				Usage: "Clean schemes and indexes",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:        "reserve",
						Aliases:     []string{"r"},
						Usage:       "reserve indexes",
						Value:       false,
						Destination: &reserveIndexes,
					},
				},
				Action: clean,
			},
		},
	}
)

func newScheme(*cli.Context) error {
	if pathExist(addIndexes) || pathExist(cleanIndexes) {
		return errors.New("A indexes scheme already exists")
	}
	add := ""
	clean := ""
	for _, table := range Database.BaseTables {
		for _, column := range table.Columns {
			if column.Key == "" {
				keyName := KeyName([]string{column.Name.String()})
				add += fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (`%s`);\n", table.Name, keyName, column.Name)
				clean += fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`;\n", table.Name, keyName)
			}
			for _, another := range table.Columns {
				if column != another {
					keyName := KeyName([]string{column.Name.String(), another.Name.String()})
					add += fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (`%s`, `%s`);\n", table.Name, keyName, column.Name, another.Name)
					clean += fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`;\n", table.Name, keyName)
				}
			}
		}
	}
	err := ioutil.WriteFile(addIndexes, []byte(add), 0644)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"path": addIndexes,
	}).Info("Add scheme `add-indexes`, use `horo index add` to apply it")

	err = ioutil.WriteFile(cleanIndexes, []byte(clean), 0644)
	if err == nil {
		log.WithFields(log.Fields{
			"path": cleanIndexes,
		}).Info("Add scheme `clean-indexes`, use `horo index clean` to apply it")
	}
	return err
}

func clean(*cli.Context) error {
	if pathExist(cleanIndexes) {
		if !reserveIndexes {
			err := apply(cleanIndexes)
			if err != nil {
				return err
			}
		}
		err := os.Remove(cleanIndexes)
		if err != nil {
			return err
		}
	}
	if pathExist(addIndexes) {
		err := os.Remove(addIndexes)
		if err != nil {
			return err
		}
	}
	return nil
}

func apply(path string) error {
	if !pathExist(path) {
		return errors.New(fmt.Sprintf("Fail to apply index scheme: file %s not found", path))
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	sql := string(data)
	for _, query := range strings.Split(sql, "\n") {
		if query != "" {
			log.WithField("query", query).Info("Executing...")
			_, err = Exec.Exec(query)
			if err != nil {
				log.WithField("query", query).Warnf("fails to execute: %s", err.Error())
			}
		}
	}
	return nil
}

func pathExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

func KeyName(fields []string) string {
	segments := append(fields, "ID")
	return strings.Join(segments, "_")
}
