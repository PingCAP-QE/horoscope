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

	"github.com/pingcap/parser/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/database"
	"github.com/chaos-mesh/horoscope/pkg/generator"
)

type (
	IndexDMLPair struct {
		add    string
		remove string
	}
)

const (
	AddIndexesFile   = "add-indexes.sql"
	CleanIndexesFile = "clean-indexes.sql"
)

var (
	indexOptions                 = &options.Index
	addIndexFile, cleanIndexFile string
)

func indexCommand() *cli.Command {
	return &cli.Command{
		Name:  "index",
		Usage: "Add indexes for tables",
		Before: func(*cli.Context) error {
			indexesDir := path.Join(mainOptions.Workload, IndexesDir)
			addIndexFile = path.Join(indexesDir, AddIndexesFile)
			cleanIndexFile = path.Join(indexesDir, CleanIndexesFile)
			return nil
		},
		Subcommands: cli.Commands{
			&cli.Command{
				Name:   "new",
				Usage:  "New indexes schemes",
				Action: newScheme,
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:        "max",
						Aliases:     []string{"m"},
						Usage:       "the max `numbers` of indexes in each level",
						Value:       indexOptions.MaxIndexes,
						Destination: &indexOptions.MaxIndexes,
					},
					&cli.IntFlag{
						Name:        "level",
						Aliases:     []string{"l"},
						Usage:       "the compound `level` of indexes; 0 for no compound indexes",
						Value:       indexOptions.CompoundLevel,
						Destination: &indexOptions.CompoundLevel,
					},
				},
			},
			&cli.Command{
				Name:  "add",
				Usage: "Apply scheme add-indexes",
				Action: func(context *cli.Context) error {
					return apply(addIndexFile)
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
						Value:       indexOptions.ReserveIndexes,
						Destination: &indexOptions.ReserveIndexes,
					},
				},
				Action: clean,
			},
		},
	}
}

func newScheme(*cli.Context) error {
	if pathExist(addIndexFile) || pathExist(cleanIndexFile) {
		return errors.New("A indexes scheme already exists")
	}
	add := ""
	remove := ""
	for _, table := range Database.BaseTables {
		indexPairs := indexDML(table, indexOptions.CompoundLevel, indexOptions.MaxIndexes)
		for _, pairs := range indexPairs {
			for _, pair := range pairs {
				add += pair.add
				remove += pair.remove
			}
		}
	}
	err := ioutil.WriteFile(addIndexFile, []byte(add), 0644)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"path": addIndexFile,
	}).Info("Add scheme `add-indexes`, use `horo index add` to apply it")

	err = ioutil.WriteFile(cleanIndexFile, []byte(remove), 0644)
	if err == nil {
		log.WithFields(log.Fields{
			"path": cleanIndexFile,
		}).Info("Add scheme `clean-indexes`, use `horo index clean` to apply it")
	}
	return err
}

func clean(*cli.Context) error {
	if pathExist(cleanIndexFile) {
		if !indexOptions.ReserveIndexes {
			err := apply(cleanIndexFile)
			if err != nil {
				return err
			}
		}
		err := os.Remove(cleanIndexFile)
		if err != nil {
			return err
		}
	}
	if pathExist(addIndexFile) {
		err := os.Remove(addIndexFile)
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
			_, err = Pool.Executor().Exec(query)
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

func keyName(table *database.Table, fields []string) string {
	segments := append([]string{strings.ToUpper(table.Name.String())}, fields...)
	segments = append(segments, "IDX")
	return strings.Join(segments, "_")
}

func indexDML(table *database.Table, level, max int) [][]IndexDMLPair {
	allLevelPairs := indexLevel(table, level)
	for level, pairs := range allLevelPairs {
		allLevelPairs[level] = randMax(pairs, max)
	}
	return allLevelPairs
}

func indexLevel(table *database.Table, level int) [][]IndexDMLPair {
	allLevelPairs := make([][]IndexDMLPair, 0, level)
	allLevelFieldLists := make([][][]string, 0, level)
	pairs := make([]IndexDMLPair, 0, len(table.Columns))
	fieldLists := make([][]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		// We ignore BLOB and TEXT columns
		if column.Type.Tp == mysql.TypeBlob || column.Type.Tp == mysql.TypeLongBlob ||
			column.Type.Tp == mysql.TypeMediumBlob || column.Type.Tp == mysql.TypeTinyBlob {
			continue
		}
		fields := []string{column.Name.String()}
		fieldLists = append(fieldLists, fields)
		if column.Key == "" {
			pairs = append(pairs, fields2Pair(table, fields))
		}
	}
	allLevelPairs = append(allLevelPairs, pairs)
	allLevelFieldLists = append(allLevelFieldLists, fieldLists)

	for i := 1; i < level; i++ {
		pairs = make([]IndexDMLPair, 0)
		fieldLists = make([][]string, 0)
		fieldSet := make(map[string]bool)
		for _, list := range allLevelFieldLists[i-1] {
			for _, field := range list {
				fieldSet[field] = true
			}
			for _, column := range table.Columns {
				if column.Type.Tp == mysql.TypeBlob || column.Type.Tp == mysql.TypeLongBlob ||
					column.Type.Tp == mysql.TypeMediumBlob || column.Type.Tp == mysql.TypeTinyBlob {
					continue
				}
				if !fieldSet[column.Name.String()] {
					newList := append(list, column.Name.String())
					fieldLists = append(fieldLists, newList)
					pairs = append(pairs, fields2Pair(table, newList))
				}
			}
			fieldSet = make(map[string]bool)
		}
		allLevelPairs = append(allLevelPairs, pairs)
		allLevelFieldLists = append(allLevelFieldLists, fieldLists)
	}
	return allLevelPairs
}

func randMax(allPairs []IndexDMLPair, max int) []IndexDMLPair {
	if len(allPairs) <= max {
		return allPairs
	}
	pairMap := make(map[IndexDMLPair]bool)
	for len(pairMap) < max {
		pair := allPairs[generator.Rd(len(allPairs))]
		pairMap[pair] = true
	}

	ret := make([]IndexDMLPair, 0, max)
	for pair := range pairMap {
		ret = append(ret, pair)
	}
	return ret
}

func fields2Pair(table *database.Table, fields []string) IndexDMLPair {
	key := keyName(table, fields)
	fieldList := strings.Join(fields, ",")
	return IndexDMLPair{
		add:    fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (%s);\n", table.Name, key, fieldList),
		remove: fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`;\n", table.Name, key),
	}
}
