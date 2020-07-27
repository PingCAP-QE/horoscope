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
	"path"

	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/database-types"
	"github.com/chaos-mesh/horoscope/pkg/keymap"
)

var (
	groups = cli.NewStringSlice()
	evens  = cli.NewStringSlice()

	keymapPath = path.Join(dynWorkload, ".keymap")
	slicesDir  = path.Join(dynWorkload, "slices")

	splitCommand = &cli.Command{
		Name:    "split",
		Aliases: []string{"s"},
		Usage:   "Split data into several slices",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:        "group",
				Aliases:     []string{"g"},
				Usage:       "group split, group by `<table>.<column>`",
				Destination: groups,
			},
			&cli.StringSliceFlag{
				Name:        "even",
				Aliases:     []string{"e"},
				Usage:       "evenly split by `<table>(<nums>)`",
				Destination: evens,
			},
		},
		Action: func(context *cli.Context) error {
			keymaps, err := keymap.ParseFile(keymapPath)
			if err != nil {
				return err
			}
			if err = checkKeymaps(Database, keymaps); err != nil {
				return err
			}
			return nil
		},
	}
)

func checkKeymaps(db *types.Database, maps []keymap.KeyMap) error {
	for _, kp := range maps {
		if err := checkKey(db, kp.PrimaryKey); err != nil {
			return nil
		}
		for _, key := range kp.ForeignKeys {
			if err := checkKey(db, key); err != nil {
				return nil
			}
		}
	}
	return nil
}

func checkKey(db *types.Database, key *keymap.Key) error {
	if table := db.BaseTables[key.Table]; table == nil || !table.ColumnsSet[key.Column] {
		return fmt.Errorf("key `%s` not exists", key)
	}
	return nil
}
