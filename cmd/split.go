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
	split_data "github.com/chaos-mesh/horoscope/pkg/split-data"
	"path"

	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/keymap"
)

var (
	group  string
	slices uint

	groupKey *keymap.Key

	keymapPath = path.Join(dynWorkload, ".keymap")
	slicesDir  = path.Join(dynWorkload, "slices")

	splitCommand = &cli.Command{
		Name:    "split",
		Aliases: []string{"s"},
		Usage:   "Split data into several slices",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "group",
				Aliases:     []string{"g"},
				Usage:       "group split, group by `<table>.<column>`",
				Destination: &group,
			},
			&cli.UintFlag{
				Name:        "slices",
				Aliases:     []string{"s"},
				Usage:       "the `numbers` of slices to split, only used when flag --group is not set",
				Value:       100,
				Destination: &slices,
			},
		},
		Before: func(*cli.Context) (err error) {
			if group != "" {
				groupKey, err = keymap.ParseKey(group)
			}
			return
		},
		Action: func(context *cli.Context) error {
			keymaps, err := keymap.ParseFile(keymapPath)
			if err != nil {
				return err
			}

			var keepTable string

			if groupKey != nil {
				keepTable = groupKey.Table
			}

			if _, err := split_data.BuildTrees(Database, keymaps, keepTable); err != nil {
				return err
			}
			return nil
		},
	}
)
