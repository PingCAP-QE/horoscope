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
	"path"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/keymap"
	split_data "github.com/chaos-mesh/horoscope/pkg/split-data"
)

var (
	group       string
	slices      uint
	useBitArray bool

	groupKey *keymap.Key

	keymapPath = path.Join(dynWorkload, ".keymap")
	schemaPath = path.Join(dynWorkload, "schema.sql")
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
			&cli.BoolFlag{
				Name:        "bitarray",
				Aliases:     []string{"b"},
				Usage:       "filter duplicated rows with a bitarray",
				Destination: &useBitArray,
			},
		},
		Before: func(*cli.Context) (err error) {
			if group != "" {
				groupKey, err = keymap.ParseKey(group)
			}
			return
		},
		Action: func(context *cli.Context) (err error) {
			keymaps, err := keymap.ParseFile(keymapPath)
			if err != nil {
				return err
			}

			splitor, err := split_data.Split(Exec, Database, keymaps, groupKey, int(slices), useBitArray)

			if err != nil {
				return err
			}

			err = splitor.DumpSchema(schemaPath)
			if err != nil {
				return err
			}

			id := 0
			if !pathExist(slicesDir) {
				err = os.Mkdir(slicesDir, 0700)
				if err != nil {
					return err
				}
			}
			for {
				log.Infof("dumping slice (%d/%d)", id+1, splitor.Slices())

				id, err = splitor.Next(path.Join(slicesDir, fmt.Sprintf("%d", id)))
				if err != nil {
					return err
				}
				if id == 0 {
					return nil
				}
			}
		},
		After: rollback,
	}
)
