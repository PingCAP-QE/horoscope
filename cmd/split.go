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

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/chaos-mesh/horoscope/pkg/keymap"
	split_data "github.com/chaos-mesh/horoscope/pkg/split-data"
)

var (
	splitOptions = &options.Split

	groupKey *keymap.Key

	splitCommand = &cli.Command{
		Name:    "split",
		Aliases: []string{"s"},
		Usage:   "Split data into several slices",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "group",
				Aliases:     []string{"g"},
				Usage:       "group split, group by `<table>.<column>`",
				Value:       splitOptions.Group,
				Destination: &splitOptions.Group,
			},
			&cli.UintFlag{
				Name:        "slices",
				Aliases:     []string{"s"},
				Usage:       "the `numbers` of slices to split, only used when flag --group is not set",
				Value:       splitOptions.Slices,
				Destination: &splitOptions.Slices,
			},
			&cli.UintFlag{
				Name:        "batch",
				Aliases:     []string{"b"},
				Usage:       "the `size` of batch insert",
				Value:       splitOptions.BatchSize,
				Destination: &splitOptions.BatchSize,
			},
			&cli.BoolFlag{
				Name:        "bitarray",
				Usage:       "filter duplicated rows with a bitarray",
				Value:       splitOptions.UseBitArray,
				Destination: &splitOptions.UseBitArray,
			},
		},
		Before: func(ctx *cli.Context) (err error) {
			if splitOptions.Group != "" {
				groupKey, err = keymap.ParseKey(splitOptions.Group)
			}
			if err != nil {
				return
			}
			return initTx(ctx)
		},
		After: rollback,
		Action: func(context *cli.Context) (err error) {
			keymaps, err := keymap.ParseFile(path.Join(mainOptions.Workload, KeymapFile))
			if err != nil {
				return err
			}

			splitor, err := split_data.Split(Tx, Database, keymaps, groupKey, int(splitOptions.Slices), splitOptions.UseBitArray)

			if err != nil {
				return err
			}

			err = splitor.DumpSchema(path.Join(mainOptions.Workload, SchemaFile))
			if err != nil {
				return err
			}

			id := 0
			for {
				log.Infof("dumping slice (%d/%d)", id+1, splitor.Slices())

				id, err = splitor.Next(path.Join(path.Join(mainOptions.Workload, SliceDir), fmt.Sprintf("%d", id)), splitOptions.BatchSize)
				if err != nil {
					return err
				}
				if id == 0 {
					return nil
				}
			}
		},
	}
)
