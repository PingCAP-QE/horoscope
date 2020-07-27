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

import "github.com/urfave/cli/v2"

var (
	splitMode    string
	slices       uint
	splitCommand = &cli.Command{
		Name:    "split",
		Aliases: []string{"s"},
		Usage:   "Split data into several slices",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "mode",
				Aliases:     []string{"m"},
				Usage:       "split mode, `<even|group>`",
				Value:       "even",
				Destination: &splitMode,
			},
			&cli.UintFlag{
				Name:        "slices",
				Aliases:     []string{"s"},
				Usage:       "the `numbers` of slices in evenly split",
				Value:       100,
				Destination: &slices,
			},
		},
		Action: func(context *cli.Context) error {
			return nil
		},
	}
)
