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
	"os"
	"path"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func initCommand() *cli.Command {
	return &cli.Command{
		Name:    "init",
		Aliases: []string{"i"},
		Usage:   "initialize workload",
		Action: func(context *cli.Context) error {
			workload := mainOptions.Workload
			if err := tryMkdir(workload); err != nil {
				return err
			}

			if err := tryMkdir(path.Join(workload, QueriesDir)); err != nil {
				return err
			}

			if err := tryMkdir(path.Join(workload, IndexesDir)); err != nil {
				return err
			}

			if err := tryMkdir(path.Join(workload, SliceDir)); err != nil {
				return err
			}

			if err := tryCreate(path.Join(workload, KeymapFile)); err != nil {
				return err
			}

			return nil
		},
	}
}

func tryMkdir(path string) error {
	if !pathExist(path) {
		log.WithField("path", path).Info("making directory")
		if err := os.Mkdir(path, 0700); err != nil {
			return err
		}
	}
	return nil
}

func tryCreate(path string) error {
	if !pathExist(path) {
		log.WithField("path", path).Info("creating file")
		if _, err := os.Create(path); err != nil {
			return err
		}
	}
	return nil
}
