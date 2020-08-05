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
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var (
	loadCommand = &cli.Command{
		Name:  "load",
		Usage: "Load data in a directory",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "workload",
				Aliases:     []string{"w"},
				Usage:       "specify the workload `DIR`",
				Required:    true,
				Destination: &workloadDir,
			},
		},
		Action: func(context *cli.Context) error {
			if !pathExist(workloadDir) {
				return fmt.Errorf("directory %s not exists", workloadDir)
			}

			taskChan := make(chan struct{}, poolOptions.MaxOpenConns)
			var eg errgroup.Group

			err := filepath.Walk(workloadDir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if strings.HasSuffix(path, ".sql") && !info.IsDir() {
					eg.Go(func() error {
						file, err := os.Open(path)
						if err != nil {
							return fmt.Errorf("read file %s error: %v", path, err)
						}

						scanner := bufio.NewScanner(file)
						scanner.Split(bufio.ScanLines)

						log.Infof("loading file %s", path)

						queryCounter := 0

						for scanner.Scan() {
							if err = scanner.Err(); err != nil {
								return err
							}
							queryCounter++
							queryId := queryCounter

							query := scanner.Text()
							taskChan <- struct{}{}
							eg.Go(func() error {
								defer func() {
									<-taskChan
								}()

								_, err := Pool.Executor().Exec(query)
								if err != nil {
									err = fmt.Errorf("error in file `%s`, row(%d): %s", path, queryId, err.Error())
								}
								return err
							})
						}

						log.Debugf("file %s; query counter %d", path, queryCounter)
						return nil
					})
				}
				return nil
			})

			if err != nil {
				return err
			}
			return eg.Wait()
		},
	}
)
