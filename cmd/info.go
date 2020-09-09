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

	"github.com/jedib0t/go-pretty/table"
	"github.com/urfave/cli/v2"
)

var (
	infoOptions = &options.Info
)

func infoCommand() *cli.Command {
	return &cli.Command{
		Name:  "info",
		Usage: "Show database information",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "table",
				Aliases:     []string{"t"},
				Usage:       "show single `TABLE`",
				Value:       infoOptions.Table,
				Destination: &infoOptions.Table,
			},
		},
		Action: func(context *cli.Context) error {
			if infoOptions.Table == "" {
				fmt.Println(showTables())
			} else {
				repr, err := showTable(infoOptions.Table)
				if err != nil {
					return err
				}
				fmt.Println(repr)
			}
			return nil
		},
	}
}

func showTables() string {
	t := table.NewWriter()
	t.AppendHeader(table.Row{"Base Tables"})
	for name := range Database.BaseTables {
		t.AppendRow(table.Row{name})
	}
	return t.Render()
}

func showTable(tableName string) (repr string, err error) {
	tb, ok := Database.BaseTables[tableName]
	if !ok {
		err = errors.New(fmt.Sprintf("Table %s not found", tableName))
		return
	}
	t := table.NewWriter()
	t.AppendHeader(table.Row{"Field", "Type", "Null", "Key"})
	for _, column := range tb.Columns {
		t.AppendRow(table.Row{column.Name.String(), column.FullType(), column.Null, column.Key})
	}
	repr = t.Render()
	return
}
