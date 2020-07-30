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

package split_data

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	types "github.com/chaos-mesh/horoscope/pkg/database-types"
	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/generator"
	"github.com/chaos-mesh/horoscope/pkg/keymap"
)

type Splitor struct {
	sliceCounter int
	groupKey     *keymap.Key
	groupValues  [][]byte
	slices       int
	tx           *sql.Tx
	db           *types.Database
	maps         Maps
	sliceSizeMap map[string]int
}

func StartSplit(exec executor.Executor, db *types.Database, maps []keymap.KeyMap, groupKey *keymap.Key, slices int) (splitor Splitor, err error) {
	splitor.groupKey = groupKey
	splitor.db = db
	splitor.slices = slices
	splitor.sliceSizeMap = make(map[string]int)

	splitor.maps, err = BuildMaps(db, maps, groupKey)
	if err != nil {
		return
	}

	splitor.tx, err = exec.Transaction(context.Background(), nil)
	if err != nil {
		return
	}

	if splitor.groupKey != nil {
		err = splitor.loadGroupValues()
		if err != nil {
			return
		}

		splitor.updateSlices()
	}

	err = splitor.calculateSliceSize()

	return
}

func (s *Splitor) loadGroupValues() error {
	rawData, err := s.tx.Query(fmt.Sprintf(
		"select %s from %s group by %s order by %s",
		s.groupKey.Column,
		s.groupKey.Table,
		s.groupKey.Column,
		s.groupKey.Column,
	))
	if err != nil {
		return err
	}

	rows, err := executor.NewRows(rawData)

	if err != nil {
		return err
	}

	if rows.RowCount() == 0 || rows.ColumnNums() != 1 {
		return fmt.Errorf("invalid group values: %s", rows.String())
	}

	s.groupValues = make([][]byte, 0, rows.RowCount())
	for _, row := range rows.Data {
		s.groupValues = append(s.groupValues, row[0])
	}
	return nil
}

func (s *Splitor) updateSlices() {
	s.slices = len(s.groupValues)
}

func (s *Splitor) calculateSliceSize() error {
	for table := range s.maps {
		if s.groupKey == nil || s.groupKey.Table != table {
			raw, err := s.tx.Query(fmt.Sprintf("select count(*) from %s", table))
			if err != nil {
				return err
			}

			rows, err := executor.NewRows(raw)
			if err != nil {
				return err
			}

			count, err := strconv.Atoi(string(rows.Data[0][0]))
			if err != nil {
				return err
			}

			if count >= s.slices {
				s.sliceSizeMap[table] = count / s.slices
			}
		}
	}
	return nil
}

func (s *Splitor) DumpSchema(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	for table := range s.db.BaseTables {
		raw, err := s.tx.Query(fmt.Sprintf("show create table %s", table))
		if err != nil {
			return err
		}
		rows, err := executor.NewRows(raw)
		if err != nil {
			return err
		}

		_, err = file.WriteString(fmt.Sprintf("\n%s;\n", string(rows.Data[0][1])))
		if err != nil {
			return err
		}
	}
	return nil
}

/// Dump data into path; return id of next slice
func (s *Splitor) Next(path string) (id int, err error) {
	file, err := os.Create(path)
	if err != nil {
		return
	}

	for table, node := range s.maps {
		deleteList := make([]string, 0)
		var clause string
		if s.groupKey != nil && s.groupKey.Table == table {
			// group split
			groupValue := s.groupValues[s.sliceCounter]
			clause = fmt.Sprintf(
				"where %s<=>%s",
				s.groupKey.String(),
				generator.FormatValue(node.table.ColumnsMap[s.groupKey.Column].Type, groupValue),
			)
		} else if s.sliceSizeMap[table] != 0 {
			// evenly split

			pk := node.table.PrimaryKey()
			if pk == nil {
				err = fmt.Errorf("table %s has no primary key; evenly split fails", table)
				return
			}

			clause = fmt.Sprintf(
				"order by %s limit %d",
				pk.Name,
				s.sliceSizeMap[table],
			)
		}

		visitedSet := make(map[*Node]bool)

		writeToFile := func(table *types.Table, rows *sql.Rows) error {
			stream, err := executor.NewRowStream(rows)
			if err != nil {
				return err
			}

			for {
				var row executor.Row
				row, err = stream.Next()
				if err != nil {
					return err
				}

				if row == nil {
					return nil
				}

				valueList := make([]string, 0, len(row))
				for i, value := range row {
					column := table.ColumnsMap[string(stream.Columns[i])]
					valueList = append(valueList, generator.FormatValue(column.Type, value))
				}

				insertStmt := fmt.Sprintf(
					"insert into %s values (%s);\n",
					table.Name, strings.Join(valueList, ","),
				)

				_, err := file.WriteString(insertStmt)
				if err != nil {
					return err
				}
			}
		}

		var recursivelyWrite func(node *Node, clause string) error
		recursivelyWrite = func(node *Node, clause string) error {
			visitedSet[node] = true
			deleteList = append([]string{fmt.Sprintf("delete from %s %s", node.table.Name, clause)}, deleteList...)
			rows, err := s.tx.Query(fmt.Sprintf("select * from %s %s", node.table.Name, clause))
			if err != nil {
				return err
			}
			err = writeToFile(node.table, rows)
			if err != nil {
				return err
			}

			for _, link := range node.links {
				if !visitedSet[link.node] {
					linkClause := fmt.Sprintf(
						"where %s in (select %s from %s %s)",
						link.to, link.from, node.table.Name, clause,
					)
					err = recursivelyWrite(link.node, linkClause)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}

		err = recursivelyWrite(node, clause)
		if err != nil {
			return
		}

		for _, deleteStmt := range deleteList {
			_, err = s.tx.Exec(deleteStmt)
			if err != nil {
				return
			}
		}
	}

	defer func() {
		s.sliceCounter++
		if s.sliceCounter < s.slices {
			id = s.sliceCounter
		} else {
			id = 0
		}
	}()
	return
}

func (s *Splitor) EndSplit() error {
	return s.tx.Rollback()
}

func (s Splitor) Slices() int {
	return s.slices
}
