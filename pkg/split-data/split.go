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
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/golang-collections/go-datastructures/bitarray"
	log "github.com/sirupsen/logrus"

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
	exec         executor.Executor
	db           *types.Database
	maps         Maps
	sliceSizeMap map[string]int
	filterMap    map[string]bitarray.BitArray
}

func Split(exec executor.Executor, db *types.Database, maps []keymap.KeyMap, groupKey *keymap.Key, slices int, useBitArray bool) (splitor *Splitor, err error) {
	splitor = &Splitor{
		groupKey:     groupKey,
		db:           db,
		slices:       slices,
		exec:         exec,
		sliceSizeMap: make(map[string]int),
	}
	splitor.initFilter(useBitArray)

	splitor.maps, err = BuildMaps(db, maps, groupKey)
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
	if err != nil {
		return
	}

	return
}

func (s *Splitor) loadGroupValues() error {
	rows, err := s.exec.Query(fmt.Sprintf(
		"select %s from %s group by %s order by %s",
		s.groupKey.Column,
		s.groupKey.Table,
		s.groupKey.Column,
		s.groupKey.Column,
	))
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
			rows, err := s.exec.Query(fmt.Sprintf("select count(*) from %s", table))
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

func (s *Splitor) initFilter(useBitArray bool) {
	if !useBitArray {
		return
	}

	s.filterMap = make(map[string]bitarray.BitArray)

	for table := range s.db.BaseTables {
		s.filterMap[table] = bitarray.NewSparseBitArray()
	}
}

func (s *Splitor) DumpSchema(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	for table := range s.db.BaseTables {
		rows, err := s.exec.Query(fmt.Sprintf("show create table %s", table))
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
		err = s.dumpMap(table, node, file)
		if err != nil {
			return
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

func (s *Splitor) dumpMap(table string, node *Node, file *os.File) error {
	deleteList := make([]string, 0)
	clause, err := s.genRootClause(table, node)

	if err != nil {
		return err
	}

	visitedSet := make(map[*Node]bool)

	var recursivelyWrite func(node *Node, clause string) error
	recursivelyWrite = func(node *Node, clause string) error {
		visitedSet[node] = true
		deleteList = append([]string{fmt.Sprintf("delete from %s %s", node.table.Name, clause)}, deleteList...)
		selectStmt := fmt.Sprintf("select * from %s %s", node.table.Name, clause)
		log.Debug(selectStmt)
		stream, err := s.exec.QueryStream(selectStmt)
		if err != nil {
			return err
		}
		err = s.writeToFile(node.table, file, stream)
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
		return err
	}

	if s.filterMap == nil {
		// no filter map, filter by delete

		for _, deleteStmt := range deleteList {
			log.Debug(deleteStmt)
			_, err = s.exec.Exec(deleteStmt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Splitor) genRootClause(table string, node *Node) (clause string, err error) {
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

		if node.table.PrimaryKey == nil {
			err = fmt.Errorf("table %s has no primary key; evenly split fails", table)
			return
		}

		clause = fmt.Sprintf(
			"order by %s limit %d",
			node.table.PrimaryKey.Name,
			s.sliceSizeMap[table],
		)
	}
	return
}

func (s *Splitor) writeToFile(table *types.Table, file *os.File, stream executor.RowStream) error {
	for {
		var row executor.Row
		row, err := stream.Next()
		if err != nil {
			return err
		}

		if row == nil {
			return nil
		}

		if s.filterMap != nil {
			if table.PrimaryKey == nil {
				return fmt.Errorf("table %s has no primary key, cannot be filtered", table.Name)
			}

			index, ok := stream.ColumnsMap[table.PrimaryKey.Name.String()]
			if !ok {
				return fmt.Errorf("table %s has no column %s", table.Name, table.PrimaryKey.Name)
			}

			value := string(row[index])
			pk, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return fmt.Errorf("primary key %s of %s is not a invalid unsigned int, value: %s",
					table.PrimaryKey.Name, table.Name, value,
				)
			}

			// A sparse bit array never returns an error
			filter := s.filterMap[table.Name.String()]
			set, _ := filter.GetBit(pk)

			if set {
				continue
			}

			_ = filter.SetBit(pk)
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

		_, err = file.WriteString(insertStmt)
		if err != nil {
			return err
		}
	}
}

func (s *Splitor) Slices() int {
	return s.slices
}
