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
	"github.com/chaos-mesh/horoscope/pkg/database-types"
	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/keymap"
	"os"
)

type Splitor struct {
	groupKey    *keymap.Key
	groupValues [][]byte
	slices      uint
	tx          *sql.Tx
	db          *types.Database
	maps        Maps
}

func StartSplit(exec executor.Executor, db *types.Database, maps []keymap.KeyMap, groupKey *keymap.Key, slices uint) (splitor Splitor, err error) {
	splitor.groupKey = groupKey
	splitor.db = db
	splitor.slices = slices

	splitor.maps, err = BuildMaps(db, maps, groupKey)
	if err != nil {
		return
	}

	splitor.tx, err = exec.Transaction(context.Background(), nil)
	if err != nil {
		return
	}

	err = splitor.tryLoadGroupValues()

	return
}

func (s *Splitor) tryLoadGroupValues() error {
	if s.groupKey != nil {
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
	}
	return nil
}

func (s *Splitor) Slices() int {
	slices := int(s.slices)
	if len(s.groupValues) != 0 {
		slices = len(s.groupValues)
	}
	return slices
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

func (s *Splitor) EndSplit() error {
	return s.tx.Rollback()
}
