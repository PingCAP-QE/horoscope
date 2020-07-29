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

	"github.com/chaos-mesh/horoscope/pkg/database-types"
	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/keymap"
)

type Splitor struct {
	groupKey *keymap.Key
	tx       *sql.Tx
	db       *types.Database
	trees    Maps
}

func StartSplit(exec executor.Executor, db *types.Database, maps []keymap.KeyMap, groupKey *keymap.Key) (splitor Splitor, err error) {
	splitor.groupKey = groupKey
	splitor.db = db

	splitor.trees, err = BuildMaps(db, maps, groupKey)
	if err != nil {
		return
	}

	splitor.tx, err = exec.Transaction(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	return
}

func (s *Splitor) EndSplit() error {
	return s.tx.Rollback()
}
