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

	"github.com/chaos-mesh/horoscope/pkg/database-types"
	"github.com/chaos-mesh/horoscope/pkg/keymap"
)

type (
	Trees map[string]*Node

	Node struct {
		primaryKey *keymap.Key
		table      *types.Table
		children   []*Link
	}

	Link struct {
		node       *Node
		foreignKey *keymap.Key
	}
)

func BuildTrees(db *types.Database, maps []keymap.KeyMap, keepTable string) (trees Trees, err error) {
	if err = checkKeymaps(db, maps); err != nil {
		return
	}

	trees = make(Trees)

	for tableName, table := range db.BaseTables {
		trees[tableName] = &Node{
			table:    table,
			children: make([]*Link, 0),
		}
	}

	for _, kp := range maps {
		primaryKey := kp.PrimaryKey
		tree := trees[primaryKey.Table]
		if tree.primaryKey != nil && primaryKey.Column != tree.primaryKey.Column {
			err = fmt.Errorf("duplicated primary key: %s <!> %s", kp.PrimaryKey, tree.primaryKey)
			return
		}

		trees[primaryKey.Table].primaryKey = primaryKey

		for _, foreignKey := range kp.ForeignKeys {
			if child, ok := trees[foreignKey.Table]; ok && foreignKey.Table != keepTable {
				tree.children = append(tree.children, &Link{
					node:       child,
					foreignKey: foreignKey,
				})
				delete(trees, foreignKey.Table)
			}
		}
	}

	return
}

func checkKeymaps(db *types.Database, maps []keymap.KeyMap) error {
	for _, kp := range maps {
		if err := checkKey(db, kp.PrimaryKey); err != nil {
			return nil
		}
		for _, key := range kp.ForeignKeys {
			if err := checkKey(db, key); err != nil {
				return nil
			}
		}
	}
	return nil
}

func checkKey(db *types.Database, key *keymap.Key) error {
	if table := db.BaseTables[key.Table]; table == nil || !table.ColumnsSet[key.Column] {
		return fmt.Errorf("key `%s` not exists", key)
	}
	return nil
}
