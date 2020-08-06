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

	"github.com/chaos-mesh/horoscope/pkg/database"
	"github.com/chaos-mesh/horoscope/pkg/keymap"
)

type (
	Maps map[string]*Node

	Node struct {
		table *database.Table
		links []*Link
	}

	Link struct {
		node     *Node
		from, to string
	}
)

func (n *Node) search(tableName string) *Node {
	queue := make([]*Node, 0)
	visited := make(map[*Node]bool)

	tryPush := func(node *Node) {
		if !visited[node] {
			queue = append(queue, node)
			visited[node] = true
		}
	}

	drain := func() []*Node {
		nodes := queue
		queue = make([]*Node, 0)
		return nodes
	}

	tryPush(n)

	for len(queue) > 0 {
		for _, node := range drain() {
			if node.table.Name.String() == tableName {
				return node
			}

			for _, link := range node.links {
				tryPush(link.node)
			}
		}
	}

	return nil
}

func BuildMaps(db *database.Database, mapList []keymap.KeyMap, groupKey *keymap.Key) (maps Maps, err error) {
	if err = checkKeymaps(db, mapList); err != nil {
		return
	}

	maps = make(Maps)

	for tableName, table := range db.BaseTables {
		maps[tableName] = &Node{
			table: table,
			links: make([]*Link, 0),
		}
	}

	for _, kp := range mapList {
		node := maps[kp.PrimaryKey.Table]

		for _, foreignKey := range kp.ForeignKeys {
			otherNode := maps[foreignKey.Table]
			node.links = append(node.links, &Link{
				node: otherNode,
				from: kp.PrimaryKey.Column,
				to:   foreignKey.Column,
			})
			otherNode.links = append(otherNode.links, &Link{
				node: node,
				from: foreignKey.Column,
				to:   kp.PrimaryKey.Column,
			})
		}
	}

	maps.autoPrune(mapList, groupKey)
	return
}

func (m Maps) autoPrune(maps []keymap.KeyMap, groupKey *keymap.Key) {
	if groupKey != nil {
		m.prune(groupKey.Table)
	}

	for _, kp := range maps {
		m.prune(kp.PrimaryKey.Table)
	}
}

func (m Maps) prune(root string) {
	if rootNode, ok := m[root]; ok {
		for table := range m {
			if table != rootNode.table.Name.String() && rootNode.search(table) != nil {
				delete(m, table)
			}
		}
	}

}

func checkKeymaps(db *database.Database, maps []keymap.KeyMap) error {
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

func checkKey(db *database.Database, key *keymap.Key) error {
	if table := db.BaseTables[key.Table]; table == nil || table.ColumnsMap[key.Column] == nil {
		return fmt.Errorf("key `%s` not exists", key)
	}
	return nil
}
