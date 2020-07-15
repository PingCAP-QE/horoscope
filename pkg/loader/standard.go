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

package loader

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	log "github.com/sirupsen/logrus"
)

const (
	QueryDir = "queries"
)

type StandardLoader struct {
	workloadDir string
	parser      *parser.Parser
	index       int
	queries     []queryInfo
}

type queryInfo struct {
	name string
	sql  string
}

func (g *StandardLoader) Next() (string, ast.StmtNode) {
	if g.index >= len(g.queries) {
		return "", nil
	}
	queryId := g.index
	g.index++
	query := g.queries[queryId]

	stmt, err := g.parser.ParseOneStmt(query.sql, "", "")

	if err != nil {
		log.WithFields(log.Fields{
			"query": query,
			"err":   err.Error(),
		}).Warn("Fails to parse, ignore this query")
		return g.Next()
	}
	return query.name, stmt
}

func (g *StandardLoader) load() error {
	dir := path.Join(g.workloadDir, QueryDir)
	err := filepath.Walk(dir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				sql, err := ioutil.ReadFile(path)
				if err != nil {
					return fmt.Errorf("read file %s error: %v", path, err)
				}
				g.queries = append(g.queries, queryInfo{
					name: info.Name(),
					sql:  string(sql),
				})
			}
			return nil
		})
	if err != nil {
		return fmt.Errorf("walk dir %s error: %v", dir, err)
	}
	return nil
}

func LoadDir(workloadDir string) (QueryLoader, error) {
	newLoader := &StandardLoader{workloadDir: workloadDir, parser: parser.New()}
	err := newLoader.load()
	return newLoader, err
}
