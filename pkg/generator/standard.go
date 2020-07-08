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

package generator

import (
	"fmt"
	"github.com/pingcap/errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	log "github.com/sirupsen/logrus"
)

type StandardGenerator struct {
	workloadDir string
	parser      *parser.Parser
	index       int
	queries     []queryInfo
}

type queryInfo struct {
	name string
	sql  string
}

func (g *StandardGenerator) Next() (string, ast.StmtNode) {
	if g.index == 0 {
		g.init()
	}
	if g.index >= len(g.queries) {
		return "", nil
	}
	queryId := g.index
	g.index++
	query := g.queries[queryId]

	stmt, warns, err := g.parser.Parse(query.sql, "", "")

	if err != nil || len(warns) > 0 || len(stmt) != 1 {
		if err != nil {
			log.WithFields(log.Fields{
				"query": query,
				"err":   err.Error(),
			}).Fatal("Fails to parse query")
		}

		if len(warns) > 0 {
			for _, warn := range warns {
				log.WithFields(log.Fields{
					"query":   query,
					"warning": warn.Error(),
				}).Warn("Warns in parsing query")
			}
		}

		return g.Next()
	}
	return query.name, stmt[0]
}

func (g *StandardGenerator) init() error {
	err := filepath.Walk(fmt.Sprintf("%s/queries", g.workloadDir),
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				sql, err := ioutil.ReadFile(path)
				if err != nil {
					return errors.Trace(err)
				}
				g.queries = append(g.queries, queryInfo{
					name: info.Name(),
					sql:  string(sql),
				})
			}
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func NewStandardGenerator(workloadDir string) Generator {
	return &StandardGenerator{workloadDir: workloadDir, parser: parser.New()}
}
