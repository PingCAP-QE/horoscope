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

package types

import (
	"fmt"
	
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/types"
)

type Parser struct {
	parser.Parser
}

func NewParser() *Parser {
	return &Parser{*parser.New()}
}

func (p *Parser) ParseFieldType(tp string) (fieldType *types.FieldType, err error) {
	ddl := fmt.Sprintf("CREATE TABLE t (c %s)", tp)
	stmt, err := p.ParseOneStmt(ddl, "", "")
	if err != nil {
		return
	}

	nodes, ok := stmt.(*ast.CreateTableStmt)

	if !ok || len(nodes.Cols) != 1 {
		err = fmt.Errorf("invalid field type: %s", tp)
		return
	}
	fieldType = nodes.Cols[0].Tp
	return
}
