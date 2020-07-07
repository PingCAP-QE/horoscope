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

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

func Parse(sql string) (stmt ast.StmtNode, err error) {
	stmts, warns, err := parser.New().Parse(sql, "", "")
	if err != nil {
		return
	}

	for _, warn := range warns {
		if warn != nil {
			err = warn
			return
		}
	}

	if len(stmts) != 1 {
		err = errors.New(fmt.Sprintf("Invalid statement: %s", sql))
		return
	}
	stmt = stmts[0]
	return
}
