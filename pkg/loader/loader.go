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

import "github.com/pingcap/parser/ast"

type QueryLoader interface {
	Next() (queryID string, queryStmt ast.StmtNode)
}

// NoopLoader does nothing
type NoopLoader struct{}

func (n NoopLoader) Next() (queryID string, queryStmt ast.StmtNode) {
	return "", nil
}
