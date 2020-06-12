package generator

import "github.com/pingcap/parser/ast"

type (
	Generator interface {
		Query() ast.StmtNode
	}
)
