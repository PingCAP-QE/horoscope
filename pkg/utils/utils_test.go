package utils

import (
	"testing"

	"github.com/magiconair/properties/assert"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/require"
)

func TestNewNilValueExpr(t *testing.T) {
	stmt := &ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{
			Fields: []*ast.SelectField{
				{
					Expr: NewValueExpr([]byte(nil)),
				},
			},
		},
	}

	query, err := BufferOut(stmt)
	require.Nil(t, err)
	assert.Equal(t, query, "SELECT NULL")
}
