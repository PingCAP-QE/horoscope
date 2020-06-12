package horoscope

import (
	"fmt"
	"testing"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
)

func TestHoroscope_Plan(t *testing.T) {
	stmts, warns, err := parser.New().Parse("SELECT /*+ NTH_PLAN(1) */ * FROM t", "", "")
	assert.Nil(t, err)
	assert.Empty(t, warns)
	assert.Len(t, stmts, 1)
	selectStmt, ok := stmts[0].(*ast.SelectStmt)
	assert.True(t, ok)
	fmt.Printf("%#v", selectStmt.TableHints[0])
}
