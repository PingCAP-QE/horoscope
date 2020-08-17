package utils

import (
	"bytes"
	"math"
	"reflect"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
)

func QError(est, act float64) float64 {
	if act == 0 || est == 0 {
		return math.Inf(1)
	}
	z := est / act
	zp := act / est
	if z < 0 {
		return math.Inf(1)
	}
	return math.Max(z, zp)
}

func BufferOut(node ast.Node) (string, error) {
	out := new(bytes.Buffer)
	err := node.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

func NewValueExpr(value interface{}) ast.ValueExpr {
	if value == nil {
		return ast.NewValueExpr(nil, "", "")
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Slice, reflect.Ptr:
		return ast.NewValueExpr(nil, "", "")
	default:
		return ast.NewValueExpr(value, "", "")
	}
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
