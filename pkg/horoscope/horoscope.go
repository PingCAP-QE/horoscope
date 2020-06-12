package horoscope

import (
	"bytes"
	"errors"
	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
)

var (
	/// Config

	Dsn        = "root:@tcp(localhost:4000)/test?charset=utf8"
	Round uint = 100
)

type (
	Horoscope struct {
		exec executor.Executor
	}
)

func NewHoroscope() (scope *Horoscope, err error) {
	exec, err := executor.NewExecutor(Dsn)
	if err != nil {
		return
	}
	return &Horoscope{exec: exec}, err
}

func (h *Horoscope) Plan(node ast.StmtNode, planId uint) (string, error) {
	switch stmt := node.(type) {
	case *ast.SelectStmt:
		stmt.TableHints = []*ast.TableOptimizerHint{}
	default:
		return "", errors.New("unsupported statement")
	}
	return bufferOut(node)
}

func (h *Horoscope) Run() {

}

func bufferOut(node ast.Node) (string, error) {
	out := new(bytes.Buffer)
	err := node.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		return "", err
	}
	return out.String(), nil
}
