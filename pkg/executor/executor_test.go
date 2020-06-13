package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMySQLExecutor_Query(t *testing.T) {
	exec, err := NewExecutor("root:@tcp(localhost:4000)/test?charset=utf8")
	assert.Nil(t, err)
	_, err = exec.Query(`SELECT /*+ NTH_PLAN(3)*/ l_returnflag,l_linestatus,sum(l_quantity) AS sum_qty,sum(l_extendedprice) AS sum_base_price,sum(l_extendedprice*(1-l_discount)) AS sum_disc_price,sum(l_extendedprice*(1-l_discount)*(1+l_tax)) AS sum_charge,avg(l_quantity) AS avg_qty,avg(l_extendedprice) AS avg_price,avg(l_discount) AS avg_disc,count(1) AS count_order FROM lineitem WHERE l_shipdate<=date_sub("1998-12-01", INTERVAL 108 DAY) GROUP BY l_returnflag,l_linestatus ORDER BY l_returnflag,l_linestatus;`, 1)
	assert.NotNil(t, err)
	fmt.Println(err.Error())
}
