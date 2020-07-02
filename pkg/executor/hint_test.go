package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestCase struct {
	origin, expect string
	fail           bool
}

var HintsTestCases = []TestCase{
	{
		origin: "use_index(@`sel_1` `test`.`region` ), use_index(@`sel_1` `test`.`nation` ), hash_join(@`sel_1` `test`.`region`), use_index(@`sel_1` `test`.`supplier` ), hash_join(@`sel_1` `test`.`supplier`), use_index(@`sel_1` `test`.`partsupp` ), hash_join(@`sel_1` `test`.`partsupp`), use_index(@`sel_1` `test`.`part` ), inl_merge_join(@`sel_1` ), use_index(@`sel_2` `test`.`region` ), use_index(@`sel_2` `test`.`nation` ), hash_join(@`sel_2` `test`.`region`), use_index(@`sel_2` `test`.`supplier` ), hash_join(@`sel_2` `test`.`supplier`), use_index(@`sel_2` `test`.`partsupp` ), hash_join(@`sel_2` `test`.`partsupp`), hash_agg(@`sel_2`)",
		expect: "use_index(@`sel_1` `test`.`region` ), use_index(@`sel_1` `test`.`nation` ), inl_join(@`sel_1` `test`.`region`), use_index(@`sel_1` `test`.`supplier` ), hash_join(@`sel_1` `test`.`supplier`), use_index(@`sel_1` `test`.`partsupp` ), hash_join(@`sel_1` `test`.`partsupp`), use_index(@`sel_1` `test`.`part` ), inl_join(@`sel_1` `test`.`part`), use_index(@`sel_2` `test`.`region` ), use_index(@`sel_2` `test`.`nation` ), inl_join(@`sel_2` `test`.`region`), use_index(@`sel_2` `test`.`supplier` ), hash_join(@`sel_2` `test`.`supplier`), use_index(@`sel_2` `test`.`partsupp` ), hash_join(@`sel_2` `test`.`partsupp`), hash_agg(@`sel_2`), nth_plan(1)",
		fail:   true,
	},
}

func TestHints_Equal(t *testing.T) {
	for _, testCase := range HintsTestCases {
		h1 := NewHints(testCase.origin)
		h2 := NewHints(testCase.expect)
		h1.RemoveNTHPlan()
		h2.RemoveNTHPlan()
		if testCase.fail {
			assert.False(t, h1.Equal(h2))
		} else {
			assert.True(t, h1.Equal(h2))
		}
	}
}
