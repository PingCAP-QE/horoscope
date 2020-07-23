package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewExplainAnalyzeInfo(t *testing.T) {
	rows := Rows{
		Columns: [][]byte{[]byte("id"), []byte("estRows"), []byte("actRows"), []byte("task"), []byte("access object"), []byte("execution info"), []byte("operator info")},
		Data: []Row{
			[][]byte{[]byte("HashAgg_30"), []byte("1.00"), []byte("1"), []byte("root"), []byte(""), []byte("time:5.143960214s, loops:2, PartialConcurrency:5, FinalConcurrency:5"), []byte("funcs:min(imdb.char_name.name)->Column#43, funcs:min(imdb.title.title)->Column#44")},
			[][]byte{[]byte("└─HashJoin_45"), []byte("76450.66"), []byte("405"), []byte("root"), []byte(""), []byte("time:5.143791993s, loops:6, Concurrency:5, probe collision:0, build:32.444µs"), []byte("inner join, equal:[eq(imdb.movie_companies.company_type_id, imdb.company_type.id)]")},
			[][]byte{[]byte("  ├─TableReader_308(Build)"), []byte("4.00"), []byte("4"), []byte("root"), []byte(""), []byte("time:20.912442ms, loops:2, rpc num: 1, rpc time:20.926766ms, proc keys:4"), []byte("data:TableFullScan_307")},
			[][]byte{[]byte("  │ └─TableFullScan_307"), []byte("4.00"), []byte("4"), []byte("cop[tikv]"), []byte("table:ct"), []byte("time:0s, loops:1"), []byte("keep order:false")},
			[][]byte{[]byte("  └─HashJoin_74(Probe)"), []byte("76450.66"), []byte("405"), []byte("root"), []byte(""), []byte("time:5.143556164s, loops:6, Concurrency:5, probe collision:0, build:850.482µs"), []byte("")},
		},
	}
	got := NewExplainAnalyzeInfo(rows)
	require.Equal(t, got.Op, "HashAgg")
	require.Equal(t, got.Items[0].Op, "HashJoin")
	require.Equal(t, got.Items[0].Items[0].Op, "TableReader")
	require.Equal(t, got.Items[0].Items[0].Items[0].Op, "TableFullScan")
	require.Equal(t, got.Items[0].Items[1].Op, "HashJoin")
}
