package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewExplainAnalyzeInfo(t *testing.T) {
	rows := Rows{
		Columns: [][]byte{[]byte("id"), []byte("estRows"), []byte("actRows"), []byte("access object"), []byte("operator info")},
		Data: []Row{
			[][]byte{[]byte("HashAgg_30"), []byte("1.00"), []byte("1"), []byte(""), []byte("funcs:min(imdb.char_name.name)->Column#43, funcs:min(imdb.title.title)->Column#44")},
			[][]byte{[]byte("└─HashJoin_43"), []byte("74878.55"), []byte("405"), []byte(""), []byte("inner join, equal:[eq(imdb.movie_companies.company_type_id, imdb.company_type.id)]")},
			[][]byte{[]byte("  ├─TableReader_274(Build)"), []byte("4.00"), []byte("4"), []byte(""), []byte("data:TableFullScan_273")},
			[][]byte{[]byte("  │ └─TableFullScan_273"), []byte("4.00"), []byte("4"), []byte("table:ct"), []byte("keep order:false")},
			[][]byte{[]byte("  └─HashJoin_70(Probe)"), []byte("74878.55"), []byte("405"), []byte("table:ct"), []byte("inner join, equal:[eq(imdb.movie_companies.company_id, imdb.company_name.id)]")},
		},
	}
	got := NewExplainAnalyzeInfo(rows)
	require.Equal(t, got.Op, "HashAgg")
	require.Equal(t, got.Items[0].Op, "HashJoin")
	require.Equal(t, got.Items[0].Items[0].Op, "TableReader")
	require.Equal(t, got.Items[0].Items[0].Items[0].Op, "TableFullScan")
	require.Equal(t, got.Items[0].Items[1].Op, "HashJoin")
}
