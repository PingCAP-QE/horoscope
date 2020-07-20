package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewExplainAnalyzeInfo(t *testing.T) {
	rows := Rows{
		Columns: []string{"id", "estRows", "actRows", "access object", "operator info"},
		Data: []NullableRow{
			[]string{"HashAgg_30", "1.00", "1", "", "funcs:min(imdb.char_name.name)->Column#43, funcs:min(imdb.title.title)->Column#44"},
			[]string{"└─HashJoin_43", "74878.55", "405", "", "inner join, equal:[eq(imdb.movie_companies.company_type_id, imdb.company_type.id)]"},
			[]string{"  ├─TableReader_274(Build)", "4.00", "4", "", "data:TableFullScan_273"},
			[]string{"  │ └─TableFullScan_273", "4.00", "4", "table:ct", "keep order:false"},
			[]string{"  └─HashJoin_70(Probe)", "74878.55", "405", "table:ct", "inner join, equal:[eq(imdb.movie_companies.company_id, imdb.company_name.id)]"},
		},
	}
	got := NewExplainAnalyzeInfo(rows)
	require.Equal(t, got.Op, "HashAgg")
	require.Equal(t, got.Items[0].Op, "HashJoin")
	require.Equal(t, got.Items[0].Items[0].Op, "TableReader")
	require.Equal(t, got.Items[0].Items[0].Items[0].Op, "TableFullScan")
	require.Equal(t, got.Items[0].Items[1].Op, "HashJoin")
}
