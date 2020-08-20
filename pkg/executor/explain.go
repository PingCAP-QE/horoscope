package executor

import (
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/chaos-mesh/horoscope/pkg/utils"
)

var operatorRegex = regexp.MustCompile(`[a-zA-Z]+`)

type ExplainAnalyzeInfo struct {
	Op      string
	EstRows float64
	ActRows float64
	OpInfo  string
	Items   []*ExplainAnalyzeInfo
	parent  *ExplainAnalyzeInfo
}

type CardinalityInfo struct {
	*ExplainAnalyzeInfo
	QError float64
}

func NewExplainAnalyzeInfo(data Rows) *ExplainAnalyzeInfo {
	if !data.Columns[0:7].Equal([][]byte{[]byte("id"), []byte("estRows"), []byte("actRows"), []byte("task"), []byte("access object"), []byte("execution info"), []byte("operator info")}) {
		return nil
	}
	var ei, lastInfo *ExplainAnalyzeInfo
	lastLevel := 0
	for index, row := range data.Data {
		op, level := parseAnalyzeID(string(row[0]))
		estRows := parseFloatColumn(string(row[1]))
		actRows := parseFloatColumn(string(row[2]))
		opInfo := string(row[6])
		cur := &ExplainAnalyzeInfo{
			Op:      op,
			EstRows: estRows,
			ActRows: actRows,
			OpInfo:  opInfo,
		}
		if index == 0 {
			ei, lastInfo = cur, cur
		} else {
			levelDiff := level - lastLevel
			for i := 0; i < -levelDiff+1; i++ {
				lastInfo = lastInfo.parent
			}
			lastInfo.Items = append(lastInfo.Items, cur)
			cur.parent = lastInfo
		}
		lastInfo = cur
		lastLevel = level
	}
	return ei
}

func parseAnalyzeID(str string) (op string, level int) {
	level = utf8.RuneCountInString(str[0:strings.IndexAny(str, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")]) / 2
	op = operatorRegex.FindStringSubmatch(str)[0]
	return
}

func parseFloatColumn(str string) float64 {
	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		panic(err)
	}
	return f
}

func CollectEstAndActRows(ei *ExplainAnalyzeInfo) []*CardinalityInfo {
	if ei == nil {
		return nil
	}
	var infos []*CardinalityInfo
	if ei.EstRows > 0 && ei.ActRows > 0 {
		infos = append(infos, &CardinalityInfo{
			ExplainAnalyzeInfo: ei,
			QError:             utils.QError(ei.EstRows, ei.ActRows),
		})
	}
	if len(ei.Items) != 0 {
		for _, e := range ei.Items {
			infos = append(infos, CollectEstAndActRows(e)...)
		}
	}
	return infos
}
