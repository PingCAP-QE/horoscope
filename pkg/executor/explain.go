package executor

import (
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/chaos-mesh/horoscope/pkg"
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
	if !data.Columns[0:7].Equal([]string{"id", "estRows", "actRows", "task", "access object", "execution info", "operator info"}) {
		return nil
	}
	var ei, lastInfo *ExplainAnalyzeInfo
	lastLevel := 0
	for index, row := range data.Data {
		op, level := parseAnalyzeID(*row[0])
		estRows := parseFloatColumn(*row[1])
		actRows := parseFloatColumn(*row[2])
		opInfo := *row[6]
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
	infos := []*CardinalityInfo{&CardinalityInfo{
		ExplainAnalyzeInfo: ei,
		QError:             pkg.QError(ei.EstRows, ei.ActRows),
	}}
	if len(ei.Items) != 0 {
		for _, e := range ei.Items {
			infos = append(infos, CollectEstAndActRows(e)...)
		}
	}
	return infos
}
