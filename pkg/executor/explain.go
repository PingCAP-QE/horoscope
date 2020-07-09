package executor

import (
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var operatorRegex = regexp.MustCompile(`[a-zA-Z]+`)

type ExplainAnalyzeInfo struct {
	Op      string
	EstRows float64
	ActRows float64
	Items   []*ExplainAnalyzeInfo
	parent  *ExplainAnalyzeInfo
}

func NewExplainAnalyzeInfo(data Rows) *ExplainAnalyzeInfo {
	if !data.Columns[0:3].Equal([]string{"id", "estRows", "actRows"}) {
		return nil
	}
	var ei, lastInfo *ExplainAnalyzeInfo
	lastLevel := 0
	for index, row := range data.Data {
		op, level := parseAnalyzeID(row[0])
		estRows := parseFloatColumn(row[1])
		actRows := parseFloatColumn(row[2])
		cur := &ExplainAnalyzeInfo{
			Op:      op,
			EstRows: estRows,
			ActRows: actRows,
			Items:   nil,
			parent:  nil,
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
