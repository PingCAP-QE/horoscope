package horoscope

import (
	"fmt"
	"strconv"

	"github.com/chaos-mesh/horoscope/pkg/executor"
	log "github.com/sirupsen/logrus"
)

type CardinalityQueryType int

const (
	TypeEMQ CardinalityQueryType = iota
	TypeDCT
	TypeRGE
)

type Cardinalitor struct {
	exec            executor.Executor
	Type            CardinalityQueryType
	TableColumns    map[string][]string
	MaxLimitRequest int
}

func NewCardinalitor(exec executor.Executor, typ CardinalityQueryType, tableColumns map[string][]string) *Cardinalitor {
	return &Cardinalitor{
		exec:         exec,
		Type:         typ,
		TableColumns: tableColumns,
	}
}

func (c *Cardinalitor) Test() (map[string]map[string]*Metrics, error) {
	result := make(map[string]map[string]*Metrics)
	var fun func(tableName, columnName string) (*Metrics, error)
	switch c.Type {
	case TypeEMQ:
		fun = c.testEMQ
	default:
		panic("implement me!")
	}
	for tableName, columns := range c.TableColumns {
		if _, e := result[tableName]; !e {
			result[tableName] = make(map[string]*Metrics)
		}
		tableMap := result[tableName]
		for _, columnName := range columns {
			m, err := fun(tableName, columnName)
			if err != nil {
				return nil, err
			}
			log.WithFields(log.Fields{
				"table":        tableName,
				"column":       columnName,
				"q-error 50th": m.quantile(0.50),
				"q-error 90th": m.quantile(0.90),
				"q-error 95th": m.quantile(0.95),
				"q-error max":  m.quantile(1),
			}).Infof("q-error for %s.%s", tableName, columnName)
			tableMap[columnName] = m
		}
	}
	return result, nil
}

func (c *Cardinalitor) testEMQ(tableName, columnName string) (*Metrics, error) {
	const batchSize = 1000
	rows, err := c.exec.Query(fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM %s", columnName, tableName))
	if err != nil {
		return nil, fmt.Errorf("fetch count(distinct %s) from %s occurred an error: %v", columnName, tableName, err)
	}
	count, err := strconv.ParseInt(rows.Data[0][0], 10, 64)
	if err != nil {
		return nil, err
	}
	metrics := &Metrics{}
	for i := 0; i*batchSize < int(count); i++ {
		rows, err := c.exec.Query(fmt.Sprintf("SELECT DISTINCT %s FROM %s ORDER BY %s LIMIT %d, %d",
			columnName, tableName, columnName, i*batchSize, batchSize,
		))
		if err != nil {
			return nil, err
		}
		for _, row := range rows.Data {
			value := row[0]
			ei, err := c.exec.ExplainAnalyze(fmt.Sprintf("SELECT %s FROM %s WHERE %s = '%s'", columnName, tableName, columnName, value))
			if err != nil {
				return nil, err
			}
			cis := executor.CollectEstAndActRows(ei)
			qError := cis[0].QError
			metrics.Values = append(metrics.Values, qError)
		}
	}
	return metrics, nil
}
