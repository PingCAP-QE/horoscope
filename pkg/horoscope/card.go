package horoscope

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/chaos-mesh/horoscope/pkg/executor"
)

type CardinalityQueryType string

const (
	TypeEMQ CardinalityQueryType = "emq"
	TypeRGE CardinalityQueryType = "rge"
	TypeDCT CardinalityQueryType = "dct"
)

type Cardinalitor struct {
	exec         executor.Executor
	Type         CardinalityQueryType
	TableColumns map[string][]string
	Timeout      time.Duration
}

func NewCardinalitor(exec executor.Executor, tableColumns map[string][]string, typ CardinalityQueryType, timeout time.Duration) *Cardinalitor {
	return &Cardinalitor{
		exec:         exec,
		Type:         typ,
		TableColumns: tableColumns,
		Timeout:      timeout,
	}
}

func (c *Cardinalitor) Test() (map[string]map[string]*Metrics, error) {
	result := make(map[string]map[string]*Metrics)
	var fun func(ctx context.Context, tableName, columnName string) (*Metrics, error)
	switch c.Type {
	case TypeEMQ:
		fun = c.testEMQ
	case TypeRGE:
		fun = c.testREG
	default:
		panic(fmt.Sprintf("illegal type %s", c.Type))
	}
	ctx := context.TODO()
	if c.Timeout != time.Duration(0) {
		ctx, _ = context.WithTimeout(context.TODO(), c.Timeout)
	}
	for tableName, columns := range c.TableColumns {
		if _, e := result[tableName]; !e {
			result[tableName] = make(map[string]*Metrics)
		}
		tableMap := result[tableName]
		for _, columnName := range columns {
			m, err := fun(ctx, tableName, columnName)
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

func (c *Cardinalitor) testEMQ(ctx context.Context, tableName, columnName string) (metrics *Metrics, err error) {
	const batchSize = 1000
	metrics = &Metrics{}
	rows, err := c.exec.Query(fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM %s", columnName, tableName))
	if err != nil {
		return nil, fmt.Errorf("fetch count(distinct %s) from %s occurred an error: %v", columnName, tableName, err)
	}
	count, err := strconv.ParseInt(string(rows.Data[0][0]), 10, 64)
	if err != nil {
		return nil, err
	}
	for i := 0; i*batchSize < int(count); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		rows, err := c.exec.Query(fmt.Sprintf("SELECT DISTINCT %s FROM %s ORDER BY %s LIMIT %d, %d",
			columnName, tableName, columnName, i*batchSize, batchSize,
		))
		if err != nil {
			return nil, err
		}
		for _, row := range rows.Data {
			value, op := row[0], "IS"
			stringValue, bareValue := "NULL", "NULL"
			if value != nil {
				bareValue, op = strings.Replace(string(value), "'", "\\'", -1), "="
				stringValue = fmt.Sprintf("'%s'", bareValue)
			}
			rows, _, err := c.exec.ExplainAnalyze(fmt.Sprintf("SELECT %s FROM %s WHERE %s %s %s", columnName, tableName, columnName, op, stringValue))
			if err != nil {
				return nil, err
			}
			cis := executor.CollectEstAndActRows(executor.NewExplainAnalyzeInfo(rows))
			qError := cis[0].QError
			if qError != math.Inf(1) {
				metrics.Values = append(metrics.Values, qError)
			}

			log.WithFields(log.Fields{
				"table":   tableName,
				"column":  columnName,
				"value":   bareValue,
				"q-error": qError,
			}).Info("q-error result")
		}
	}
	return metrics, nil
}

func (c *Cardinalitor) testREG(ctx context.Context, tableName, columnName string) (metrics *Metrics, err error) {
	metrics = &Metrics{}
	rows, err := c.exec.Query(fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM %s WHERE %s IS NOT NULL", columnName, tableName, columnName))
	if err != nil {
		return nil, fmt.Errorf("fetch count(distinct %s) from %s occurred an error: %v", columnName, tableName, err)
	}
	count, err := strconv.ParseInt(string(rows.Data[0][0]), 10, 64)
	if err != nil {
		return nil, err
	}
	for lbIndex := 0; lbIndex < int(count)-1; lbIndex++ {
		rows, err = c.exec.Query(fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL ORDER BY %s LIMIT %d, 1",
			columnName, tableName, columnName, columnName, lbIndex,
		))
		if err != nil {
			return nil, err
		}
		if len(rows.Data) == 0 {
			return
		}
		lb := string(rows.Data[0][0])
		for upIndex := lbIndex + 1; upIndex < int(count); upIndex++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			rows, err = c.exec.Query(fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL ORDER BY %s LIMIT %d, 1",
				columnName, tableName, columnName, columnName, upIndex,
			))
			if err != nil {
				return nil, err
			}
			if len(rows.Data) == 0 {
				break
			}
			ub := string(rows.Data[0][0])
			rows, _, err := c.exec.ExplainAnalyze(fmt.Sprintf("SELECT %s FROM %s WHERE %s >= '%s' and %s < '%s'",
				columnName, tableName,
				columnName, lb,
				columnName, ub))
			if err != nil {
				return nil, err
			}
			cis := executor.CollectEstAndActRows(executor.NewExplainAnalyzeInfo(rows))
			qError := cis[0].QError
			if qError != math.Inf(1) {
				metrics.Values = append(metrics.Values, qError)
			}
			log.WithFields(log.Fields{
				"table":   tableName,
				"column":  columnName,
				"lb":      lb,
				"ub":      ub,
				"q-error": qError,
			}).Info("q-error result")
		}
	}
	return
}
