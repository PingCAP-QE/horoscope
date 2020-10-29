package horoscope

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/chaos-mesh/horoscope/pkg/executor"
)

type CardinalityQueryType string

const (
	TypeEMQ CardinalityQueryType = "emq"
	TypeRGE CardinalityQueryType = "rge"
	TypeDCT CardinalityQueryType = "dct"

	defaultConcurrency = 30
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

func (c *Cardinalitor) Test() (map[string]map[string]map[string]*Metrics, error) {
	result := make(map[string]map[string]map[string]*Metrics)
	var fun func(ctx context.Context, tableName, columnName string) (map[string]*Metrics, error)
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
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.TODO(), c.Timeout)
		defer cancel()
	}
	for tableName, columns := range c.TableColumns {
		if _, e := result[tableName]; !e {
			result[tableName] = make(map[string]map[string]*Metrics)
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
				"q-error 50th": m["all"].quantile(0.50),
				"q-error 90th": m["all"].quantile(0.90),
				"q-error 95th": m["all"].quantile(0.95),
				"q-error max":  m["all"].quantile(1),
			}).Infof("q-error for %s.%s", tableName, columnName)
			tableMap[columnName] = m
		}
	}
	return result, nil
}

func (c *Cardinalitor) testEMQ(ctx context.Context, tableName, columnName string) (metrics map[string]*Metrics, err error) {
	var mLock sync.Mutex
	metrics = make(map[string]*Metrics)
	metrics["all"] = &Metrics{}
	metrics["most_common_10%"] = &Metrics{}
	metrics["least_common_10%"] = &Metrics{}

	rows, err := c.exec.Query(fmt.Sprintf("SELECT COUNT(DISTINCT(%s)) FROM %s", columnName, tableName))
	if err != nil {
		return nil, fmt.Errorf("fetch total count error: %v", err)
	}
	tot, err := strconv.Atoi(string(rows.Data[0][0]))
	if err != nil {
		return nil, fmt.Errorf("fetch total count error: %v", err)
	}

	p10 := tot / 10
	rows, err = c.exec.Query(fmt.Sprintf("SELECT %s FROM %s GROUP BY %s ORDER BY COUNT(*) DESC LIMIT %v", columnName, tableName, columnName, p10))
	if err != nil {
		return nil, fmt.Errorf("fetch most common values error: %v", err)
	}
	mcvs := make(map[string]struct{})
	for _, d := range rows.Data {
		mcvs[string(d[0])] = struct{}{}
	}
	rows, err = c.exec.Query(fmt.Sprintf("SELECT %s FROM %s GROUP BY %s ORDER BY COUNT(*) LIMIT %v", columnName, tableName, columnName, p10))
	if err != nil {
		return nil, fmt.Errorf("fetch least common values error: %v", err)
	}
	lcvs := make(map[string]struct{})
	for _, d := range rows.Data {
		lcvs[string(d[0])] = struct{}{}
	}

	rows, err = c.exec.Query(fmt.Sprintf("SELECT DISTINCT(%s) FROM %s", columnName, tableName))
	if err != nil {
		return nil, fmt.Errorf("fetch distinct error: %v", err)
	}

	rowCh := make(chan executor.Row, defaultConcurrency)
	var wg sync.WaitGroup
	for i := 0; i < defaultConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for true {
				row, ok := <-rowCh
				if !ok {
					return
				}
				value, op := row[0], "IS"
				stringValue, bareValue := "NULL", "NULL"
				if value != nil {
					bareValue, op = strings.Replace(string(value), "'", "\\'", -1), "="
					stringValue = fmt.Sprintf("'%s'", bareValue)
				}
				rows, _, err := c.exec.ExplainAnalyze(fmt.Sprintf("SELECT %s FROM %s WHERE %s %s %s", columnName, tableName, columnName, op, stringValue))
				if err != nil {
					log.Fatalln(err)
					return
				}
				cis := executor.CollectEstAndActRows(executor.NewExplainAnalyzeInfo(rows))
				if len(cis) == 0 {
					continue
				}
				qError := cis[0].QError
				if qError != math.Inf(1) {
					mLock.Lock()
					metrics["all"].Values = append(metrics["all"].Values, qError)
					if _, ok := mcvs[string(value)]; ok {
						metrics["most_common_10%"].Values = append(metrics["most_common_10%"].Values, qError)
					}
					if _, ok := lcvs[string(value)]; ok {
						metrics["least_common_10%"].Values = append(metrics["least_common_10%"].Values, qError)
					}
					mLock.Unlock()
				}

				log.WithFields(log.Fields{
					"table":   tableName,
					"column":  columnName,
					"value":   bareValue,
					"q-error": qError,
				}).Info("q-error result")
			}
		}()
	}
	for _, row := range rows.Data {
		rowCh <- row
	}
	close(rowCh)
	wg.Wait()
	return metrics, err
}

func (c *Cardinalitor) testREG(ctx context.Context, tableName, columnName string) (metrics map[string]*Metrics, err error) {
	metrics = make(map[string]*Metrics)
	metrics["all"] = &Metrics{}
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
				metrics["all"].Values = append(metrics["all"].Values, qError)
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
