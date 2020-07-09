// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

type (
	QueryMode uint8
	Executor  interface {
		Query(query string) (Rows, error)
		Exec(query string) (Result, error)
		GetHints(query string) (Hints, []error, error)
		Explain(query string) (Rows, error)
		ExplainAnalyze(query string) (Rows, error)
	}

	MySQLExecutor struct {
		db *sql.DB
	}
)

func (e *MySQLExecutor) EnterTx(options *sql.TxOptions, task func(tx *sql.Tx) error) (err error) {
	tx, err := e.db.BeginTx(context.Background(), options)
	if err != nil {
		return err
	}
	defer func() {
		rollbackError := tx.Rollback()
		if err == nil {
			err = rollbackError
		}
	}()
	err = task(tx)
	return
}

func (e *MySQLExecutor) Query(query string) (Rows, error) {
	var row Rows
	err := e.EnterTx(nil, func(tx *sql.Tx) error {
		data, err := tx.Query(query)
		if err != nil {
			return err
		}

		row, err = NewRows(data)
		if err != nil {
			return err
		}
		return nil
	})
	return row, err
}

func (e *MySQLExecutor) Exec(query string) (result Result, err error) {
	err = e.EnterTx(&sql.TxOptions{Isolation: sql.LevelDefault}, func(tx *sql.Tx) error {
		data, err := tx.Exec(query)
		if err != nil {
			return err
		}
		result, err = NewResult(data)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

/// GetHints would query plan out of range warnings
func (e *MySQLExecutor) GetHints(query string) (hints Hints, warnings []error, err error) {
	_ = e.EnterTx(nil, func(tx *sql.Tx) (_ error) {
		hints, err = getHints(tx, query)
		if err != nil {
			return
		}
		// TODO: check warning in "Explain"
		warnings, err = queryWarnings(tx)
		return
	})
	return
}

func (e *MySQLExecutor) Explain(query string) (rows Rows, err error) {
	rows, err = e.Query(fmt.Sprintf("EXPLAIN %s", query))
	if err != nil {
		return Rows{}, fmt.Errorf("explain error: %v", err)
	}
	return
}

func (e *MySQLExecutor) ExplainAnalyze(query string) (rows Rows, err error) {
	rows, err = e.Query(fmt.Sprintf("EXPLAIN ANALYZE %s", query))
	if err != nil {
		return Rows{}, fmt.Errorf("explain analyze error: %v", err)
	}
	return
}

func NewExecutor(dsn string) (Executor, error) {
	db, err := sql.Open("mysql", dsn)
	return &MySQLExecutor{db: db}, err
}

func queryWarnings(tx *sql.Tx) (warnings []error, err error) {
	data, err := tx.Query("SHOW WARNINGS;")
	if err != nil {
		return
	}
	rows, err := NewRows(data)
	if err != nil {
		return
	}

	warnings = make([]error, 0)
	var warning error
	for _, row := range rows.Data {
		warning, err = Warning(row)
		if err != nil {
			return
		}
		warnings = append(warnings, warning)
	}

	return
}

func getHints(tx *sql.Tx, query string) (hints Hints, err error) {
	explanation := fmt.Sprintf("explain format = 'hint' %s", query)
	rawRows, err := tx.Query(explanation)
	if err != nil {
		return
	}
	rows, err := NewRows(rawRows)
	if err != nil {
		return
	}
	if rows.RowCount() != 1 || rows.ColumnNums() != 1 {
		err = errors.New(fmt.Sprintf("Unexpected hint explanation: %#v", rows))
		return
	}
	hints = NewHints(rows.Data[0][0])

	log.WithFields(log.Fields{
		"query": query,
		"hints": hints,
	}).Debug("hints of query")
	return
}
