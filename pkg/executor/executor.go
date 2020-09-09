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
	"database/sql"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

type (
	QueryMode uint8

	Pool interface {
		Executor() Executor
		Transaction() (Transaction, error)
	}

	RawExecutor interface {
		Query(query string, args ...interface{}) (*sql.Rows, error)
		Exec(query string, args ...interface{}) (sql.Result, error)
	}

	RawTransaction interface {
		RawExecutor
		Commit() error
		Rollback() error
	}

	Executor interface {
		Query(query string) (Rows, error)
		QueryStream(query string) (RowStream, error)
		Exec(query string) (Result, error)
		GetHints(query string) (Hints, error)
		Explain(query string) (Rows, []error, error)
		ExplainAnalyze(query string) (Rows, []error, error)
	}

	Transaction interface {
		Executor
		Commit() error
		Rollback() error
	}

	PoolOptions struct {
		MaxOpenConns   uint `json:"max_open_conns"`
		MaxIdleConns   uint `json:"max_idle_conns"`
		MaxLifeSeconds uint `json:"max_life_seconds"`
	}

	PoolImpl struct {
		db *sql.DB
	}

	ExecutorImpl struct {
		exec RawExecutor
	}

	TransactionImpl struct {
		ExecutorImpl
		tx RawTransaction
	}
)

func NewPool(dsn string, options *PoolOptions) (pool Pool, err error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return
	}

	if options.MaxOpenConns != 0 {
		db.SetMaxOpenConns(int(options.MaxOpenConns))
	}

	if options.MaxIdleConns != 0 {
		db.SetMaxIdleConns(int(options.MaxIdleConns))
	}

	if options.MaxLifeSeconds != 0 {
		db.SetConnMaxLifetime(time.Second * time.Duration(options.MaxLifeSeconds))
	}

	pool = &PoolImpl{
		db: db,
	}
	return pool, err
}

func (e *ExecutorImpl) Query(query string) (rows Rows, err error) {
	data, err := e.exec.Query(query)
	if err != nil {
		return
	}
	rows, err = NewRows(data)
	return
}

func (e *ExecutorImpl) QueryStream(query string) (stream RowStream, err error) {
	data, err := e.exec.Query(query)
	if err != nil {
		return
	}
	stream, err = NewRowStream(data)
	return
}

func (e *ExecutorImpl) Exec(query string) (result Result, err error) {
	data, err := e.exec.Exec(query)
	if err != nil {
		return
	}
	result, err = NewResult(data)
	return
}

/// GetHints would query plan out of range warnings
func (e *ExecutorImpl) GetHints(query string) (hints Hints, err error) {
	explanation := fmt.Sprintf("explain format = 'hint' %s", query)
	rawRows, err := e.exec.Query(explanation)
	if err != nil {
		return
	}
	rows, err := NewRows(rawRows)
	if err != nil {
		return
	}
	if rows.RowCount() != 1 || rows.ColumnNums() != 1 {
		err = errors.New(fmt.Sprintf("Unexpected hints: %#v", rows))
		return
	}
	hints = NewHints(string(rows.Data[0][0]))

	log.WithFields(log.Fields{
		"query": query,
		"hints": hints,
	}).Debug("hints of query")
	return
}

func (e *ExecutorImpl) Explain(query string) (rows Rows, warnings []error, err error) {
	rows, err = e.Query(fmt.Sprintf("EXPLAIN %s", query))
	if err != nil {
		err = fmt.Errorf("explain error: %v", err)
		return
	}
	warnings, err = e.queryWarnings()
	return
}

func (e *ExecutorImpl) ExplainAnalyze(query string) (rows Rows, warnings []error, err error) {
	rows, err = e.Query(fmt.Sprintf("EXPLAIN ANALYZE %s", query))
	if err != nil {
		err = fmt.Errorf("explain error: %v", err)
		return
	}
	warnings, err = e.queryWarnings()
	return
}

func (e *ExecutorImpl) queryWarnings() (warnings []error, err error) {
	data, err := e.exec.Query("SHOW WARNINGS;")
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

func (t *TransactionImpl) Commit() error {
	return t.tx.Commit()
}

func (t *TransactionImpl) Rollback() error {
	return t.tx.Rollback()
}

func (p *PoolImpl) Executor() Executor {
	return &ExecutorImpl{exec: p.db}
}

func (p *PoolImpl) Transaction() (Transaction, error) {
	tx, err := p.db.Begin()
	return &TransactionImpl{ExecutorImpl: ExecutorImpl{exec: tx}, tx: tx}, err
}
