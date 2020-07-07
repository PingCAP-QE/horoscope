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
		GetHints(query string) (Hints, error)
		IsSamePlan(q1, q2 string) (equal bool, err error)
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

	return task(tx)
}

func (e *MySQLExecutor) Query(query string) (Rows, error) {
	var row Rows
	err := e.EnterTx(&sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead}, func(tx *sql.Tx) error {
		data, err := tx.Query(query)
		if err != nil {
			return err
		}

		row, err = NewRows(data)
		if err != nil {
			return err
		}

		err = queryWarning(tx)
		if err != nil {
			return err
		}
		return nil
	})
	return row, err
}

func (e *MySQLExecutor) Exec(query string) (result Result, err error) {
	err = e.EnterTx(&sql.TxOptions{Isolation: sql.LevelReadCommitted}, func(tx *sql.Tx) error {
		data, err := tx.Exec(query)
		if err != nil {
			return err
		}

		result, err = NewResult(data)
		if err != nil {
			return err
		}

		err = queryWarning(tx)
		if err != nil {
			return err
		}

		return nil
	})
	return
}

func (e *MySQLExecutor) IsSamePlan(q1, q2 string) (equal bool, err error) {
	var h1, h2 Hints
	err = e.EnterTx(&sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadCommitted}, func(tx *sql.Tx) (err error) {
		h1, err = getHints(tx, q1)
		if err != nil {
			return
		}
		h2, err = getHints(tx, q2)
		return
	})
	if err != nil {
		return
	}
	h1.RemoveNTHPlan()
	h2.RemoveNTHPlan()
	equal = h1.Equal(h2)
	return
}

func (e *MySQLExecutor) GetHints(query string) (hints Hints, err error) {
	_ = e.EnterTx(nil, func(tx *sql.Tx) error {
		hints, err = getHints(tx, query)
		return nil
	})
	return
}

func NewExecutor(dsn string) (Executor, error) {
	db, err := sql.Open("mysql", dsn)
	return &MySQLExecutor{db: db}, err
}

func queryWarning(tx *sql.Tx) (err error) {
	data, err := tx.Query("SHOW WARNINGS;")
	if err != nil {
		return
	}
	rows, err := NewRows(data)
	if err != nil {
		return
	}

	for _, row := range rows {
		return Warning(row)
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
	if len(rows) != 1 || len(rows[0]) != 1 {
		err = errors.New(fmt.Sprintf("Unexpected hint explanation: %#v", rows))
		return
	}
	hints = NewHints(rows[0][0])

	log.WithFields(log.Fields{
		"query": query,
		"hints": hints,
	}).Debug("hints of query")
	return
}
