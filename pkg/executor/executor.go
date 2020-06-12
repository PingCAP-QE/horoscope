package executor

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type (
	QueryMode uint8
	Executor  interface {
		Query(query string, round uint) ([]Rows, error)
		Exec(query string, round uint) ([]Result, error)
	}

	MySQLExecutor struct {
		db *sql.DB
	}
)

func (e *MySQLExecutor) Query(query string, round uint) (rowsList []Rows, err error) {
	rowsList = make([]Rows, 0, round)
	tx, err := e.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	defer func() {
		err = tx.Rollback()
	}()
	if err != nil {
		return
	}

	var i uint
	for i = 0; i < round; i++ {
		var data *sql.Rows
		var row Rows
		data, err = tx.Query(query)
		if err != nil {
			return
		}

		row, err = NewRows(data)
		if err != nil {
			return
		}
		rowsList = append(rowsList, row)
	}
	return
}

func (e *MySQLExecutor) Exec(query string, round uint) (results []Result, err error) {
	results = make([]Result, 0, round)
	var i uint
	for i = 0; i < round; i++ {
		var tx *sql.Tx
		tx, err = e.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return
		}

		var data sql.Result
		var result Result
		data, err = tx.Exec(query)
		if err != nil {
			return
		}

		result, err = NewResult(data)
		if err != nil {
			return
		}

		results = append(results, result)
		err = tx.Rollback()
		if err != nil {
			return
		}
	}
	return
}

func NewExecutor(dsn string) (Executor, error) {
	db, err := sql.Open("mysql", dsn)
	return &MySQLExecutor{db: db}, err
}
