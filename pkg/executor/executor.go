package executor

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type (
	QueryMode uint8
	Executor  interface {
		Query(query string, round uint) ([]*sql.Rows, error)
		Exec(query string, round uint) ([]sql.Result, error)
	}

	MySQLExecutor struct {
		db *sql.DB
	}
)

func (e *MySQLExecutor) Query(query string, round uint) (rowsList []*sql.Rows, err error) {
	rowsList = make([]*sql.Rows, 0, round)
	tx, err := e.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	defer func() {
		err = tx.Rollback()
	}()
	if err != nil {
		return
	}

	var i uint
	for i = 0; i < round; i++ {
		var rows *sql.Rows
		rows, err = tx.Query(query)
		if err != nil {
			return
		}
		rowsList = append(rowsList, rows)
	}
	return
}

func (e *MySQLExecutor) Exec(query string, round uint) (results []sql.Result, err error) {
	results = make([]sql.Result, 0, round)
	var i uint
	for i = 0; i < round; i++ {
		var tx *sql.Tx
		tx, err = e.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return
		}

		var result sql.Result
		result, err = tx.Exec(query)
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
	if err != nil {
		return nil, err
	}
	return &MySQLExecutor{db: db}, nil
}
