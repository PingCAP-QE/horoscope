package executor

import "database/sql"

type Result struct {
	LastInsertId, RowsAffected int64
}

func NewResult(result sql.Result) (ret Result, err error) {
	ret.LastInsertId, err = result.LastInsertId()
	if err != nil {
		return
	}

	ret.RowsAffected, err = result.RowsAffected()
	return
}
