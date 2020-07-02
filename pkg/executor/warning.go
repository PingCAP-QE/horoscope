package executor

import (
	"errors"
	"github.com/go-sql-driver/mysql"
	"log"
	"strconv"
)

func Warning(row Row) error {
	if len(row) != 3 {
		return errors.New("warning table should have 3 columns")
	}

	code, err := strconv.Atoi(row[1])
	if err != nil {
		return err
	}

	log.Printf("warning: %d %s", code, row[2])

	return &mysql.MySQLError{Number: uint16(code), Message: row[2]}
}
