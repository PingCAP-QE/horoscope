package executor

import (
	"errors"
	"strconv"

	"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

func Warning(row Row) error {
	if len(row) != 3 {
		return errors.New("warning table should have 3 columns")
	}

	code, err := strconv.Atoi(row[1])
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"code": code,
		"msg":  row[2],
	}).Debug("sql warning")

	return &mysql.MySQLError{Number: uint16(code), Message: row[2]}
}
