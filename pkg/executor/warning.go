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
	"errors"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/errno"
	log "github.com/sirupsen/logrus"
)

func Warning(row Row) (warning error, err error) {
	if len(row) != 3 {
		err = errors.New("warning table should have 3 columns")
		return
	}

	code, err := strconv.Atoi(string(row[1]))
	if err != nil {
		return
	}

	log.WithFields(log.Fields{
		"code": code,
		"msg":  string(row[2]),
	}).Debug("sql warning")

	warning = &mysql.MySQLError{Number: uint16(code), Message: string(row[2])}
	return
}

func PlanOutOfRange(err error) bool {
	mysqlErr, ok := err.(*mysql.MySQLError)
	return ok && mysqlErr.Number == errno.ErrUnknown && strings.Contains(strings.ToLower(mysqlErr.Message), "nth_plan")
}
