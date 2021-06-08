// Copyright 2019 PingCAP, Inc.
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

package loader

import (
	"crypto/tls"
	gosql "database/sql"
	"fmt"
	"hash/crc32"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb/errno"
)

var (
	// ErrTableNotExist means the table not exist.
	ErrTableNotExist   = errors.New("table not exist")
	defaultTiDBTxnMode = "optimistic"
)

const (
	colsSQL = `
SELECT column_name, extra FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?;`
	uniqKeysSQL = `
SELECT non_unique, index_name, seq_in_index, column_name 
FROM information_schema.statistics
WHERE table_schema = ? AND table_name = ?
ORDER BY seq_in_index ASC;`
)

type tableInfo struct {
	columns    []string
	primaryKey *indexInfo
	// include primary key if have
	uniqueKeys []indexInfo
}

type indexInfo struct {
	name    string
	columns []string
}

// getTableInfo returns information like (non-generated) column names and
// unique keys about the specified table
func getTableInfo(db *gosql.DB, schema string, table string) (info *tableInfo, err error) {
	info = new(tableInfo)

	if info.columns, err = getColsOfTbl(db, schema, table); err != nil {
		return nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table)
	}

	if info.uniqueKeys, err = getUniqKeys(db, schema, table); err != nil {
		return nil, errors.Trace(err)
	}

	// put primary key at first place
	// and set primaryKey
	for i := 0; i < len(info.uniqueKeys); i++ {
		if info.uniqueKeys[i].name == "PRIMARY" {
			info.uniqueKeys[i], info.uniqueKeys[0] = info.uniqueKeys[0], info.uniqueKeys[i]
			info.primaryKey = &info.uniqueKeys[0]
			break
		}
	}

	return
}

var customID int64

func isUnknownSystemVariableErr(err error) bool {
	code, ok := sql.GetSQLErrCode(err)
	if !ok {
		return strings.Contains(err.Error(), "Unknown system variable")
	}

	return code == errno.ErrUnknownSystemVariable
}

func createDBWitSessions(dsn string, params map[string]string) (db *gosql.DB, err error) {
	// Try set this sessions if it's supported.
	defaultParams := map[string]string{
		// After https://github.com/pingcap/tidb/pull/17102
		// default is false, must enable for insert value explicit, or can't replicate.
		"allow_auto_random_explicit_insert": "1",
		"tidb_txn_mode":                     defaultTiDBTxnMode,
	}
	var tryDB *gosql.DB
	tryDB, err = gosql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer tryDB.Close()

	support := make(map[string]string)
	for k, v := range defaultParams {
		s := fmt.Sprintf("SET SESSION %s = ?", k)
		_, err := tryDB.Exec(s, v)
		if err != nil {
			if isUnknownSystemVariableErr(err) {
				continue
			}
			return nil, errors.Trace(err)
		}

		support[k] = v
	}
	for k, v := range params {
		s := fmt.Sprintf("SET SESSION %s = ?", k)
		_, err := tryDB.Exec(s, v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		support[k] = v
	}

	for k, v := range support {
		dsn += fmt.Sprintf("&%s=%s", k, url.QueryEscape(v))
	}

	db, err = gosql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return
}

// CreateDBWithSQLMode return sql.DB
func CreateDBWithSQLMode(user string, password string, host string, port int, tlsConfig *tls.Config, sqlMode *string, params map[string]string) (db *gosql.DB, err error) {
	hosts := strings.Split(host, ",")

	if len(hosts) < 1 {
		return nil, errors.Annotate(err, "You must provide at least one mysql address")
	}

	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	index := random.Intn(len(hosts))
	h := hosts[index]

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4,utf8&interpolateParams=true&readTimeout=1m&multiStatements=true", user, password, h, port)
	if sqlMode != nil {
		// same as "set sql_mode = '<sqlMode>'"
		dsn += "&sql_mode='" + url.QueryEscape(*sqlMode) + "'"
	}

	if tlsConfig != nil {
		name := "custom_" + strconv.FormatInt(atomic.AddInt64(&customID, 1), 10)
		err := mysql.RegisterTLSConfig(name, tlsConfig)
		if err != nil {
			return nil, errors.Annotate(err, "failed to RegisterTLSConfig")
		}
		dsn += "&tls=" + name
	}

	return createDBWitSessions(dsn, params)
}

// CreateDB return sql.DB
func CreateDB(user string, password string, host string, port int, tls *tls.Config) (db *gosql.DB, err error) {
	return CreateDBWithSQLMode(user, password, host, port, tls, nil, nil)
}

func quoteSchema(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

func quoteName(name string) string {
	return "`" + escapeName(name) + "`"
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

func holderString(n int) string {
	builder := new(strings.Builder)
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	return builder.String()
}

func genHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func splitDMLs(dmls []*DML, size int) (res [][]*DML) {
	for i := 0; i < len(dmls); i += size {
		end := i + size
		if end > len(dmls) {
			end = len(dmls)
		}

		res = append(res, dmls[i:end])
	}
	return
}

func buildColumnList(names []string) string {
	var b strings.Builder
	for i, name := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(quoteName(name))

	}

	return b.String()
}

// getColsOfTbl returns a slice of the names of all columns,
// generated columns are excluded.
// https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/columns-table.html
func getColsOfTbl(db *gosql.DB, schema, table string) ([]string, error) {
	rows, err := db.Query(colsSQL, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	cols := make([]string, 0, 1)
	for rows.Next() {
		var name, extra string
		err = rows.Scan(&name, &extra)
		if err != nil {
			return nil, errors.Trace(err)
		}
		isGenerated := strings.Contains(extra, "VIRTUAL GENERATED") || strings.Contains(extra, "STORED GENERATED")
		if isGenerated {
			continue
		}
		cols = append(cols, name)
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	// if no any columns returns, means the table not exist.
	if len(cols) == 0 {
		return nil, ErrTableNotExist
	}

	return cols, nil
}

// https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/statistics-table.html
func getUniqKeys(db *gosql.DB, schema, table string) (uniqueKeys []indexInfo, err error) {
	rows, err := db.Query(uniqKeysSQL, schema, table)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	defer rows.Close()

	var nonUnique int
	var keyName string
	var columnName string
	var seqInIndex int // start at 1

	// get pk and uk
	// key for PRIMARY or other index name
	for rows.Next() {
		err = rows.Scan(&nonUnique, &keyName, &seqInIndex, &columnName)
		if err != nil {
			err = errors.Trace(err)
			return
		}

		if nonUnique == 1 {
			continue
		}

		var i int
		// Search for indexInfo with the current keyName
		for i = 0; i < len(uniqueKeys); i++ {
			if uniqueKeys[i].name == keyName {
				uniqueKeys[i].columns = append(uniqueKeys[i].columns, columnName)
				break
			}
		}
		// If we don't find the indexInfo with the loop above, create a new one
		if i == len(uniqueKeys) {
			uniqueKeys = append(uniqueKeys, indexInfo{keyName, []string{columnName}})
		}
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	return
}
