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
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// DMLType represents the dml type
type DMLType int

// DMLType types
const (
	UnknownDMLType DMLType = 0
	InsertDMLType  DMLType = 1
	UpdateDMLType  DMLType = 2
	DeleteDMLType  DMLType = 3
)

// DML holds the dml info
type DML struct {
	Database string
	Table    string

	Tp DMLType
	// only set when Tp = UpdateDMLType
	OldValues map[string]interface{}
	Values    map[string]interface{}

	info *tableInfo
}

// DDL holds the ddl info
type DDL struct {
	Database string
	Table    string
	SQL      string
	// should skip to execute this DDL at downstream and just refresh the downstream table info.
	// one case for this usage is for bidirectional replication and only execute DDL at one side.
	ShouldSkip bool
}

// Txn holds transaction info, an DDL or DML sequences
type Txn struct {
	DMLs []*DML
	DDL  *DDL

	AppliedTS int64

	// This field is used to hold arbitrary data you wish to include so it
	// will be available when receiving on the Successes channel
	Metadata interface{}
}

// AppendDML append a dml
func (t *Txn) AppendDML(dml *DML) {
	t.DMLs = append(t.DMLs, dml)
}

// NewDDLTxn return a Txn
func NewDDLTxn(db string, table string, sql string) *Txn {
	txn := new(Txn)
	txn.DDL = &DDL{
		Database: db,
		Table:    table,
		SQL:      sql,
	}

	return txn
}

func (t *Txn) String() string {
	if t.isDDL() {
		return fmt.Sprintf("{ddl: %s}", t.DDL.SQL)
	}

	return fmt.Sprintf("dml: %v", t.DMLs)
}

func (t *Txn) isDDL() bool {
	return t.DDL != nil
}

func (dml *DML) primaryKeys() []string {
	if dml.info.primaryKey == nil {
		return nil
	}

	return dml.info.primaryKey.columns
}

func (dml *DML) primaryKeyValues() []interface{} {
	names := dml.primaryKeys()

	values := make([]interface{}, 0, len(names))
	for _, name := range names {
		v := dml.Values[name]
		values = append(values, v)
	}

	return values
}

func (dml *DML) formatKey() string {
	return formatKey(dml.primaryKeyValues())
}

func (dml *DML) updateKey() bool {
	if len(dml.OldValues) == 0 {
		return false
	}

	values := dml.primaryKeyValues()
	oldValues := dml.oldPrimaryKeyValues()

	for i := 0; i < len(values); i++ {
		if values[i] != oldValues[i] {
			return true
		}
	}

	return false
}

func (dml *DML) String() string {
	return fmt.Sprintf("{db: %s, table: %s,tp: %v values: %d old_values: %d}",
		dml.Database, dml.Table, dml.Tp, len(dml.Values), len(dml.OldValues))
}

func (dml *DML) oldPrimaryKeyValues() []interface{} {
	if len(dml.OldValues) == 0 {
		return dml.primaryKeyValues()
	}

	names := dml.primaryKeys()

	values := make([]interface{}, 0, len(names))
	for _, name := range names {
		v := dml.OldValues[name]
		values = append(values, v)
	}

	return values
}

// TableName returns the fully qualified name of the DML's table
func (dml *DML) TableName() string {
	return quoteSchema(dml.Database, dml.Table)
}

func (dml *DML) updateSQL() (sql string, args []interface{}) {
	builder := new(strings.Builder)

	fmt.Fprintf(builder, "UPDATE %s SET ", dml.TableName())

	for _, name := range dml.columnNames() {
		if len(args) > 0 {
			builder.WriteByte(',')
		}
		arg := dml.Values[name]
		fmt.Fprintf(builder, "%s = ?", quoteName(name))
		args = append(args, arg)
	}

	builder.WriteString(" WHERE ")

	whereArgs := dml.buildWhere(builder)
	args = append(args, whereArgs...)

	builder.WriteString(" LIMIT 1")
	sql = builder.String()
	return
}

func (dml *DML) buildWhere(builder *strings.Builder) (args []interface{}) {
	wnames, wargs := dml.whereSlice()
	for i := 0; i < len(wnames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if wargs[i] == nil {
			builder.WriteString(quoteName(wnames[i]) + " IS NULL")
		} else {
			builder.WriteString(quoteName(wnames[i]) + " = ?")
			args = append(args, wargs[i])
		}
	}
	return
}

func (dml *DML) whereValues(names []string) (values []interface{}) {
	valueMap := dml.Values
	if dml.Tp == UpdateDMLType {
		valueMap = dml.OldValues
	}

	for _, name := range names {
		v := valueMap[name]
		values = append(values, v)
	}
	return
}

func (dml *DML) whereSlice() (colNames []string, args []interface{}) {
	// Try to use unique key values when available
	for _, index := range dml.info.uniqueKeys {
		values := dml.whereValues(index.columns)
		notAnyNil := true
		for i := 0; i < len(values); i++ {
			if values[i] == nil {
				notAnyNil = false
				break
			}
		}
		if notAnyNil {
			return index.columns, values
		}
	}

	// Fallback to use all columns
	names := dml.columnNames()
	return names, dml.whereValues(names)
}

func (dml *DML) deleteSQL() (sql string, args []interface{}) {
	builder := new(strings.Builder)

	fmt.Fprintf(builder, "DELETE FROM %s WHERE ", dml.TableName())
	args = dml.buildWhere(builder)
	builder.WriteString(" LIMIT 1")

	sql = builder.String()
	return
}

func (dml *DML) columnNames() []string {
	names := make([]string, 0, len(dml.Values))

	for name := range dml.Values {
		names = append(names, name)
	}

	// why need sort
	sort.Strings(names)
	return names
}

func (dml *DML) replaceSQL() (sql string, args []interface{}) {
	names := dml.columnNames()
	sql = fmt.Sprintf("REPLACE INTO %s(%s) VALUES(%s)", dml.TableName(), buildColumnList(names), holderString(len(names)))
	for _, name := range names {
		v := dml.Values[name]
		args = append(args, v)
	}
	return
}

func (dml *DML) insertSQL() (sql string, args []interface{}) {
	sql, args = dml.replaceSQL()
	sql = strings.Replace(sql, "REPLACE", "INSERT", 1)
	return
}

func (dml *DML) sql() (sql string, args []interface{}) {
	switch dml.Tp {
	case InsertDMLType:
		return dml.insertSQL()
	case UpdateDMLType:
		return dml.updateSQL()
	case DeleteDMLType:
		return dml.deleteSQL()
	}

	log.Debug("get sql for dml", zap.Reflect("dml", dml), zap.String("sql", sql), zap.Reflect("args", args))

	return
}

func formatKey(values []interface{}) string {
	builder := new(strings.Builder)
	for i, v := range values {
		if i != 0 {
			builder.WriteString("--")
		}
		fmt.Fprintf(builder, "%v", v)
	}

	return builder.String()
}

func getKey(names []string, values map[string]interface{}) string {
	builder := new(strings.Builder)
	for _, name := range names {
		v := values[name]
		if v == nil {
			continue
		}

		fmt.Fprintf(builder, "(%s: %v)", name, v)
	}

	return builder.String()
}

func getKeys(dml *DML) (keys []string) {
	info := dml.info

	tableName := dml.TableName()

	var addOldKey int
	var addNewKey int

	for _, index := range info.uniqueKeys {
		key := getKey(index.columns, dml.Values)
		if len(key) > 0 {
			addNewKey++
			keys = append(keys, key+tableName)
		}
	}

	if dml.Tp == UpdateDMLType {
		for _, index := range info.uniqueKeys {
			key := getKey(index.columns, dml.OldValues)
			if len(key) > 0 {
				addOldKey++
				keys = append(keys, key+tableName)
			}
		}
	}

	if addNewKey == 0 {
		key := getKey(info.columns, dml.Values) + tableName
		key = strconv.Itoa(int(genHashKey(key)))
		keys = append(keys, key)
	}

	if dml.Tp == UpdateDMLType && addOldKey == 0 {
		key := getKey(info.columns, dml.OldValues) + tableName
		key = strconv.Itoa(int(genHashKey(key)))
		keys = append(keys, key)
	}

	return
}
