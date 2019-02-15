package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/diff"
	"github.com/pingcap/tidb-binlog/tests/dailytest"
	"github.com/pingcap/tidb-binlog/tests/util"
	"github.com/pingcap/tidb-tools/tidb-binlog/driver/reader"
	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
)

// drainer -> kafka, syn data from kafka to downstream TiDB, and run the dailytest
// most copy from github.com/pingcap/tidb-tools/tidb-binlog/driver/example/mysql/mysql.go
// TODO maybe later we can replace by the `new tool` package

var (
	kafkaAddr = flag.String("kafkaAddr", "127.0.0.1:9092", "kafkaAddr like 127.0.0.1:9092,127.0.0.1:9093")
	topic     = flag.String("topic", "", "topic name to consume binlog")
	offset    = flag.Int64("offset", sarama.OffsetNewest, "offset")
	commitTS  = flag.Int64("commitTS", 0, "commitTS")
)

func main() {
	flag.Parse()

	cfg := &reader.Config{
		KafkaAddr: strings.Split(*kafkaAddr, ","),
		Offset:    *offset,
		CommitTS:  *commitTS,
		Topic:     *topic,
	}

	breader, err := reader.NewReader(cfg)
	if err != nil {
		panic(err)
	}

	sourceDB, err := util.CreateSourceDB()
	if err != nil {
		panic(err)
	}

	sinkDB, err := util.CreateSinkDB()
	if err != nil {
		panic(err)
	}

	// start sync to mysql from kafka
	go func() {
		for {
			select {
			case msg := <-breader.Messages():
				str := msg.Binlog.String()
				if len(str) > 2000 {
					str = str[:2000] + "..."
				}
				log.Debug("recv: ", str)
				binlog := msg.Binlog
				sqls, args := toSQL(binlog)

				tx, err := sinkDB.Begin()
				if err != nil {
					log.Fatal(err)
				}

				for i := 0; i < len(sqls); i++ {
					// log.Debug("exec: args: ", sqls[i], args[i])
					_, err = tx.Exec(sqls[i], args[i]...)
					if err != nil {
						tx.Rollback()
						log.Fatal(err)
					}
				}
				err = tx.Commit()
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}()

	time.Sleep(5 * time.Second)

	// run the dailytest
	diffCfg := &diff.Config{
		EqualIndex:       true,
		EqualCreateTable: true,
		EqualRowCount:    true,
		EqualData:        true,
	}
	dailytest.Run(sourceDB, sinkDB, diffCfg, 10, 1000, 10)
}

func columnToArg(c *pb.Column) (arg interface{}) {
	if c.GetIsNull() {
		return nil
	}

	if c.Int64Value != nil {
		return c.GetInt64Value()
	}

	if c.Uint64Value != nil {
		return c.GetUint64Value()
	}

	if c.DoubleValue != nil {
		return c.GetDoubleValue()
	}

	if c.BytesValue != nil {
		return c.GetBytesValue()
	}

	return c.GetStringValue()
}

func tableToSQL(table *pb.Table) (sqls []string, sqlArgs [][]interface{}) {
	replace := func(row *pb.Row) {
		sql := fmt.Sprintf("replace into `%s`.`%s`", table.GetSchemaName(), table.GetTableName())

		var names []string
		var placeHolders []string
		for _, c := range table.GetColumnInfo() {
			names = append(names, c.GetName())
			placeHolders = append(placeHolders, "?")
		}
		sql += "(" + strings.Join(names, ",") + ")"
		sql += "values(" + strings.Join(placeHolders, ",") + ")"

		var args []interface{}
		for _, col := range row.GetColumns() {
			args = append(args, columnToArg(col))
		}

		sqls = append(sqls, sql)
		sqlArgs = append(sqlArgs, args)
	}

	constructWhere := func(args []interface{}) (sql string, usePK bool) {
		var whereColumns []string
		var whereArgs []interface{}
		for i, col := range table.GetColumnInfo() {
			if col.GetIsPrimaryKey() {
				whereColumns = append(whereColumns, col.GetName())
				whereArgs = append(whereArgs, args[i])
				usePK = true
			}
		}
		// no primary key
		if len(whereColumns) == 0 {
			for i, col := range table.GetColumnInfo() {
				whereColumns = append(whereColumns, col.GetName())
				whereArgs = append(whereArgs, args[i])
			}
		}

		sql = " where "
		for i, col := range whereColumns {
			if i != 0 {
				sql += " and "
			}

			if whereArgs[i] == nil {
				sql += fmt.Sprintf("%s IS NULL ", col)
			} else {
				sql += fmt.Sprintf("%s = ? ", col)
			}
		}

		sql += " limit 1"

		return
	}

	for _, mutation := range table.Mutations {
		switch mutation.GetType() {
		case pb.MutationType_Insert:
			replace(mutation.Row)
		case pb.MutationType_Update:
			columnInfo := table.GetColumnInfo()
			sql := fmt.Sprintf("update `%s`.`%s` set ", table.GetSchemaName(), table.GetTableName())
			// construct c1 = ?, c2 = ?...
			for i, col := range columnInfo {
				if i != 0 {
					sql += ","
				}
				sql += fmt.Sprintf("%s = ? ", col.Name)
			}

			row := mutation.Row
			changedRow := mutation.ChangeRow

			var args []interface{}
			// for set
			for _, col := range row.GetColumns() {
				args = append(args, columnToArg(col))
			}

			where, usePK := constructWhere(args)
			sql += where

			// for where
			for i, col := range changedRow.GetColumns() {
				if columnToArg(col) == nil {
					continue
				}
				if !usePK || columnInfo[i].GetIsPrimaryKey() {
					args = append(args, columnToArg(col))
				}
			}

			sqls = append(sqls, sql)
			sqlArgs = append(sqlArgs, args)

		case pb.MutationType_Delete:
			columnInfo := table.GetColumnInfo()
			row := mutation.Row

			var values []interface{}
			for _, col := range row.GetColumns() {
				values = append(values, columnToArg(col))
			}
			where, usePK := constructWhere(values)

			sql := fmt.Sprintf("delete from `%s`.`%s` %s", table.GetSchemaName(), table.GetTableName(), where)

			var args []interface{}
			for i, col := range row.GetColumns() {
				if columnToArg(col) == nil {
					continue
				}
				if !usePK || columnInfo[i].GetIsPrimaryKey() {
					args = append(args, columnToArg(col))
				}
			}

			sqls = append(sqls, sql)
			sqlArgs = append(sqlArgs, args)
		}
	}

	return
}

func isCreateDatabase(sql string) (isCreateDatabase bool, err error) {
	if !strings.Contains(strings.ToLower(sql), "database") {
		return false, nil
	}

	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return false, errors.Annotate(err, fmt.Sprintf("parse: %s", sql))
	}

	_, isCreateDatabase = stmt.(*ast.CreateDatabaseStmt)

	return
}

func toSQL(binlog *pb.Binlog) ([]string, [][]interface{}) {
	var allSQL []string
	var allArgs [][]interface{}

	switch binlog.GetType() {
	case pb.BinlogType_DDL:
		ddl := binlog.DdlData
		isCreateDatabase, err := isCreateDatabase(string(ddl.DdlQuery))
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}
		if !isCreateDatabase {
			sql := fmt.Sprintf("use %s", ddl.GetSchemaName())
			allSQL = append(allSQL, sql)
			allArgs = append(allArgs, nil)
		}
		allSQL = append(allSQL, string(ddl.DdlQuery))
		allArgs = append(allArgs, nil)

	case pb.BinlogType_DML:
		dml := binlog.DmlData
		for _, table := range dml.GetTables() {
			sqls, sqlArgs := tableToSQL(table)
			allSQL = append(allSQL, sqls...)
			allArgs = append(allArgs, sqlArgs...)
		}

	default:
		log.Fatal("unknown type: ", binlog.GetType())
	}

	return allSQL, allArgs
}
