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

package sync

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/zanmato1984/clickhouse"
	"go.uber.org/zap"
)

var extraRowSize = 1024

// flashRowBatch is an in-memory row batch caching rows about to be passed to flash.
// It's not thread-safe, so callers must take care of the synchronizing.
type flashRowBatch struct {
	sql            string
	columnSize     int
	capacity       int
	rows           [][]interface{}
	latestCommitTS int64
}

func newFlashRowBatch(sql string, capacity int) *flashRowBatch {
	pos := strings.LastIndex(sql, "(")
	values := sql[pos:]
	columnSize := strings.Count(values, "?")
	// Loosing the space to tolerant a little more rows being added.
	rows := make([][]interface{}, 0, capacity+extraRowSize)
	return &flashRowBatch{
		sql:            sql,
		columnSize:     columnSize,
		capacity:       capacity,
		rows:           rows,
		latestCommitTS: 0,
	}
}

// AddRow appends single row into this row batch.
func (batch *flashRowBatch) AddRow(args []interface{}, commitTS int64) error {
	if len(args) != batch.columnSize {
		return errors.Errorf("Row %v column size %d mismatches the row batch column size %d", args, len(args), batch.columnSize)
	}
	batch.rows = append(batch.rows, args)

	if batch.latestCommitTS < commitTS {
		batch.latestCommitTS = commitTS
	}

	log.Debug(fmt.Sprintf("[add_row] Added row %v.", args))
	return nil
}

// Size returns the number of rows stored in this batch.
func (batch *flashRowBatch) Size() int {
	return len(batch.rows)
}

// Flush writes all the rows in this row batch into CH, with retrying when failure.
func (batch *flashRowBatch) Flush(db *sql.DB) (commitTS int64, err error) {
	for i := 0; i < pkgsql.MaxDMLRetryCount; i++ {
		if i > 0 {
			log.Warn("retrying flushing row batch", zap.String("sql", batch.sql), zap.Duration("RetryWaitTime", pkgsql.RetryWaitTime))
			time.Sleep(pkgsql.RetryWaitTime)
		}
		commitTS, err = batch.flushInternal(db)
		if err == nil {
			return commitTS, nil
		}
		log.Warn("flushing row batch failed", zap.Error(err), zap.String("sql", batch.sql))
	}

	return commitTS, errors.Trace(err)
}

func (batch *flashRowBatch) flushInternal(db *sql.DB) (_ int64, err error) {
	log.Debug("flushing row batch", zap.Int("size", batch.Size()), zap.String("sql", batch.sql))

	if batch.Size() == 0 {
		return batch.latestCommitTS, nil
	}

	tx, err := db.Begin()
	if err != nil {
		return batch.latestCommitTS, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			if err := tx.Rollback(); err != nil {
				log.Error(err.Error())
			}
		}
	}()

	stmt, err := tx.Prepare(batch.sql)
	if err != nil {
		return batch.latestCommitTS, errors.Trace(err)
	}
	defer stmt.Close()

	for _, row := range batch.rows {
		_, err = stmt.Exec(row...)
		if err != nil {
			return batch.latestCommitTS, errors.Trace(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		if ce, ok := err.(*clickhouse.Exception); ok {
			// Stack trace from server side could be very helpful for triaging problems.
			log.Error("commit failed", zap.String("stack trace", ce.StackTrace))
		}
		return batch.latestCommitTS, errors.Trace(err)
	}

	// Clearing all rows.
	// Loosing the space to tolerant a little more rows being added.
	batch.rows = make([][]interface{}, 0, batch.capacity+extraRowSize)

	return batch.latestCommitTS, nil
}

// FlashSyncer sync binlog to TiFlash
type FlashSyncer struct {
	sync.Mutex
	close chan bool
	wg    sync.WaitGroup

	timeLimit time.Duration
	sizeLimit int

	dbs []*sql.DB
	// [sql][len(dbs)], group by same sql and shard by hashKey
	rowBatches map[string][]*flashRowBatch

	items []*Item
	*baseSyncer
}

// openCH should only be change for unit test mock
var openCH = pkgsql.OpenCH

var _ Syncer = &FlashSyncer{}

// NewFlashSyncer returns a instance of FlashSyncer
func NewFlashSyncer(cfg *DBConfig, tableInfoGetter translator.TableInfoGetter) (*FlashSyncer, error) {
	timeLimit, err := time.ParseDuration(cfg.TimeLimit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: check time limit validity, and give a default value (and a warning) if invalid.

	sizeLimit, err := strconv.Atoi(cfg.SizeLimit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: check size limit validity, and give a default value (and a warning) if invalid.

	hostAndPorts, err := pkgsql.ParseCHAddr(cfg.Host)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbs := make([]*sql.DB, 0, len(hostAndPorts))
	for _, hostAndPort := range hostAndPorts {
		db, err := openCH(hostAndPort.Host, hostAndPort.Port, cfg.User, cfg.Password, "", sizeLimit)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dbs = append(dbs, db)
	}

	e := FlashSyncer{
		close:      make(chan bool),
		timeLimit:  timeLimit,
		sizeLimit:  sizeLimit,
		dbs:        dbs,
		rowBatches: make(map[string][]*flashRowBatch),
		baseSyncer: newBaseSyncer(tableInfoGetter),
	}

	e.wg.Add(1)
	go e.flushRoutine()

	return &e, nil
}

// Sync implements Syncer interface
func (e *FlashSyncer) Sync(item *Item) error {
	e.Lock()
	defer e.Unlock()

	if e.err != nil {
		return errors.Annotate(e.err, "executor seeing error from the flush thread")
	}

	tiBinlog := item.Binlog

	if tiBinlog.DdlJobId > 0 {
		// Flush all row batches.
		e.err = e.flushAll()
		if e.err != nil {
			return errors.Annotate(e.err, "executor seeing error when flushing")
		}

		sql, err := translator.GenFlashDDLSQL(string(tiBinlog.GetDdlQuery()), item.Schema)
		if err != nil {
			return errors.Annotate(err, "gen ddl sql fail")
		}

		var args [][]interface{}
		args = append(args, nil)
		for _, db := range e.dbs {
			e.err = pkgsql.ExecuteSQLs(db, []string{sql}, args, true)
			if e.err != nil {
				return errors.Trace(e.err)
			}

			e.success <- item
		}
	} else {
		sqls, args, err := translator.GenFlashSQLs(e.tableInfoGetter, item.PrewriteValue, tiBinlog.GetCommitTs())
		if err != nil {
			return errors.Annotate(err, "gen sqls fail")
		}

		for i, row := range args {
			hashKey := e.partition(row[0].(int64))
			sql := sqls[i]
			args := row[1:]
			if _, ok := e.rowBatches[sql]; !ok {
				e.rowBatches[sql] = make([]*flashRowBatch, len(e.dbs))
			}
			if e.rowBatches[sql][hashKey] == nil {
				e.rowBatches[sql][hashKey] = newFlashRowBatch(sql, e.sizeLimit)
			}
			rb := e.rowBatches[sql][hashKey]
			e.err = rb.AddRow(args, tiBinlog.GetCommitTs())
			if e.err != nil {
				return errors.Trace(e.err)
			}

			// Check if size limit exceeded.
			if rb.Size() >= e.sizeLimit {
				_, e.err = rb.Flush(e.dbs[hashKey])
				if e.err != nil {
					return errors.Trace(e.err)
				}
			}
		}

		e.items = append(e.items, item)
	}

	return nil
}

// Close implements Syncer interface
func (e *FlashSyncer) Close() error {
	// Could have had error in async flush goroutine, log it.
	e.Lock()
	if e.err != nil {
		log.Error("close see error", zap.Error(e.err))
	}
	e.Unlock()

	// Wait for async flush goroutine to exit.
	log.Info("Waiting for flush thread to close")
	close(e.close)

	hasError := false
	for _, db := range e.dbs {
		err := db.Close()
		if err != nil {
			hasError = true
			log.Error("close db failed", zap.Error(err))
		}
	}
	if hasError {
		return errors.New("error in closing some flash connector, check log for details")
	}

	return nil
}

func (e *FlashSyncer) flushRoutine() {
	defer e.wg.Done()
	log.Info("Flush thread started.")
	for {
		select {
		case <-e.close:
			log.Info("Flush thread closing.")
			return
		case <-time.After(e.timeLimit):
			e.Lock()
			log.Debug("Flush thread reached time limit, flushing.")
			if e.err != nil {
				e.Unlock()
				log.Error("Flush thread seeing error from the executor, exiting", zap.Error(e.err))
				return
			}
			err := e.flushAll()
			if err != nil {
				e.Unlock()
				log.Error("Flush thread seeing error when flushing, exiting.", zap.Error(e.err))
				e.setErr(err)
				return
			}
			e.Unlock()
		}
	}
}

// partition must be a index of dbs
func (e *FlashSyncer) partition(key int64) int {
	return int(key) % len(e.dbs)
}

func (e *FlashSyncer) flushAll() error {
	log.Debug("[flush_all] Flushing all row batches.")

	for _, rbs := range e.rowBatches {
		for i, rb := range rbs {
			if rb == nil {
				continue
			}
			_, err := rb.Flush(e.dbs[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	for _, item := range e.items {
		e.success <- item
	}
	e.items = nil

	return nil
}
