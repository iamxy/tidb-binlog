package executor

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/flash"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/zanmato1984/clickhouse"
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
func (batch *flashRowBatch) Flush(chDB *chDB) (commitTS int64, err error) {
	for i := 0; i < pkgsql.MaxDMLRetryCount; i++ {
		if i > 0 {
			log.Warnf("[flush] Retrying %d flushing row batch %v in %d seconds", i, batch.sql, pkgsql.RetryWaitTime)
			time.Sleep(pkgsql.RetryWaitTime)
			_ = chDB.DB.Close()
			chDB.reopen()
		}
		commitTS, err = batch.flushInternal(chDB)
		if err == nil {
			return commitTS, nil
		}
		log.Warnf("[flush] Error %v when flushing row batch %v", err, batch.sql)
	}

	return commitTS, errors.Trace(err)
}

func (batch *flashRowBatch) flushInternal(chDB *chDB) (_ int64, err error) {
	log.Debugf("[flush] Flushing %d rows for \"%s\".", batch.Size(), batch.sql)
	defer func() {
		if err != nil {
			log.Warnf("[flush] Flushing rows for \"%s\" failed due to error %v.", batch.sql, err)
		} else {
			log.Debugf("[flush] Flushed %d rows for \"%s\".", batch.Size(), batch.sql)
		}
	}()

	if batch.Size() == 0 {
		return batch.latestCommitTS, nil
	}

	tx, err := chDB.DB.Begin()
	if err != nil {
		return batch.latestCommitTS, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
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
			log.Error("[flush] ", ce.StackTrace)
		}
		return batch.latestCommitTS, errors.Trace(err)
	}

	// Clearing all rows.
	// Loosing the space to tolerant a little more rows being added.
	batch.rows = make([][]interface{}, 0, batch.capacity+extraRowSize)

	return batch.latestCommitTS, nil
}

// chDB wraps DB connection information and the long-live connection to CH.
// Can be re-opened once retrying on error.
type chDB struct {
	hostAndPort pkgsql.CHHostAndPort
	user        string
	password    string
	blockSize   int
	DB          *sql.DB
}

func openChDB(hostAndPort pkgsql.CHHostAndPort, user string, password string, blockSize int) (*chDB, error) {
	chDB := &chDB{hostAndPort: hostAndPort, user: user, password: password, blockSize: blockSize}
	err := chDB.reopen()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return chDB, nil
}

func (chDB *chDB) reopen() (err error) {
	chDB.DB, err = pkgsql.OpenCH(chDB.hostAndPort.Host, chDB.hostAndPort.Port, chDB.user, chDB.password, "", chDB.blockSize)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type flashExecutor struct {
	sync.Mutex
	close chan bool
	wg    sync.WaitGroup

	timeLimit time.Duration
	sizeLimit int

	chDBs      []*chDB
	rowBatches map[string][]*flashRowBatch
	metaCP     *flash.MetaCheckpoint

	*baseError
}

func newFlash(cfg *DBConfig) (Executor, error) {
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

	chDBs := make([]*chDB, 0, len(hostAndPorts))
	for _, hostAndPort := range hostAndPorts {
		chDB, err := openChDB(hostAndPort, cfg.User, cfg.Password, sizeLimit)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chDBs = append(chDBs, chDB)
	}

	e := flashExecutor{
		close:      make(chan bool),
		timeLimit:  timeLimit,
		sizeLimit:  sizeLimit,
		chDBs:      chDBs,
		rowBatches: make(map[string][]*flashRowBatch),
		metaCP:     flash.GetInstance(),
		baseError:  newBaseError(),
	}

	e.wg.Add(1)
	go e.flushRoutine()

	return &e, nil
}

func (e *flashExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	e.Lock()
	defer e.Unlock()

	if e.err != nil {
		log.Errorf("[execute] Executor seeing error %v from the flush thread, exiting.", e.err)
		return errors.Trace(e.err)
	}

	if isDDL {
		// Flush all row batches.
		e.err = e.flushAll(true)
		if e.err != nil {
			log.Errorf("[execute] Executor seeing error %v when flushing, exiting.", e.err)
			return errors.Trace(e.err)
		}
		for _, chDB := range e.chDBs {
			e.err = pkgsql.ExecuteSQLs(chDB.DB, sqls, args, isDDL)
			if e.err != nil {
				return errors.Trace(e.err)
			}
		}
	} else {
		for i, row := range args {
			hashKey := e.partition(row[0].(int64))
			sql := sqls[i]
			args := row[1:]
			if _, ok := e.rowBatches[sql]; !ok {
				e.rowBatches[sql] = make([]*flashRowBatch, len(e.chDBs))
			}
			if e.rowBatches[sql][hashKey] == nil {
				e.rowBatches[sql][hashKey] = newFlashRowBatch(sql, e.sizeLimit)
			}
			rb := e.rowBatches[sql][hashKey]
			e.err = rb.AddRow(args, commitTSs[i])
			if e.err != nil {
				return errors.Trace(e.err)
			}

			// Check if size limit exceeded.
			if rb.Size() >= e.sizeLimit {
				_, e.err = rb.Flush(e.chDBs[hashKey])
				if e.err != nil {
					return errors.Trace(e.err)
				}
			}
		}
	}

	return nil
}

func (e *flashExecutor) Close() error {
	// Could have had error in async flush goroutine, log it.
	e.Lock()
	if e.err != nil {
		log.Error("[close] ", e.err)
	}
	e.Unlock()

	// Wait for async flush goroutine to exit.
	log.Info("[close] Waiting for flush thread to close.")
	close(e.close)

	hasError := false
	for _, chDB := range e.chDBs {
		err := chDB.DB.Close()
		if err != nil {
			hasError = true
			log.Error("[close] ", err)
		}
	}
	if hasError {
		return errors.New("error in closing some flash connector, check log for details")
	}
	return nil
}

func (e *flashExecutor) flushRoutine() {
	defer e.wg.Done()
	log.Info("[flush_thread] Flush thread started.")
	for {
		select {
		case <-e.close:
			log.Info("[flush_thread] Flush thread closing.")
			return
		case <-time.After(e.timeLimit):
			e.Lock()
			log.Debug("[flush_thread] Flush thread reached time limit, flushing.")
			if e.err != nil {
				e.Unlock()
				log.Errorf("[flush_thread] Flush thread seeing error %v from the executor, exiting.", errors.Trace(e.err))
				return
			}
			err := e.flushAll(false)
			if err != nil {
				e.Unlock()
				log.Errorf("[flush_thread] Flush thread seeing error %v when flushing, exiting.", errors.Trace(e.err))
				e.SetErr(err)
				return
			}
			// TODO: save checkpoint.
			e.Unlock()
		}
	}
}

// partition must be a index of dbs
func (e *flashExecutor) partition(key int64) int {
	return int(key) % len(e.chDBs)
}

func (e *flashExecutor) flushAll(forceSaveCP bool) error {
	log.Debug("[flush_all] Flushing all row batches.")

	// Pick the latest commitTS among all row batches.
	// TODO: consider if it's safe enough.
	maxCommitTS := int64(0)
	for _, rbs := range e.rowBatches {
		for i, rb := range rbs {
			if rb == nil {
				continue
			}
			lastestCommitTS, err := rb.Flush(e.chDBs[i])
			if err != nil {
				return errors.Trace(err)
			}
			if maxCommitTS < lastestCommitTS {
				maxCommitTS = lastestCommitTS
			}
		}
	}

	e.metaCP.Flush(maxCommitTS, forceSaveCP)
	return nil
}
