package syncer

// execute sql to mysql/tidb

import (
	"database/sql"
	"sync"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	Port     int    `toml:"port" json:"port"`
}

type mysqlSyncer struct {
	db *sql.DB

	loader *loader.Loader

	loaderQuit chan struct{}
	loaderErr  error
}

var _ Syncer = &mysqlSyncer{}

func newMysqlSyncer(cfg *DBConfig) (*mysqlSyncer, error) {
	db, err := loader.CreateDB(cfg.User, cfg.Password, cfg.Host, cfg.Port)
	if err != nil {
		log.Infof("create db failed %v", err)
		return nil, errors.Trace(err)
	}

	loader, err := loader.NewLoader(db, loader.WorkerCount(16), loader.BatchSize(20))
	if err != nil {
		return nil, errors.Annotate(err, "new loader failed")
	}

	syncer := &mysqlSyncer{db: db, loader: loader}
	syncer.runLoader()

	return syncer, nil
}

type item struct {
	binlog *pb.Binlog
	cb     func(binlog *pb.Binlog)
}

func (m *mysqlSyncer) Sync(pbBinlog *pb.Binlog, cb func(binlog *pb.Binlog)) error {
	txn, err := pbBinlogToTxn(pbBinlog)
	if err != nil {
		return errors.Annotate(err, "pbBinlogToTxn failed")
	}

	item := &item{binlog: pbBinlog, cb: cb}
	txn.Metadata = item

	select {
	case <-m.loaderQuit:
		return m.loaderErr
	case m.loader.Input() <- txn:
		return nil
	}
}

func (m *mysqlSyncer) Close() error {
	m.loader.Close()

	<-m.loaderQuit

	m.db.Close()

	return m.loaderErr
}

func (m *mysqlSyncer) runLoader() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for txn := range m.loader.Successes() {
			item := txn.Metadata.(*item)
			item.cb(item.binlog)
		}
		log.Info("Successes chan quit")
		wg.Done()
	}()

	m.loaderQuit = make(chan struct{})
	m.loaderErr = nil
	go func() {
		err := m.loader.Run()
		if err != nil {
			m.loaderErr = err
		}
		wg.Wait()
		close(m.loaderQuit)
	}()
}
