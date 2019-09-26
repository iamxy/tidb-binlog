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
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

var _ = check.Suite(&mysqlSuite{})

type mysqlSuite struct {
}

type fakeMySQLLoader struct {
	loader.Loader
	successes chan *loader.Txn
	input     chan *loader.Txn
}

func (l *fakeMySQLLoader) Input() chan<- *loader.Txn {
	return l.input
}

func (l *fakeMySQLLoader) Run() error {
	close(l.successes)
	return errors.New("MySQLSyncerMockTest")
}

func (l *fakeMySQLLoader) Successes() <-chan *loader.Txn {
	return l.successes
}

func (s *mysqlSuite) TestMySQLSyncerAvoidBlock(c *check.C) {
	var infoGetter translator.TableInfoGetter
	// create mysql syncer
	fakeMySQLLoaderImpl := &fakeMySQLLoader{
		successes: make(chan *loader.Txn),
		input:     make(chan *loader.Txn),
	}
	db, _, _ := sqlmock.New()
	syncer := &MysqlSyncer{
		db:         db,
		loader:     fakeMySQLLoaderImpl,
		baseSyncer: newBaseSyncer(infoGetter),
	}
	go syncer.run()
	gen := translator.BinlogGenrator{}
	gen.SetDDL()
	item := &Item{
		Binlog:        gen.TiBinlog,
		PrewriteValue: gen.PV,
		Schema:        gen.Schema,
		Table:         gen.Table,
	}
	select {
	case err := <-syncer.Error():
		c.Assert(err, check.ErrorMatches, ".*MySQLSyncerMockTest.*")
	case <-time.After(time.Second):
		c.Fatal("mysql syncer hasn't quit in 1s after some error occurs in loader")
	}

	finishSync := make(chan struct{})
	go func() {
		err := syncer.Sync(item)
		c.Assert(err, check.ErrorMatches, ".*MySQLSyncerMockTest.*")
		close(finishSync)
	}()
	select {
	case <-finishSync:
	case <-time.After(time.Second):
		c.Fatal("mysql syncer hasn't synced item in 1s after some error occurs in loader")
	}
}
