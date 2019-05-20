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

package drainer

import (
	"testing"

	"github.com/coreos/etcd/integration"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
)

var testEtcdCluster *integration.ClusterV3

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)
	TestingT(t)
}

var _ = Suite(&testDrainerSuite{})

type testDrainerSuite struct{}

func (t *testDrainerSuite) TestConfig(c *C) {
	args := []string{
		"-metrics-addr", "127.0.0.1:9091",
		"-txn-batch", "1",
		"-data-dir", "data.drainer",
		"-dest-db-type", "mysql",
		"-config", "../cmd/drainer/drainer.toml",
	}

	cfg := NewConfig()
	err := cfg.Parse(args)
	c.Assert(err, IsNil)
	c.Assert(cfg.MetricsAddr, Equals, "127.0.0.1:9091")
	c.Assert(cfg.DataDir, Equals, "data.drainer")
	c.Assert(cfg.SyncerCfg.TxnBatch, Equals, 1)
	c.Assert(cfg.SyncerCfg.DestDBType, Equals, "mysql")
	c.Assert(cfg.SyncerCfg.To.Host, Equals, "127.0.0.1")
	var strSQLMode *string
	c.Assert(cfg.SyncerCfg.StrSQLMode, Equals, strSQLMode)
	c.Assert(cfg.SyncerCfg.SQLMode, Equals, mysql.SQLMode(0))
}

func (t *testDrainerSuite) TestValidate(c *C) {
	cfg := NewConfig()

	cfg.ListenAddr = "http://123：9091"
	err := cfg.validate()
	c.Assert(err, ErrorMatches, ".*ListenAddr.*")
	cfg.ListenAddr = "http://192.168.10.12:9091"

	cfg.EtcdURLs = "127.0.0.1:2379,127.0.0.1:2380"
	err = cfg.validate()
	c.Assert(err, ErrorMatches, ".*EtcdURLs.*")

	cfg.EtcdURLs = "http://127.0.0.1,http://192.168.12.12"
	err = cfg.validate()
	c.Assert(err, ErrorMatches, ".*EtcdURLs.*")

	cfg.EtcdURLs = "http://127.0.0.1:2379,http://192.168.12.12:2379"
	err = cfg.validate()
	c.Assert(err, IsNil)

	cfg.Compressor = "urada"
	err = cfg.validate()
	c.Assert(err, ErrorMatches, ".*Invalid compressor.*")

	cfg.Compressor = "gzip"
	err = cfg.validate()
	c.Assert(err, IsNil)
}

func (t *testDrainerSuite) TestAdjustConfig(c *C) {
	cfg := NewConfig()
	cfg.SyncerCfg.DestDBType = "pb"
	cfg.SyncerCfg.WorkerCount = 10
	cfg.SyncerCfg.DisableDispatch = false

	err := cfg.adjustConfig()
	c.Assert(err, IsNil)
	c.Assert(cfg.SyncerCfg.DestDBType, Equals, "file")
	c.Assert(cfg.SyncerCfg.WorkerCount, Equals, 1)
	c.Assert(cfg.SyncerCfg.DisableDispatch, IsTrue)
}
