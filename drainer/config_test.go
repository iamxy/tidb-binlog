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
	"bytes"
	"fmt"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	"github.com/pingcap/check"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	dsync "github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/pkg/encrypt"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	"github.com/pingcap/tidb-binlog/pkg/util"
	pkgzk "github.com/pingcap/tidb-binlog/pkg/zk"
	"github.com/samuel/go-zookeeper/zk"
	"go.etcd.io/etcd/integration"
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
		"-metrics-addr", "192.168.15.10:9091",
		"-txn-batch", "1",
		"-data-dir", "data.drainer",
		"-dest-db-type", "mysql",
		"-config", "../cmd/drainer/drainer.toml",
		"-addr", "192.168.15.10:8257",
		"-advertise-addr", "192.168.15.10:8257",
	}

	cfg := NewConfig()
	err := cfg.Parse(args)
	c.Assert(err, IsNil)
	c.Assert(cfg.MetricsAddr, Equals, "192.168.15.10:9091")
	c.Assert(cfg.DataDir, Equals, "data.drainer")
	c.Assert(cfg.SyncerCfg.TxnBatch, Equals, 1)
	c.Assert(cfg.SyncerCfg.DestDBType, Equals, "mysql")
	c.Assert(cfg.SyncerCfg.To.Host, Equals, "127.0.0.1")
	var strSQLMode *string
	c.Assert(cfg.SyncerCfg.StrSQLMode, Equals, strSQLMode)
	c.Assert(cfg.SyncerCfg.SQLMode, Equals, mysql.SQLMode(0))
}

func (t *testDrainerSuite) TestValidateFilter(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.validateFilter(), IsNil)

	cfg = NewConfig()
	cfg.SyncerCfg.DoDBs = []string{""}
	c.Assert(cfg.validateFilter(), NotNil)

	cfg = NewConfig()
	cfg.SyncerCfg.IgnoreSchemas = "a,,c"
	c.Assert(cfg.validateFilter(), NotNil)

	emptyScheme := []filter.TableName{{Schema: "", Table: "t"}}
	emptyTable := []filter.TableName{{Schema: "s", Table: ""}}

	cfg = NewConfig()
	cfg.SyncerCfg.DoTables = emptyScheme
	c.Assert(cfg.validateFilter(), NotNil)

	cfg = NewConfig()
	cfg.SyncerCfg.DoTables = emptyTable
	c.Assert(cfg.validateFilter(), NotNil)

	cfg = NewConfig()
	cfg.SyncerCfg.IgnoreTables = emptyScheme
	c.Assert(cfg.validateFilter(), NotNil)

	cfg = NewConfig()
	cfg.SyncerCfg.IgnoreTables = emptyTable
	c.Assert(cfg.validateFilter(), NotNil)
}

func (t *testDrainerSuite) TestValidate(c *C) {
	cfg := NewConfig()

	cfg.ListenAddr = "http://123：9091"
	err := cfg.validate()
	c.Assert(err, ErrorMatches, ".*invalid addr.*")

	cfg.ListenAddr = "http://192.168.10.12:9091"
	err = cfg.validate()
	c.Assert(err, ErrorMatches, ".*invalid advertise-addr.*")

	cfg.AdvertiseAddr = "http://192.168.10.12:9091"
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

func (t *testDrainerSuite) TestEnableDisable(c *C) {
	truev := true
	falsev := false
	for _, enableFlag := range []*bool{nil, &truev, &falsev} {
		for _, enableFile := range []*bool{nil, &truev, &falsev} {
			for _, disableFlag := range []*bool{nil, &truev, &falsev} {
				for _, disableFile := range []*bool{nil, &truev, &falsev} {
					f, err := os.CreateTemp("", "test-enable")
					c.Assert(err, check.IsNil)

					fmt.Fprintf(f, "[syncer]\n")
					if enableFile != nil {
						fmt.Fprintf(f, "enable-dispatch = %v\n", *enableFile)
						fmt.Fprintf(f, "enable-detect = %v\n", *enableFile)
					}

					if disableFile != nil {
						fmt.Fprintf(f, "disable-dispatch = %v\n", *disableFile)
						fmt.Fprintf(f, "disable-detect = %v\n", *disableFile)
					}

					var args []string

					if enableFlag != nil {
						args = append(args, fmt.Sprintf("-enable-dispatch=%v", *enableFlag))
						args = append(args, fmt.Sprintf("-enable-detect=%v", *enableFlag))
					}

					if disableFlag != nil {
						args = append(args, fmt.Sprintf("-disable-dispatch=%v", *disableFlag))
						args = append(args, fmt.Sprintf("-disable-detect=%v", *disableFlag))
					}

					args = append(args, "--config", f.Name())

					cf := NewConfig()
					err = cf.Parse(args)
					c.Assert(err, check.IsNil)

					res := true
					if disableFlag != nil {
						res = !*disableFlag
					} else if disableFile != nil {
						res = !*disableFile
					} else if enableFlag != nil {
						res = *enableFlag
					} else if enableFile != nil {
						res = *enableFile
					}

					c.Log("config file: ", f.Name())
					c.Log(enableFlag, enableFile, disableFlag, disableFile)

					c.Assert(cf.SyncerCfg.EnableDispatch(), check.Equals, res)
					c.Assert(cf.SyncerCfg.EnableCausality(), check.Equals, res)
				}
			}
		}
	}
}

func (t *testDrainerSuite) TestAdjustConfig(c *C) {
	cfg := NewConfig()
	cfg.SyncerCfg.DestDBType = "pb"
	cfg.SyncerCfg.WorkerCount = 10

	err := cfg.adjustConfig()
	c.Assert(err, IsNil)
	c.Assert(cfg.SyncerCfg.DestDBType, Equals, "file")
	c.Assert(cfg.SyncerCfg.WorkerCount, Equals, 1)
	c.Assert(maxMsgSize, Equals, maxGrpcMsgSize)

	cfg = NewConfig()
	err = cfg.adjustConfig()
	c.Assert(err, IsNil)
	c.Assert(cfg.ListenAddr, Equals, "http://"+util.DefaultListenAddr(8249))
	c.Assert(cfg.AdvertiseAddr, Equals, cfg.ListenAddr)

	// test EncryptedPassword
	cfg = NewConfig()
	cfg.ListenAddr = "0.0.0.0:8257"
	cfg.AdvertiseAddr = "192.168.15.12:8257"
	err = cfg.adjustConfig()
	c.Assert(err, IsNil)
	c.Assert(cfg.ListenAddr, Equals, "http://0.0.0.0:8257")
	c.Assert(cfg.AdvertiseAddr, Equals, "http://192.168.15.12:8257")

	cfg = NewConfig()
	encrypted, err := encrypt.Encrypt("origin")
	c.Assert(err, IsNil)

	cfg.SyncerCfg.To = &dsync.DBConfig{
		EncryptedPassword: string(encrypted),
		Checkpoint: dsync.CheckpointConfig{
			EncryptedPassword: string(encrypted),
		},
	}
	err = cfg.adjustConfig()
	c.Assert(err, IsNil)
	c.Assert(cfg.SyncerCfg.To.Password, check.Equals, "origin")
	c.Assert(cfg.SyncerCfg.To.Checkpoint.Password, check.Equals, "origin")

	// test false positive
	cfg.SyncerCfg.To = &dsync.DBConfig{
		EncryptedPassword: "what ever" + string(encrypted),
		Checkpoint: dsync.CheckpointConfig{
			EncryptedPassword: "what ever" + string(encrypted),
		},
	}

	c.Logf("to.password: %v", cfg.SyncerCfg.To.Password)
	err = cfg.adjustConfig()
	c.Assert(err, NotNil)
}

func (t *testDrainerSuite) TestConfigParsingFileWithInvalidOptions(c *C) {
	yc := struct {
		DataDir                string `toml:"data-dir" json:"data-dir"`
		ListenAddr             string `toml:"addr" json:"addr"`
		AdvertiseAddr          string `toml:"advertise-addr" json:"advertise-addr"`
		UnrecognizedOptionTest bool   `toml:"unrecognized-option-test" json:"unrecognized-option-test"`
	}{
		"data.drainer",
		"192.168.15.10:8257",
		"192.168.15.10:8257",
		true,
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(yc)
	c.Assert(err, IsNil)

	configFilename := path.Join(c.MkDir(), "drainer_config_invalid.toml")
	err = os.WriteFile(configFilename, buf.Bytes(), 0644)
	c.Assert(err, IsNil)

	args := []string{
		"--config",
		configFilename,
		"-L", "debug",
	}

	cfg := NewConfig()
	err = cfg.Parse(args)
	c.Assert(err, ErrorMatches, ".*contained unknown configuration options: unrecognized-option-test.*")
}

var _ = Suite(&testKafkaSuite{})

type testKafkaSuite struct {
	origNewZKFromConnectionString func(connectionString string, dialTimeout, sessionTimeout time.Duration) (*pkgzk.Client, error)
}

func (t *testKafkaSuite) SetUpTest(c *C) {
	t.origNewZKFromConnectionString = newZKFromConnectionString
}

func (t *testKafkaSuite) TearDownTest(c *C) {
	newZKFromConnectionString = t.origNewZKFromConnectionString
}

type MockConn struct{}

func (m *MockConn) Close() {}
func (m *MockConn) Children(path string) ([]string, *zk.Stat, error) {
	return []string{"0", "1"}, nil, nil
}
func (m *MockConn) Get(path string) ([]byte, *zk.Stat, error) {
	if path[len(path)-1] == '0' {
		return []byte(`{"version":2,"host":"192.0.2.1","port":9092}`), nil, nil
	} else if path[len(path)-1] == '1' {
		return []byte(`{"version":2,"host":"192.0.2.2","port":9092}`), nil, nil
	}
	return nil, nil, nil
}

func (t *testKafkaSuite) TestConfigDestDBTypeKafka(c *C) {
	args := []string{
		"-metrics-addr", "192.168.15.10:9091",
		"-txn-batch", "1",
		"-data-dir", "data.drainer",
		"-dest-db-type", "kafka",
		"-config", "../cmd/drainer/drainer.toml",
		"-addr", "192.168.15.10:8257",
		"-advertise-addr", "192.168.15.10:8257",
	}
	newZKFromConnectionString = func(connectionString string, dialTimeout, sessionTimeout time.Duration) (client *pkgzk.Client, e error) {
		return pkgzk.NewWithConnection(&MockConn{}, nil), nil
	}

	// Without Zookeeper address
	cfg := NewConfig()
	err := cfg.Parse(args)
	c.Assert(err, IsNil)
	// kafka asserts
	c.Assert(cfg.SyncerCfg.To.KafkaAddrs, Matches, defaultKafkaAddrs)
	c.Assert(cfg.SyncerCfg.To.KafkaVersion, Equals, defaultKafkaVersion)
	c.Assert(cfg.SyncerCfg.To.KafkaMaxMessages, Equals, 1024)
	c.Assert(maxMsgSize, Equals, maxKafkaMsgSize)
	initializeSaramaGlobalConfig()
	c.Assert(sarama.MaxResponseSize, Equals, int32(maxKafkaMsgSize))
	c.Assert(sarama.MaxRequestSize, Equals, int32(maxKafkaMsgSize)+1)

	// With Zookeeper address and maxKafkaMsgSize
	maxInt32 := math.MaxInt32
	cfg = NewConfig()
	cfg.SyncerCfg.To = new(dsync.DBConfig)
	cfg.SyncerCfg.To.ZKAddrs = "host1:2181"
	cfg.SyncerCfg.To.KafkaMaxMessageSize = maxInt32
	err = cfg.Parse(args)
	c.Assert(err, IsNil)
	c.Assert(cfg.MetricsAddr, Equals, "192.168.15.10:9091")
	c.Assert(cfg.DataDir, Equals, "data.drainer")
	c.Assert(cfg.SyncerCfg.TxnBatch, Equals, 1)
	c.Assert(cfg.SyncerCfg.DestDBType, Equals, "kafka")
	var strSQLMode *string
	c.Assert(cfg.SyncerCfg.StrSQLMode, Equals, strSQLMode)
	c.Assert(cfg.SyncerCfg.SQLMode, Equals, mysql.SQLMode(0))
	// kafka asserts
	c.Assert(cfg.SyncerCfg.To.KafkaAddrs, Matches, `(192\.0\.2\.1:9092,192\.0\.2\.2:9092|192\.0\.2\.2:9092,192\.0\.2\.1:9092)`)
	c.Assert(cfg.SyncerCfg.To.KafkaVersion, Equals, defaultKafkaVersion)
	c.Assert(cfg.SyncerCfg.To.KafkaMaxMessages, Equals, 1024)
	c.Assert(maxMsgSize, Equals, maxInt32)
	initializeSaramaGlobalConfig()
	c.Assert(sarama.MaxResponseSize, Equals, int32(maxInt32))
	c.Assert(sarama.MaxRequestSize, Equals, int32(maxInt32))
	maxMsgSize = maxKafkaMsgSize
	initializeSaramaGlobalConfig()
}
