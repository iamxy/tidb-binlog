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
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"path"
	"sort"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

const (
	maxKafkaMsgSize = 1024 * 1024 * 1024
	maxGrpcMsgSize  = math.MaxInt32
)

var (
	maxMsgSize = maxGrpcMsgSize
)

// taskGroup is a wrapper of `sync.WaitGroup`.
type taskGroup struct {
	wg sync.WaitGroup
}

func (g *taskGroup) Go(name string, f func()) {
	g.start(name, f, false)
}

func (g *taskGroup) GoNoPanic(name string, f func()) {
	g.start(name, f, true)
}

func (g *taskGroup) start(name string, f func(), noPanic bool) {
	fName := zap.String("name", name)
	g.wg.Add(1)
	go func() {
		defer func() {
			if noPanic {
				if err := recover(); err != nil {
					log.Error("Recovered from panic",
						zap.Reflect("err", err),
						zap.Stack("real stack"),
						fName,
					)
				}
			}
			log.Info("Exit", fName)
			g.wg.Done()
		}()
		f()
	}()
}

func (g *taskGroup) Wait() {
	g.wg.Wait()
}

// GenCheckPointCfg returns an CheckPoint config instance
func GenCheckPointCfg(cfg *Config, id uint64) (*checkpoint.Config, error) {
	checkpointCfg := &checkpoint.Config{
		ClusterID:       id,
		InitialCommitTS: cfg.InitialCommitTS,
		CheckPointFile:  path.Join(cfg.DataDir, "savepoint"),
	}

	toCheckpoint := cfg.SyncerCfg.To.Checkpoint

	if toCheckpoint.Schema != "" {
		checkpointCfg.Schema = toCheckpoint.Schema
	}

	switch toCheckpoint.Type {
	case "mysql", "tidb":
		checkpointCfg.CheckpointType = toCheckpoint.Type
		checkpointCfg.Db = &checkpoint.DBConfig{
			Host:     toCheckpoint.Host,
			User:     toCheckpoint.User,
			Password: toCheckpoint.Password,
			Port:     toCheckpoint.Port,
			TLS:      toCheckpoint.TLS,
		}
	case "":
		switch cfg.SyncerCfg.DestDBType {
		case "mysql", "tidb", "plugin":
			checkpointCfg.CheckpointType = cfg.SyncerCfg.DestDBType
			checkpointCfg.Db = &checkpoint.DBConfig{
				Host:     cfg.SyncerCfg.To.Host,
				User:     cfg.SyncerCfg.To.User,
				Password: cfg.SyncerCfg.To.Password,
				Port:     cfg.SyncerCfg.To.Port,
				TLS:      cfg.SyncerCfg.To.TLS,
			}
		case "pb", "file":
			checkpointCfg.CheckpointType = "file"
		case "kafka":
			checkpointCfg.CheckpointType = "file"
		case "flash":
			return nil, errors.New("the flash DestDBType is no longer supported")
		default:
			return nil, errors.Errorf("unknown DestDBType: %s", cfg.SyncerCfg.DestDBType)
		}
	default:
		return nil, errors.Errorf("unknown checkpoint type: %s", toCheckpoint.Type)
	}

	return checkpointCfg, nil
}

func initializeSaramaGlobalConfig() {
	sarama.MaxResponseSize = int32(maxKafkaMsgSize)
	// add 1 to avoid confused log: Producer.MaxMessageBytes must be smaller than MaxRequestSize; it will be ignored
	sarama.MaxRequestSize = int32(maxKafkaMsgSize) + 1
}

func getDDLJob(tiStore kv.Storage, id int64) (*model.Job, error) {
	snapMeta, err := getSnapshotMeta(tiStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	job, err := snapMeta.GetHistoryDDLJob(id)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return job, nil
}

// loadHistoryDDLJobs loads all history DDL jobs from TiDB
func loadHistoryDDLJobs(tiStore kv.Storage) ([]*model.Job, error) {
	snapMeta, err := getSnapshotMeta(tiStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// jobs from GetAllHistoryDDLJobs are sorted by job id, need sorted by schema version
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].BinlogInfo.SchemaVersion < jobs[j].BinlogInfo.SchemaVersion
	})

	return jobs, nil
}

func getSnapshotMeta(tiStore kv.Storage) (*meta.Meta, error) {
	version, err := tiStore.CurrentVersion(oracle.GlobalTxnScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot := tiStore.GetSnapshot(version)
	return meta.NewSnapshotMeta(snapshot), nil
}

func genDrainerID(listenAddr string) (string, error) {
	urllis, err := url.Parse(listenAddr)
	if err != nil {
		return "", errors.Trace(err)
	}

	_, port, err := net.SplitHostPort(urllis.Host)
	if err != nil {
		return "", errors.Trace(err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", errors.Trace(err)
	}

	return fmt.Sprintf("%s:%s", hostname, port), nil
}
