package main

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
)

//PluginFactory is the Factory struct
type PluginFactory struct{}

//NewPluginFactory is factory function of plugin
func NewPluginFactory() interface{} {
	log.Info("call NewPluginFactory")
	return PluginFactory{}
}

//NewSyncerPlugin return A syncer instance which implemented interface of sync.Syncer
func (pf PluginFactory) NewSyncerPlugin(
	cfg *sync.DBConfig,
	file string,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	info *loopbacksync.LoopBackSync,
	enableDispatch bool,
	enableCausility bool,
) (sync.Syncer, error) {
	return sync.NewSyncerDemo(cfg, file, tableInfoGetter, worker, batchSize, queryHistogramVec, sqlMode,
		destDBType, relayer, info, enableDispatch, enableCausility)
}

var _ PluginFactory
var _ = NewPluginFactory()
