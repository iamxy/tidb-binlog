package sync

import (
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

//SyncerDemo is a syncer demo
type SyncerDemo struct {
	*baseSyncer
}

//Sync is the method that interface must implement
func (sd *SyncerDemo) Sync(item *Item) error {
	//demo
	log.Info("item", zap.String("%s", fmt.Sprintf("%v", item)))
	sd.success <- item
	return nil
}

//Close is the method that interface must implement
func (sd *SyncerDemo) Close() error {
	return nil
}

//SetSafeMode is the method that interface must implement
func (sd *SyncerDemo) SetSafeMode(mode bool) bool {
	return false
}

//NewSyncerDemo is a syncer demo
func NewSyncerDemo(
	cfg *DBConfig,
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
) (Syncer, error) {
	log.Info("call NewSyncerDemo()")
	executor := &SyncerDemo{}
	executor.baseSyncer = newBaseSyncer(tableInfoGetter)
	return executor, nil
}
