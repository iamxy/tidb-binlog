package syncplg

import (
	"errors"
	"fmt"
	"plugin"

	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	//NewPlugin is the name of exported function by syncer plugin
	NewPlugin = "NewPluginFactory"
)

//FactoryInterface is interface of Factory
type FactoryInterface interface {
	NewSyncerPlugin(
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
	) (sync.Syncer, error)
}

//NewSyncerFunc is a function type which syncer plugin must implement
type NewSyncerFunc func(
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
) (sync.Syncer, error)

//LoadPlugin load syncer plugin
func LoadPlugin(path, name string) (NewSyncerFunc, error) {
	fp := path + "/" + name
	p, err := plugin.Open(fp)
	if err != nil {
		return nil, fmt.Errorf("faile to Open %s . err: %s", fp, err.Error())
	}

	sym, err := p.Lookup(NewPlugin)
	if err != nil {
		return nil, err
	}
	newFactory, ok := sym.(func() interface{})
	if !ok {
		return nil, errors.New("function type is incorrect")
	}
	fac := newFactory()
	plg, ok := fac.(FactoryInterface)
	if !ok {
		return nil, errors.New("not implement FactoryInterface")
	}
	return plg.NewSyncerPlugin, nil
}
