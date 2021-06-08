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

package storage

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	pkgutil "github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	maxTxnTimeoutSecond int64 = 600
	gcMaxBlockTime            = 30 * time.Minute // we run GC at every 1 hour, but may block GC when reading and sending binlog at most in this duration.
	chanCapacity              = 1 << 20
	// if pump takes a long time to write binlog, pump will display the binlog meta information (unit: Second)
	slowWriteThreshold               = 1.0
	defaultStopWriteAtAvailableSpace = 10 * (1 << 30)
)

var (
	// ErrRequestGCedBinlog indicates a Drainer is requesting some purged binlogs.
	ErrRequestGCedBinlog = errors.New("request a purged binlog")

	// save gcTS, the max TS we have gc, for binlog not greater than gcTS, we can delete it from storage
	gcTSKey = []byte("!binlog!gcts")
	// save maxCommitTS, we can get binlog in range [gcTS, maxCommitTS]  from PullCommitBinlog
	maxCommitTSKey = []byte("!binlog!maxCommitTS")
	// save the valuePointer we should start push binlog item to sorter when restart
	handlePointerKey = []byte("!binlog!handlePointer")
	// save valuePointer headPointer, for binlog in vlog not after headPointer, we have save it in metadata db
	// at start up, we can scan the vlog from headPointer and save the ts -> valuePointer to metadata db
	headPointerKey = []byte("!binlog!headPointer")
	// If the kv channel blocks for more than this value, turn on the slow chaser
	slowChaserThreshold = 3 * time.Second
	// Values of handlePointer and MaxCommitTS will be saved at most once per interval
	handlePtrSaveInterval = time.Second
)

// Storage is the interface to handle binlog storage
type Storage interface {
	WriteBinlog(binlog *pb.Binlog) error

	// delete <= ts
	GC(ts int64)

	GetGCTS() int64

	// AllMatched return if all the P-binlog have the matching C-binlog
	AllMatched() bool

	MaxCommitTS() int64

	// GetBinlog return the binlog of ts
	GetBinlog(ts int64) (binlog *pb.Binlog, err error)

	// PullCommitBinlog return the chan to consume the binlog
	PullCommitBinlog(ctx context.Context, last int64) (<-chan []byte, <-chan error)

	Close() error
}

var _ Storage = &Append{}

// Append implement the Storage interface
type Append struct {
	dir         string
	vlog        *valueLog
	storageSize storageSize

	metadata       *leveldb.DB
	sorter         *sorter
	tiStore        kv.Storage
	helper         *Helper
	tiLockResolver *tikv.LockResolver
	latestTS       int64

	gcWorking     int32
	gcTS          GCTS
	maxCommitTS   int64
	headPointer   valuePointer
	handlePointer valuePointer

	sortItems          chan sortItem
	handleSortItemQuit chan struct{}

	writeCh chan *request

	options *Options

	close chan struct{}
	wg    sync.WaitGroup
}

// NewAppend returns a instance of Append
func NewAppend(dir string, options *Options) (append *Append, err error) {
	return NewAppendWithResolver(dir, options, nil, nil)
}

// NewAppendWithResolver returns a instance of Append
// if tiStore and tiLockResolver is not nil, we will try to query tikv to know whether a txn is committed
func NewAppendWithResolver(dir string, options *Options, tiStore kv.Storage, tiLockResolver *tikv.LockResolver) (append *Append, err error) {
	if options == nil {
		options = DefaultOptions()
	}

	log.Info("NewAppendWithResolver", zap.Reflect("options", options))

	valueDir := path.Join(dir, "value")
	err = os.MkdirAll(valueDir, 0755)
	if err != nil {
		return nil, errors.Trace(err)
	}

	vlog, err := newValueLog(valueDir, options)
	if err != nil {
		return nil, errors.Trace(err)
	}

	kvDir := path.Join(dir, "kv")
	metadata, err := openMetadataDB(kvDir, options.KVConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	writeCh := make(chan *request, chanCapacity)

	append = &Append{
		dir:            dir,
		vlog:           vlog,
		metadata:       metadata,
		writeCh:        writeCh,
		options:        options,
		tiStore:        tiStore,
		tiLockResolver: tiLockResolver,

		close:     make(chan struct{}),
		sortItems: make(chan sortItem, 1024),
	}

	gcTS, err := append.readGCTSFromDB()
	if err != nil {
		return nil, errors.Trace(err)
	}
	append.gcTS.Store(gcTS)
	gcTSGauge.Set(float64(oracle.ExtractPhysical(uint64(gcTS))))

	append.maxCommitTS, err = append.readInt64(maxCommitTSKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	maxCommitTSGauge.Set(float64(oracle.ExtractPhysical(uint64(append.maxCommitTS))))

	append.headPointer, err = append.readPointer(headPointerKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	append.handlePointer, err = append.readPointer(handlePointerKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	append.handleSortItemQuit = append.handleSortItem(append.sortItems)
	sorter := newSorter(func(item sortItem) {
		log.Debug("sorter get item", zap.Stringer("item", &item))
		append.sortItems <- item
	})

	if tiStore != nil {
		tikvStorage, ok := tiStore.(tikv.Storage)
		if !ok {
			return nil, errors.New("not tikv.Storage")
		}

		append.helper = &Helper{
			Store:       tikvStorage,
			RegionCache: tikvStorage.GetRegionCache(),
		}

		sorter.setResolver(append.resolve)
	}

	append.sorter = sorter

	minPointer := append.headPointer
	if append.handlePointer.less(minPointer) {
		minPointer = append.handlePointer
	}

	log.Info("Append info", zap.Int64("gcTS", gcTS),
		zap.Int64("maxCommitTS", append.maxCommitTS),
		zap.Reflect("headPointer", append.headPointer),
		zap.Reflect("handlePointer", append.handlePointer))

	toKV := append.writeToValueLog(writeCh)

	append.wg.Add(1)
	go append.writeToSorter(append.writeToKV(toKV))

	// for handlePointer and headPointer, it's safe to start at a forward point, so we just chose a min point between handlePointer and headPointer, and wirte to KV, will push to sorter after write to KV too
	err = append.vlog.scanRequests(minPointer, func(req *request) error {
		toKV <- req
		return nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}

	append.wg.Add(1)
	err = append.updateSize()
	if err != nil {
		return nil, errors.Trace(err)
	}

	go append.updateStatus()
	return
}

func (a *Append) persistHandlePointer(item sortItem) error {
	log.Debug("persist item", zap.Stringer("item", &item))
	tsKey := encodeTSKey(item.commit)
	pointerData, err := a.metadata.Get(tsKey, nil)
	if err != nil {
		return errors.Trace(err)
	}
	var pointer valuePointer
	err = pointer.UnmarshalBinary(pointerData)
	if err != nil {
		return errors.Trace(err)
	}

	var batch leveldb.Batch
	maxCommitTS := make([]byte, 8)
	binary.LittleEndian.PutUint64(maxCommitTS, uint64(item.commit))
	batch.Put(maxCommitTSKey, maxCommitTS)

	batch.Put(handlePointerKey, pointerData)
	err = a.metadata.Write(&batch, nil)
	if err != nil {
		return errors.Trace(err)
	}
	a.handlePointer = pointer

	return nil
}

func (a *Append) handleSortItem(items <-chan sortItem) (quit chan struct{}) {
	quit = make(chan struct{})

	go func() {
		defer close(quit)

		var toSaveItem sortItem
		var toSave <-chan time.Time
		for {
			select {
			case item, ok := <-items:
				if !ok {
					if toSave != nil {
						err := a.persistHandlePointer(toSaveItem)
						if err != nil {
							log.Error(errors.ErrorStack(err))
						}
					}
					return
				}
				// the commitTS we get from sorter is monotonic increasing, unless we forward the handlePointer at start up
				// or this should never happen
				if item.commit < atomic.LoadInt64(&a.maxCommitTS) {
					log.Warn("sortItem's commit ts less than append.maxCommitTS",
						zap.Int64("ts", item.commit),
						zap.Int64("maxCommitTS", a.maxCommitTS))
					continue
				}
				atomic.StoreInt64(&a.maxCommitTS, item.commit)
				maxCommitTSGauge.Set(float64(oracle.ExtractPhysical(uint64(item.commit))))
				toSaveItem = item
				if toSave == nil {
					toSave = time.After(handlePtrSaveInterval)
				}
				log.Debug("get sort item", zap.Stringer("item", &item))
			case <-toSave:
				err := a.persistHandlePointer(toSaveItem)
				if err != nil {
					log.Error(errors.ErrorStack(err))
				}
				toSave = nil
			}

		}
	}()

	return quit
}

func (a *Append) updateSize() error {
	size, err := getStorageSize(a.dir)
	if err != nil {
		return errors.Annotatef(err, "update storage size failed, dir: %s", a.dir)
	}

	storageSizeGauge.WithLabelValues("capacity").Set(float64(size.capacity))
	storageSizeGauge.WithLabelValues("available").Set(float64(size.available))

	atomic.StoreUint64(&a.storageSize.available, size.available)
	atomic.StoreUint64(&a.storageSize.capacity, size.capacity)

	if !a.writableOfSpace() {
		log.Warn("no available space, you may want to free up some space or decrease `stop-write-at-available-space` configuration",
			zap.Uint64("available", size.available),
			zap.Uint64("StopWriteAtAvailableSpace", a.options.StopWriteAtAvailableSpace))
	}

	return nil
}

func (a *Append) updateStatus() {
	defer a.wg.Done()

	var updateLatest <-chan time.Time
	if a.tiStore != nil {
		updateLatestTicker := time.NewTicker(time.Second)
		defer updateLatestTicker.Stop()
		updateLatest = updateLatestTicker.C
	}

	updateSizeTicker := time.NewTicker(time.Second * 3)
	defer updateSizeTicker.Stop()
	updateSize := updateSizeTicker.C

	logStatsTicker := time.NewTicker(time.Second * 10)
	defer logStatsTicker.Stop()

	for {
		select {
		case <-a.close:
			return
		case <-updateLatest:
			ts, err := pkgutil.QueryLatestTsFromPD(a.tiStore)
			if err != nil {
				log.Error("QueryLatestTSFromPD failed", zap.Error(err))
			} else {
				atomic.StoreInt64(&a.latestTS, ts)
			}
		case <-updateSize:
			err := a.updateSize()
			if err != nil {
				log.Error("update size failed", zap.Error(err))
			}
		case <-logStatsTicker.C:
			var stats leveldb.DBStats
			err := a.metadata.Stats(&stats)
			if err != nil {
				log.Error("get Stats failed", zap.Error(err))
			} else {
				log.Info("DBStats", zap.Reflect("DBStats", stats))
				if stats.WritePaused {
					log.Warn("in WritePaused stat")
				}
			}
		}
	}
}

func (a *Append) writableOfSpace() bool {
	return atomic.LoadUint64(&a.storageSize.available) > a.options.StopWriteAtAvailableSpace
}

// Write a commit binlog myself if the status is committed,
// otherwise we can just ignore it, we will not get the commit binlog while iterating the kv by ts
func (a *Append) writeCBinlog(pbinlog *pb.Binlog, commitTS int64) error {
	// write the commit binlog myself
	cbinlog := new(pb.Binlog)
	cbinlog.Tp = pb.BinlogType_Commit
	cbinlog.StartTs = pbinlog.StartTs
	cbinlog.CommitTs = commitTS

	req := a.writeBinlog(cbinlog)
	if req.err != nil {
		return errors.Annotate(req.err, "writeBinlog failed")
	}

	// when writeBinlog return success, the pointer will be write to kv async,
	// but we need to make sure it has been write to kv when we return true in the func, then we can get this commit binlog when
	// we update maxCommitTS
	// write the ts -> pointer to KV here to make sure it.
	pointer, err := req.valuePointer.MarshalBinary()
	if err != nil {
		panic(err)
	}

	err = a.metadata.Put(encodeTSKey(req.ts()), pointer, nil)
	if err != nil {
		return errors.Annotate(req.err, "put into metadata failed")
	}

	return nil
}

func (a *Append) resolve(startTS int64) bool {
	latestTS := atomic.LoadInt64(&a.latestTS)
	if latestTS <= 0 {
		return false
	}

	pbinlog, err := a.readBinlogByTS(startTS)
	if err != nil {
		log.Error(errors.ErrorStack(err))
		return false
	}

	resp, err := a.helper.GetMvccByEncodedKey(pbinlog.PrewriteKey)
	if err != nil {
		log.Error("GetMvccByEncodedKey failed", zap.Int64("start ts", startTS), zap.Error(err))
	} else if resp.RegionError != nil {
		log.Error("GetMvccByEncodedKey failed", zap.Int64("start ts", startTS), zap.Stringer("RegionError", resp.RegionError))
	} else if len(resp.Error) > 0 {
		log.Error("GetMvccByEncodedKey failed", zap.Int64("start ts", startTS), zap.String("Error", resp.Error))
	} else {
		for _, w := range resp.Info.Writes {
			if int64(w.StartTs) != startTS {
				continue
			}

			if w.Type != kvrpcpb.Op_Rollback {
				// Sanity checks
				if int64(w.CommitTs) <= startTS {
					log.Error("op type not Rollback, but have unexpect commit ts",
						zap.Int64("startTS", startTS),
						zap.Uint64("commitTS", w.CommitTs))
					break
				}

				err := a.writeCBinlog(pbinlog, int64(w.CommitTs))
				if err != nil {
					log.Error("writeCBinlog failed", zap.Int64("start ts", startTS), zap.Error(err))
					return false
				}
			} else {
				// Will get the same value as start ts if it's rollback, set to 0 for log
				w.CommitTs = 0
			}

			log.Info("known txn is committed or rollback from tikv",
				zap.Int64("start ts", startTS),
				zap.Uint64("commit ts", w.CommitTs))
			return true
		}
	}

	startSecond := oracle.ExtractPhysical(uint64(startTS)) / int64(time.Second/time.Millisecond)
	maxSecond := oracle.ExtractPhysical(uint64(latestTS)) / int64(time.Second/time.Millisecond)
	elapseSecond := maxSecond - startSecond

	// `GetTxnStatus` will not abort valid txn now, but we still keep this logic and only `GetTxnStatus` after `maxTxnTimeoutSecond`.
	// for expired locks, Pump and/or TiDB try to cleanup them should have no side effects.
	if elapseSecond <= maxTxnTimeoutSecond {
		log.Info(
			"Find no MVCC record for a young txn",
			zap.Int64("start ts", startTS),
			zap.Int64("elapse sec", elapseSecond),
		)
		return false
	}

	tikvQueryCount.Add(1.0)
	primaryKey := pbinlog.GetPrewriteKey()
	status, err := a.tiLockResolver.GetTxnStatus(uint64(pbinlog.StartTs), uint64(pbinlog.StartTs), primaryKey)
	if err != nil {
		log.Error("get commit status failed for unknown txn", zap.Int64("start ts", startTS), zap.Error(err))
		return false
	}

	log.Info("got commit status for unknown txn",
		zap.Int64("start ts", startTS),
		zap.Uint64("commit ts", status.CommitTS()),
		zap.Uint64("ttl", status.TTL()),
		zap.Reflect("action", status.Action()),
		zap.Bool("isDDL", pbinlog.GetDdlJobId() > 0))

	// check TTL (whether the lock is valid)
	if status.TTL() > 0 {
		log.Warn("the txn lock is still valid, will retry later", zap.Int64("start ts", startTS), zap.Uint64("ttl", status.TTL()))
		return false
	}

	// Write a commit binlog myself if the status is committed,
	// otherwise we can just ignore it, we will not get the commit binlog while iterator the kv by ts
	if status.IsCommitted() {
		// write the commit binlog myself
		cbinlog := new(pb.Binlog)
		cbinlog.Tp = pb.BinlogType_Commit
		cbinlog.StartTs = pbinlog.StartTs
		cbinlog.CommitTs = int64(status.CommitTS())

		req := a.writeBinlog(cbinlog)
		if req.err != nil {
			log.Error("write missing committed binlog failed",
				zap.Int64("start ts", startTS),
				zap.Uint64("commit ts", status.CommitTS()),
				zap.Bool("isDDL", pbinlog.GetDdlJobId() > 0),
				zap.Error(req.err))
			return false
		}

		// when writeBinlog return success, the pointer will be write to kv async,
		// but we need to make sure it has been write to kv when we return true in the func, then we can get this commit binlog when
		// we update maxCommitTS
		// write the ts -> pointer to KV here to make sure it.
		pointer, err := req.valuePointer.MarshalBinary()
		if err != nil {
			panic(err)
		}

		err = a.metadata.Put(encodeTSKey(req.ts()), pointer, nil)
		if err != nil {
			log.Error("put missing committed binlog into metadata failed",
				zap.Int64("start ts", startTS),
				zap.Uint64("commit ts", status.CommitTS()),
				zap.Bool("isDDL", pbinlog.GetDdlJobId() > 0),
				zap.Error(err))
			return false
		}
	}

	return true
}

// GetBinlog gets binlog by ts
func (a *Append) GetBinlog(ts int64) (*pb.Binlog, error) {
	return a.readBinlogByTS(ts)
}

func (a *Append) readBinlogByTS(ts int64) (*pb.Binlog, error) {
	var vp valuePointer

	vpData, err := a.metadata.Get(encodeTSKey(ts), nil)
	if err != nil {
		return nil, errors.Annotatef(err, "fail read binlog by ts: %d", ts)
	}

	err = vp.UnmarshalBinary(vpData)
	if err != nil {
		return nil, errors.Annotatef(err, "fail read binlog by ts: %d", ts)
	}

	pvalue, err := a.vlog.readValue(vp)
	if err != nil {
		return nil, errors.Annotatef(err, "fail read binlog by ts: %d", ts)
	}

	binlog := new(pb.Binlog)
	err = binlog.Unmarshal(pvalue)
	if err != nil {
		return nil, errors.Annotatef(err, "fail read binlog by ts: %d", ts)
	}

	return binlog, nil
}

// if the key not exist, return 0, nil
func (a *Append) readInt64(key []byte) (int64, error) {
	value, err := a.metadata.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return 0, nil
		}
		return 0, errors.Trace(err)
	}

	return int64(binary.LittleEndian.Uint64(value)), nil
}

func (a *Append) readGCTSFromDB() (int64, error) {
	return a.readInt64(gcTSKey)
}

func (a *Append) saveGCTSToDB(ts int64) error {
	var value = make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(ts))

	return a.metadata.Put(gcTSKey, value, nil)
}

func (a *Append) isClosed() bool {
	select {
	case <-a.close:
		return true
	default:
		return false
	}
}

// Close release resource of Append
func (a *Append) Close() error {
	log.Debug("close Append")

	close(a.close)
	close(a.writeCh)

	// wait for all binlog write to vlog -> KV -> sorter
	// after this, writeToValueLog, writeToKV, writeToSorter has quit sequently
	a.wg.Wait()
	log.Debug("wait group done")

	// note the call back func will use a.metadata, so we should close sorter before a.metadata
	a.sorter.close()
	log.Debug("sorter is closed")

	close(a.sortItems)
	<-a.handleSortItemQuit
	log.Debug("handle sort item quit")

	err := a.metadata.Close()
	if err != nil {
		log.Error("close metadata failed", zap.Error(err))
	}

	err = a.vlog.close()
	if err != nil {
		log.Error("close vlog failed", zap.Error(err))
	}

	return err
}

// GetGCTS implement Storage.GetGCTS
func (a *Append) GetGCTS() int64 {
	return a.gcTS.Load()
}

// GC implement Storage.GC
func (a *Append) GC(ts int64) {
	lastTS := a.gcTS.Load()
	if ts <= lastTS {
		log.Info("ignore gc request", zap.Int64("ts", ts), zap.Int64("lastTS", lastTS))
		return
	}

	if atomic.LoadInt64(&a.maxCommitTS) <= ts {
		log.Info("Ignore unsafe gc request, may affect unsorted binlogs",
			zap.Int64("ts", ts),
			zap.Int64("lastTS", lastTS),
		)
		return
	}

	a.gcTS.Store(ts) // once `Store` returned, no guarantee for metadata or vlog.
	if err := a.saveGCTSToDB(ts); err != nil {
		log.Error("Failed to save GCTS", zap.Int64("ts", ts), zap.Error(err))
	}
	gcTSGauge.Set(float64(oracle.ExtractPhysical(uint64(ts))))

	if !atomic.CompareAndSwapInt32(&a.gcWorking, 0, 1) {
		return
	}

	go func() {
		defer atomic.StoreInt32(&a.gcWorking, 0)
		// for commit binlog TS ts_c, we may need to get the according P binlog ts_p(ts_p < ts_c
		// so we forward a little bit to make sure we can get the according P binlog
		a.doGCTS(ts - int64(oracle.EncodeTSO(maxTxnTimeoutSecond*1000)))
	}()
}

func (a *Append) doGCTS(ts int64) {
	log.Info("Starting GC", zap.Int64("ts", ts))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		a.vlog.gcTS(ts)
		log.Info("Finish VLog GC", zap.Int64("ts", ts))
		wg.Done()
	}()

	batch := new(leveldb.Batch)
	l0Trigger := defaultStorageKVConfig.CompactionL0Trigger
	if a.options.KVConfig != nil && a.options.KVConfig.CompactionL0Trigger > 0 {
		l0Trigger = a.options.KVConfig.CompactionL0Trigger
	}

	deleteNum := 0

	irange := &util.Range{
		Start: encodeTSKey(0),
		Limit: encodeTSKey(ts + 1),
	}
	var iter iterator.Iterator
	defer func() {
		if iter != nil {
			iter.Release()
			iter = nil
		}
	}()

	for {
		nStr, err := a.metadata.GetProperty("leveldb.num-files-at-level0")
		if err != nil {
			log.Error("get `leveldb.num-files-at-level0` property failed", zap.Error(err))
			break
		}

		l0Num, err := strconv.Atoi(nStr)
		if err != nil {
			log.Error("parse `leveldb.num-files-at-level0` result to int failed", zap.String("str", nStr), zap.Error(err))
			break
		}

		if l0Num >= l0Trigger {
			log.Info("wait some time to gc cause too many L0 file", zap.Int("files", l0Num))
			if iter != nil {
				iter.Release()
				iter = nil
			}
			time.Sleep(5 * time.Second)
			continue
		}

		deleteBatch := 0
		var lastKey []byte

		if iter == nil {
			log.Info("New LevelDB iterator created for GC", zap.Int64("ts", ts),
				zap.Int64("start", decodeTSKey(irange.Start)),
				zap.Int64("limit", decodeTSKey(irange.Limit)))
			iter = a.metadata.NewIterator(irange, nil)
		}

		for deleteBatch < 100 && iter.Next() {
			batch.Delete(iter.Key())
			deleteNum++
			lastKey = iter.Key()

			if batch.Len() == 1024 {
				err := a.metadata.Write(batch, nil)
				if err != nil {
					log.Error("write batch failed", zap.Error(err))
				}
				deletedKv.Add(float64(batch.Len()))
				batch.Reset()
				deleteBatch++
			}
		}

		if deleteBatch < 100 {
			if batch.Len() > 0 {
				err := a.metadata.Write(batch, nil)
				if err != nil {
					log.Error("write batch failed", zap.Error(err))
				}
				deletedKv.Add(float64(batch.Len()))
				batch.Reset()
			}
			log.Info("Finish KV GC", zap.Int64("ts", ts), zap.Int("delete num", deleteNum))
			break
		}

		if len(lastKey) > 0 {
			irange.Start = lastKey
			doneGcTSGauge.Set(float64(oracle.ExtractPhysical(uint64(decodeTSKey(lastKey)))))
		}
		log.Info("has delete", zap.Int("delete num", deleteNum))
	}
	wg.Wait()
	doneGcTSGauge.Set(float64(oracle.ExtractPhysical(uint64(ts))))
}

// MaxCommitTS implement Storage.MaxCommitTS
func (a *Append) MaxCommitTS() int64 {
	return atomic.LoadInt64(&a.maxCommitTS)
}

func isFakeBinlog(binlog *pb.Binlog) bool {
	return binlog.StartTs > 0 && binlog.StartTs == binlog.CommitTs
}

// WriteBinlog implement Storage.WriteBinlog
func (a *Append) WriteBinlog(binlog *pb.Binlog) error {
	if !a.writableOfSpace() {
		// still accept fake binlog, so will not block drainer if fake binlog writes success
		if !isFakeBinlog(binlog) {
			return errors.Errorf("no available space, available: %d, StopWriteAtAvailableSpace: %d", atomic.LoadUint64(&a.storageSize.available), a.options.StopWriteAtAvailableSpace)
		}
	}

	// pump client will write some empty Payload to detect whether pump is working, should avoid this
	// Unmarshal(nil) will success...
	if binlog.StartTs == 0 && binlog.CommitTs == 0 {
		return nil
	}

	return errors.Trace(a.writeBinlog(binlog).err)
}

func (a *Append) writeBinlog(binlog *pb.Binlog) *request {
	beginTime := time.Now()
	request := new(request)

	defer func() {
		duration := time.Since(beginTime).Seconds()
		writeBinlogTimeHistogram.WithLabelValues("single").Observe(duration)
		if request.err != nil {
			errorCount.WithLabelValues("write_binlog").Add(1.0)
		}

		if duration > a.options.SlowWriteThreshold {
			log.Warn("take a long time to write binlog", zap.Stringer("binlog type", binlog.Tp), zap.Int64("commit TS", binlog.CommitTs), zap.Int64("start TS", binlog.StartTs), zap.Int("length", len(binlog.PrewriteValue)), zap.Float64("cost time", duration))
		}
	}()

	payload, err := binlog.Marshal()
	if err != nil {
		request.err = errors.Trace(err)
		return request
	}

	writeBinlogSizeHistogram.WithLabelValues("single").Observe(float64(len(payload)))

	request.payload = payload
	request.startTS = binlog.StartTs
	request.commitTS = binlog.CommitTs
	request.tp = binlog.Tp
	request.wg.Add(1)

	a.writeCh <- request

	request.wg.Wait()

	return request
}

func (a *Append) writeToSorter(reqs chan *request) {
	defer a.wg.Done()

	for req := range reqs {
		log.Debug("write request to sorter", zap.Stringer("request", req))
		var item sortItem
		item.start = req.startTS
		item.commit = req.commitTS
		item.tp = req.tp

		a.sorter.pushTSItem(item)
	}
}

func (a *Append) writeToKV(reqs chan *request) chan *request {
	done := make(chan *request, chanCapacity)

	batchReqs := a.batchRequest(reqs, 128)

	go func() {
		defer close(done)

		for bufReqs := range batchReqs {
			if err := a.writeBatchToKV(bufReqs); err != nil {
				return
			}
			for _, req := range bufReqs {
				done <- req
			}
		}
	}()

	return done
}

func (a *Append) batchRequest(reqs chan *request, maxBatchNum int) chan []*request {
	done := make(chan []*request, 1024)
	var bufReqs []*request

	go func() {
		defer close(done)

		for {
			if len(bufReqs) >= maxBatchNum {
				done <- bufReqs
				bufReqs = make([]*request, 0, maxBatchNum)
			}

			select {
			case req, ok := <-reqs:
				if !ok {
					if len(bufReqs) > 0 {
						done <- bufReqs
					}
					return
				}
				bufReqs = append(bufReqs, req)
			default:
				if len(bufReqs) > 0 {
					done <- bufReqs
					bufReqs = make([]*request, 0, maxBatchNum)
				} else { // get first req
					req, ok := <-reqs
					if !ok {
						return
					}
					bufReqs = append(bufReqs, req)
				}
			}
		}
	}()

	return done
}

func (a *Append) writeToValueLog(reqs chan *request) chan *request {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *request, a.options.KVChanCapacity)
	slowChaser := newSlowChaser(a.vlog, time.Second, done)
	slowChaserStopped := make(chan struct{})
	go func() {
		slowChaser.Run(ctx)
		close(slowChaserStopped)
	}()

	go func() {
		defer func() {
			cancel()
			<-slowChaserStopped
			close(done)
		}()

		var bufReqs []*request
		var size int

		write := func(batch []*request) {
			slowChaser.WriteLock.Lock()
			defer slowChaser.WriteLock.Unlock()

			if len(batch) == 0 {
				return
			}
			br := batchRequest(batch)
			log.Debug("write requests to value log", zap.Stringer("requests", &br))
			beginTime := time.Now()
			writeBinlogSizeHistogram.WithLabelValues("batch").Observe(float64(size))

			err := a.vlog.write(batch)
			writeBinlogTimeHistogram.WithLabelValues("batch").Observe(time.Since(beginTime).Seconds())
			if err != nil {
				for _, req := range batch {
					req.err = err
					req.wg.Done()
				}
				errorCount.WithLabelValues("write_binlog_batch").Add(1.0)
				return
			}

			for _, req := range batch {
				log.Debug("request done", zap.Int64("startTS", req.startTS), zap.Int64("commitTS", req.commitTS))
				req.wg.Done()
				// payload is useless anymore, let it GC ASAP
				req.payload = nil
			}

			if slowChaser.IsOn() {
				return
			}
		SEND:
			for _, req := range batch {
				select {
				case done <- req:
				case <-time.After(slowChaserThreshold):
					slowChaser.TurnOn(&req.valuePointer)
					break SEND
				}
			}
		}

		for {
			select {
			case req, ok := <-reqs:
				if !ok {
					write(bufReqs)
					return
				}
				bufReqs = append(bufReqs, req)
				size += len(req.payload)

				// Allow the group to grow up to a maximum size, but if the
				// original write is small, limit the growth so we do not slow
				// down the small write too much.
				maxSize := 1 << 20
				firstSize := len(bufReqs[0].payload)
				if firstSize <= (128 << 10) {
					maxSize = firstSize + (128 << 10)
				}

				if size >= maxSize {
					write(bufReqs)
					size = 0
					bufReqs = bufReqs[:0]
				}
			default:
				if len(bufReqs) > 0 {
					write(bufReqs)
					size = 0
					bufReqs = bufReqs[:0]
					continue
				}

				// get first
				req, ok := <-reqs
				if !ok {
					return
				}
				bufReqs = append(bufReqs, req)
				size += len(req.payload)
			}
		}
	}()

	return done
}

func (a *Append) readPointer(key []byte) (valuePointer, error) {
	var vp valuePointer
	value, err := a.metadata.Get(key, nil)
	if err != nil {
		// return zero value when not found
		if err == leveldb.ErrNotFound {
			return vp, nil
		}

		return vp, errors.Trace(err)
	}
	err = vp.UnmarshalBinary(value)
	if err != nil {
		return vp, errors.Trace(err)
	}

	return vp, nil
}

func (a *Append) savePointer(key []byte, vp valuePointer) error {
	value, err := vp.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	err = a.metadata.Put(key, value, nil)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (a *Append) feedPreWriteValue(cbinlog *pb.Binlog) error {
	var vp valuePointer

	vpData, err := a.metadata.Get(encodeTSKey(cbinlog.StartTs), nil)
	if err != nil {
		return errors.Annotatef(err, "get pointer of P-Binlog(ts: %d) failed", cbinlog.StartTs)
	}

	err = vp.UnmarshalBinary(vpData)
	if err != nil {
		return errors.Trace(err)
	}

	pvalue, err := a.vlog.readValue(vp)
	if err != nil {
		return errors.Annotatef(err, "read P-Binlog value failed, vp: %+v", vp)
	}

	pbinlog := new(pb.Binlog)
	err = pbinlog.Unmarshal(pvalue)
	if err != nil {
		return errors.Trace(err)
	}

	cbinlog.StartTs = pbinlog.StartTs
	cbinlog.PrewriteValue = pbinlog.PrewriteValue
	cbinlog.DdlQuery = pbinlog.DdlQuery
	cbinlog.DdlJobId = pbinlog.DdlJobId
	cbinlog.DdlSchemaState = pbinlog.DdlSchemaState

	return nil
}

// PullCommitBinlog return commit binlog  > last
func (a *Append) PullCommitBinlog(ctx context.Context, last int64) (<-chan []byte, <-chan error) {
	log.Debug("new PullCommitBinlog", zap.Int64("last ts", last))

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-a.close:
			cancel()
		case <-ctx.Done():
		}
	}()

	values := make(chan []byte, 5)
	errs := make(chan error, 5) // we `return` after sending an error now, so it should never block on this chan.

	gcTS := a.gcTS.Load()
	if last < gcTS {
		if last == 0 {
			log.Warn("last TS is 0, will send binlog from gcTS", zap.Int64("gcTS", gcTS))
			last = gcTS
		} else {
			log.Error("last TS less than gcTS, some binlog events may be loss", zap.Int64("lastTS", last), zap.Int64("gcTS", gcTS))
			errs <- errors.Annotatef(ErrRequestGCedBinlog, "requested TS %d, GC TS %d", last, gcTS)
			return values, errs
		}
	}

	irange := &util.Range{
		Start: encodeTSKey(0),
		Limit: encodeTSKey(math.MaxInt64),
	}

	pLog := pkgutil.NewLog()
	labelWrongRange := "wrong range"
	pLog.Add(labelWrongRange, 10*time.Second)

	go func() {
		defer close(values)

		for {
			startTS := last + 1
			limitTS := atomic.LoadInt64(&a.maxCommitTS) + 1
			if startTS > limitTS {
				// if range's start is greater than limit, may cause panic, see https://github.com/syndtr/goleveldb/issues/224 for detail.
				pLog.Print(labelWrongRange, func() {
					log.Warn("last ts is greater than pump's max commit ts", zap.Int64("last ts", startTS-1), zap.Int64("max commit ts", limitTS-1))
				})
				time.Sleep(time.Second)
				continue
			}

			// acquire the lock to block GC.
			// NOTE: do not forget to release the lock carefully.
			gcTS = a.gcTS.LoadAndLock()
			if last < gcTS {
				a.gcTS.ReleaseLoadLock()
				log.Error("last TS less than gcTS, some binlog events may be loss", zap.Int64("lastTS", last), zap.Int64("gcTS", gcTS))
				errs <- errors.Annotatef(ErrRequestGCedBinlog, "requested TS %d, GC TS %d", last, gcTS)
				return
			}

			irange.Start = encodeTSKey(startTS)
			irange.Limit = encodeTSKey(limitTS)
			iter := a.metadata.NewIterator(irange, nil)

			// log.Debugf("try to get range [%d,%d)", startTS, atomic.LoadInt64(&a.maxCommitTS)+1)

		readForLoop:
			for ok := iter.Seek(encodeTSKey(startTS)); ok; ok = iter.Next() {
				var vp valuePointer
				err := vp.UnmarshalBinary(iter.Value())
				// should never happen
				if err != nil {
					panic(err)
				}

				log.Debug("get binlog", zap.Int64("ts", decodeTSKey(iter.Key())), zap.Reflect("pointer", vp))

				value, err := a.vlog.readValue(vp)
				if err != nil {
					iter.Release()
					errorCount.WithLabelValues("read_value").Add(1.0)
					a.gcTS.ReleaseLoadLock()
					log.Error("read value failed", zap.Int64("TS", decodeTSKey(iter.Key())), zap.Error(err))
					errs <- errors.Errorf("read value failed, TS %d", decodeTSKey(iter.Key()))
					return
				}

				binlog := new(pb.Binlog)
				err = binlog.Unmarshal(value)
				if err != nil {
					iter.Release()
					a.gcTS.ReleaseLoadLock()
					log.Error("Unmarshal Binlog failed", zap.Int64("TS", decodeTSKey(iter.Key())), zap.Error(err))
					errs <- errors.Errorf("Unmarshal Binlog failed, TS %d", decodeTSKey(iter.Key()))
					return
				}

				if binlog.Tp == pb.BinlogType_Prewrite {
					continue
				}

				if binlog.CommitTs == binlog.StartTs {
					// this should be a fake binlog, drainer should ignore this when push binlog to the downstream
					log.Debug("get fake c binlog", zap.Int64("CommitTS", binlog.CommitTs))
				} else {
					err = a.feedPreWriteValue(binlog)
					if err != nil {
						if errors.Cause(err) == leveldb.ErrNotFound {
							// In pump-client, a C-binlog should always be sent to the same pump instance as the matching P-binlog.
							// But in some older versions of pump-client, writing of C-binlog would fallback to some other instances when the correct one is unavailable.
							// When this error occurs, we may assume that the matching P-binlog is on a different pump instance.
							// And it would  query TiKV for the matching C-binlog. So it should be OK to ignore the error here.
							log.Error("Matching P-binlog not found", zap.Int64("commit ts", binlog.CommitTs))
							continue
						}

						errorCount.WithLabelValues("feed_pre_write_value").Add(1.0)
						iter.Release()
						a.gcTS.ReleaseLoadLock()
						log.Error("feed pre write value failed", zap.Int64("TS", decodeTSKey(iter.Key())), zap.Error(err))
						errs <- errors.Errorf("feed pre write value failed, TS %d", decodeTSKey(iter.Key()))
						return
					}
				}

				value, err = binlog.Marshal()
				if err != nil {
					iter.Release()
					a.gcTS.ReleaseLoadLock()
					log.Error("marshal failed", zap.Int64("TS", decodeTSKey(iter.Key())), zap.Error(err))
					errs <- errors.Errorf("marshal failed, TS %d", decodeTSKey(iter.Key()))
					return
				}

				select {
				case values <- value:
					log.Debug("send value success")
				case <-time.After(gcMaxBlockTime):
					// do not update `last` anymore.
					log.Warn("can not send the binlog for a long time, will try to read again", zap.Duration("duration", gcMaxBlockTime), zap.Int64("current TS", decodeTSKey(iter.Key())))
					break readForLoop
				case <-ctx.Done():
					iter.Release()
					a.gcTS.ReleaseLoadLock()
					return
				}

				last = decodeTSKey(iter.Key())
			}
			iter.Release()
			a.gcTS.ReleaseLoadLock()
			err := iter.Error()
			if err != nil {
				log.Error("encounter iterator error", zap.Error(err))
			}

			select {
			case <-ctx.Done():
				return
				// TODO signal to wait up, don't sleep
			case <-time.After(time.Millisecond * 100):
			}
		}
	}()

	return values, errs
}

type storageSize struct {
	capacity  uint64
	available uint64
}

func getStorageSize(dir string) (size storageSize, err error) {
	var stat unix.Statfs_t

	err = unix.Statfs(dir, &stat)
	if err != nil {
		return size, errors.Trace(err)
	}

	// When container is run in MacOS, `bsize` obtained by `statfs` syscall is not the fundamental block size,
	// but the `iosize` (optimal transfer block size) instead, it's usually 1024 times larger than the `bsize`.
	// for example `4096 * 1024`. To get the correct block size, we should use `frsize`. But `frsize` isn't
	// guaranteed to be supported everywhere, so we need to check whether it's supported before use it.
	// For more details, please refer to: https://github.com/docker/for-mac/issues/2136
	bSize := uint64(stat.Bsize)
	field := reflect.ValueOf(&stat).Elem().FieldByName("Frsize")
	if field.IsValid() {
		if field.Kind() == reflect.Uint64 {
			bSize = field.Uint()
		} else {
			bSize = uint64(field.Int())
		}
	}

	// Available blocks * size per block = available space in bytes
	size.available = stat.Bavail * bSize
	size.capacity = stat.Blocks * bSize

	return
}

// Config holds the configuration of storage
type Config struct {
	SyncLog *bool `toml:"sync-log" json:"sync-log"`
	// the channel to buffer binlog meta, pump will block write binlog request if the channel is full
	KVChanCapacity            int            `toml:"kv_chan_cap" json:"kv_chan_cap"`
	SlowWriteThreshold        float64        `toml:"slow_write_threshold" json:"slow_write_threshold"`
	KV                        *KVConfig      `toml:"kv" json:"kv"`
	StopWriteAtAvailableSpace *HumanizeBytes `toml:"stop-write-at-available-space" json:"stop-write-at-available-space"`
}

// GetKVChanCapacity return kv_chan_cap config option
func (c *Config) GetKVChanCapacity() int {
	if c.KVChanCapacity <= 0 {
		return chanCapacity
	}

	return c.KVChanCapacity
}

// GetSlowWriteThreshold return slow write threshold
func (c *Config) GetSlowWriteThreshold() float64 {
	if c.SlowWriteThreshold <= 0 {
		return slowWriteThreshold
	}

	return c.SlowWriteThreshold
}

// GetStopWriteAtAvailableSpace return stop write available space
func (c *Config) GetStopWriteAtAvailableSpace() uint64 {
	if c.StopWriteAtAvailableSpace == nil {
		return defaultStopWriteAtAvailableSpace
	}

	return c.StopWriteAtAvailableSpace.Uint64()
}

// GetSyncLog return sync-log config option
func (c *Config) GetSyncLog() bool {
	if c.SyncLog == nil {
		return true
	}

	return *c.SyncLog
}

// KVConfig if the configuration of goleveldb
type KVConfig struct {
	BlockCacheCapacity            int     `toml:"block-cache-capacity" json:"block-cache-capacity"`
	BlockRestartInterval          int     `toml:"block-restart-interval" json:"block-restart-interval"`
	BlockSize                     int     `toml:"block-size" json:"block-size"`
	CompactionL0Trigger           int     `toml:"compaction-L0-trigger" json:"compaction-L0-trigger"`
	CompactionTableSize           int     `toml:"compaction-table-size" json:"compaction-table-size"`
	CompactionTotalSize           int     `toml:"compaction-total-size" json:"compaction-total-size"`
	CompactionTotalSizeMultiplier float64 `toml:"compaction-total-size-multiplier" json:"compaction-total-size-multiplier"`
	WriteBuffer                   int     `toml:"write-buffer" json:"write-buffer"`
	WriteL0PauseTrigger           int     `toml:"write-L0-pause-trigger" json:"write-L0-pause-trigger"`
	WriteL0SlowdownTrigger        int     `toml:"write-L0-slowdown-trigger" json:"write-L0-slowdown-trigger"`
}

var defaultStorageKVConfig = &KVConfig{
	BlockCacheCapacity:            8 * opt.MiB,
	BlockRestartInterval:          16,
	BlockSize:                     4 * opt.KiB,
	CompactionL0Trigger:           8,
	CompactionTableSize:           64 * opt.MiB,
	CompactionTotalSize:           512 * opt.MiB,
	CompactionTotalSizeMultiplier: 8,
	WriteBuffer:                   64 * opt.MiB,
	WriteL0PauseTrigger:           24,
	WriteL0SlowdownTrigger:        17,
}

func setDefaultStorageConfig(cf *KVConfig) {
	if cf.BlockCacheCapacity <= 0 {
		cf.BlockCacheCapacity = defaultStorageKVConfig.BlockCacheCapacity
	}
	if cf.BlockRestartInterval <= 0 {
		cf.BlockRestartInterval = defaultStorageKVConfig.BlockRestartInterval
	}
	if cf.BlockSize <= 0 {
		cf.BlockSize = defaultStorageKVConfig.BlockSize
	}
	if cf.CompactionL0Trigger <= 0 {
		cf.CompactionL0Trigger = defaultStorageKVConfig.CompactionL0Trigger
	}
	if cf.CompactionTableSize <= 0 {
		cf.CompactionTableSize = defaultStorageKVConfig.CompactionTableSize
	}
	if cf.CompactionTotalSize <= 0 {
		cf.CompactionTotalSize = defaultStorageKVConfig.CompactionTotalSize
	}
	if cf.CompactionTotalSizeMultiplier <= 0 {
		cf.CompactionTotalSizeMultiplier = defaultStorageKVConfig.CompactionTotalSizeMultiplier
	}
	if cf.WriteBuffer <= 0 {
		cf.WriteBuffer = defaultStorageKVConfig.WriteBuffer
	}
	if cf.WriteL0PauseTrigger <= 0 {
		cf.WriteL0PauseTrigger = defaultStorageKVConfig.WriteL0PauseTrigger
	}
	if cf.WriteL0SlowdownTrigger <= 0 {
		cf.WriteL0SlowdownTrigger = defaultStorageKVConfig.WriteL0SlowdownTrigger
	}
}

func openMetadataDB(kvDir string, cf *KVConfig) (*leveldb.DB, error) {
	if cf == nil {
		cf = defaultStorageKVConfig
	} else {
		setDefaultStorageConfig(cf)
	}

	log.Info("open metadata db", zap.Reflect("config", cf))

	var opt opt.Options
	opt.BlockCacheCapacity = cf.BlockCacheCapacity
	opt.BlockRestartInterval = cf.BlockRestartInterval
	opt.BlockSize = cf.BlockSize
	opt.CompactionL0Trigger = cf.CompactionL0Trigger
	opt.CompactionTableSize = cf.CompactionTableSize
	opt.CompactionTotalSize = cf.CompactionTotalSize
	opt.CompactionTotalSizeMultiplier = cf.CompactionTotalSizeMultiplier
	opt.WriteBuffer = cf.WriteBuffer
	opt.WriteL0PauseTrigger = cf.WriteL0PauseTrigger
	opt.WriteL0SlowdownTrigger = cf.WriteL0SlowdownTrigger

	return leveldb.OpenFile(kvDir, &opt)
}

func (a *Append) writeBatchToKV(bufReqs []*request) error {
	var batch leveldb.Batch
	var lastPointer []byte
	for _, req := range bufReqs {
		log.Debug("write request to kv", zap.Stringer("request", req))

		pointer, err := req.valuePointer.MarshalBinary()
		if err != nil {
			panic(err)
		}

		lastPointer = pointer
		batch.Put(encodeTSKey(req.ts()), pointer)
	}
	batch.Put(headPointerKey, lastPointer)

	for {
		err := a.metadata.Write(&batch, nil)
		if err == nil {
			a.headPointer = bufReqs[len(bufReqs)-1].valuePointer
			return nil
		}
		errorCount.WithLabelValues("batch_write_kv").Add(1.0)

		// when write to vlog success, but the disk is full when write to KV here, it will cause write err
		// we just retry of quit when Append is closed
		log.Error("Failed to write batch", zap.Error(err))
		if a.isClosed() {
			log.Info("Stop writing because the appender is closed.")
			return err
		}
		time.Sleep(time.Second)
		continue
	}
}

// AllMatched implement Storage.AllMatched
func (a *Append) AllMatched() bool {
	return a.sorter.allMatched()
}
