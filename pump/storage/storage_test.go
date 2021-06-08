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
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
	"github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/syndtr/goleveldb/leveldb"
)

type Log interface {
	Fatal(args ...interface{})
	Log(args ...interface{})
}

var _ Log = &testing.B{}
var _ Log = &testing.T{}
var _ Log = &check.C{}

type AppendSuit struct{}

var _ = check.Suite(&AppendSuit{})

func newAppend(t Log) *Append {
	return newAppendWithOptions(t, nil)
}

func newAppendWithOptions(t Log, options *Options) *Append {
	dir := path.Join(os.TempDir(), strconv.Itoa(rand.Int()))
	// t.Log("use dir: ", dir)
	err := os.Mkdir(dir, 0777)
	if err != nil {
		t.Fatal(err)
	}

	append, err := NewAppend(dir, options)
	if err != nil {
		t.Fatal(err)
	}

	return append
}

func cleanAppend(append *Append) {
	select {
	case <-append.close:
	default:
		append.Close()
	}

	os.RemoveAll(append.dir)
}

func (as *AppendSuit) TestNewAppend(c *check.C) {
	append := newAppend(c)
	defer cleanAppend(append)

	append.Close()
}

func (as *AppendSuit) TestBlockedWriteKVShouldNotStopWritingVlogs(c *check.C) {
	origThres := slowChaserThreshold
	defer func() {
		slowChaserThreshold = origThres
	}()
	slowChaserThreshold = 10 * time.Millisecond
	// Set KVChanCapacity to be extremely small so that we can feed it up
	store := newAppendWithOptions(c, DefaultOptions().WithKVChanCapacity(10))
	incoming := make(chan *request, 100)
	// We are not calling WriteBinlog here, instead, we set up an isolated
	// instance of writeToValueLog so that we can control the input and output
	// channels.
	written := store.writeToValueLog(incoming)

	// Send many requests to simulate WriteBinlog calls
	finished := make(chan struct{})
	go func() {
		reqs := createDummyReqs(4000)
		for _, r := range reqs {
			r.wg.Add(1)
			incoming <- r
		}
		for _, r := range reqs {
			r.wg.Wait()
		}
		close(finished)
	}()

	// The tiny `written` channel is never consumed, once it's full,
	// no new requests can be sent to it.
	// The slowChaser should detect this and make sure writes are not blocked.
	select {
	case <-finished:
		c.Assert(
			len(written),
			check.Equals,
			store.options.KVChanCapacity,
			check.Commentf("No consumer of the written channel is set up, it should be full at this point"),
		)
	case <-time.After(3 * time.Second):
		c.Fatal("Takes too long to finish writing binlogs, writing may have been blocked.")
	}
}

func (as *AppendSuit) TestVlogsShouldBeInSyncWhenDownStreamRecovers(c *check.C) {
	origThres := slowChaserThreshold
	defer func() {
		slowChaserThreshold = origThres
	}()
	slowChaserThreshold = 10 * time.Millisecond
	opts := DefaultOptions().WithKVChanCapacity(10).WithValueLogFileSize(9000)
	store := newAppendWithOptions(c, opts)
	incoming := make(chan *request, 100)
	written := store.writeToValueLog(incoming)

	const nReqs = 3000

	finished := make(chan struct{})
	go func() {
		reqs := createDummyReqs(nReqs)
		for _, r := range reqs[:nReqs/2] {
			r.wg.Add(1)
			incoming <- r
		}
		time.Sleep(100 * time.Millisecond)
		for _, r := range reqs[nReqs/2:] {
			r.wg.Add(1)
			incoming <- r
		}
		for _, r := range reqs {
			r.wg.Wait()
		}
		close(finished)
	}()
	<-finished

	receivedTs := make([]int64, 0, nReqs)
	for i := 0; i < nReqs/3; i++ {
		r := <-written
		receivedTs = append(receivedTs, r.startTS)
	}
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < nReqs/3; i++ {
		r := <-written
		receivedTs = append(receivedTs, r.startTS)
	}
	time.Sleep(200 * time.Millisecond)
	lastTs := receivedTs[len(receivedTs)-1]
	for lastTs < nReqs {
		r := <-written
		receivedTs = append(receivedTs, r.startTS)
		lastTs = r.startTS
	}
	c.Assert(len(receivedTs), check.GreaterEqual, nReqs)
	for i := 1; i < len(receivedTs); i++ {
		c.Assert(receivedTs[i], check.GreaterEqual, receivedTs[i-1])
	}
	c.Assert(receivedTs[0], check.Equals, int64(1))
	c.Assert(receivedTs[len(receivedTs)-1], check.Equals, int64(nReqs))
}

func (as *AppendSuit) TestCloseAndOpenAgain(c *check.C) {
	append := newAppend(c)
	defer cleanAppend(append)

	err := append.Close()
	c.Assert(err, check.IsNil)

	append, err = NewAppend(append.dir, append.options)
	c.Assert(err, check.IsNil)

	origHdlPtrSaveInt := handlePtrSaveInterval
	handlePtrSaveInterval = time.Millisecond
	defer func() {
		handlePtrSaveInterval = origHdlPtrSaveInt
	}()
	// populate some data and close open back to check the status
	populateBinlog(c, append, 128, 1)
	time.Sleep(time.Millisecond * 100)

	gcTS := append.gcTS.Load()
	maxCommitTS := append.maxCommitTS
	headPointer := append.headPointer
	handlePointer := append.handlePointer

	c.Log(gcTS, maxCommitTS, headPointer, handlePointer)

	err = append.Close()
	c.Assert(err, check.IsNil)

	append, err = NewAppend(append.dir, append.options)
	c.Assert(err, check.IsNil)

	c.Assert(gcTS, check.Equals, append.gcTS.Load())
	c.Assert(maxCommitTS, check.Equals, append.maxCommitTS)
	c.Assert(headPointer, check.Equals, append.headPointer)
	c.Assert(handlePointer, check.Equals, append.handlePointer)
	append.Close()
}

func (as *AppendSuit) TestWriteBinlogAndPullBack(c *check.C) {
	as.testWriteBinlogAndPullBack(c, -1, 1024)
}

func (as *AppendSuit) testWriteBinlogAndPullBack(c *check.C, prewriteValueSize int, binlogNum int) {
	appendStorage := newAppend(c)
	defer cleanAppend(appendStorage)

	populateBinlog(c, appendStorage, prewriteValueSize, binlogNum)

	for i := 0; i < 2; i++ {
		// close and open the check again
		if i == 1 {
			err := appendStorage.Close()
			c.Assert(err, check.IsNil)

			appendStorage, err = NewAppend(appendStorage.dir, appendStorage.options)
			c.Assert(err, check.IsNil)
		}

		ctx, cancel := context.WithCancel(context.Background())
		values, errs := appendStorage.PullCommitBinlog(ctx, 0)

		// pull the binlogs back and check sorted
		var binlogs []*pb.Binlog
	PullLoop:
		for {
			select {
			case value := <-values:
				getBinlog := new(pb.Binlog)
				err := getBinlog.Unmarshal(value)
				c.Assert(err, check.IsNil)
				binlogs = append(binlogs, getBinlog)
				if len(binlogs) == int(binlogNum) {
					break PullLoop
				}
			case <-time.After(time.Second * 5):
				c.Fatal("get value timeout")
			}
		}

		// check commitTS increasing
		for i := 1; i < len(binlogs); i++ {
			c.Assert(binlogs[i].CommitTs, check.Greater, binlogs[i-1].CommitTs)
		}
		c.Assert(errs, check.HasLen, 0)

		cancel()
	}
	appendStorage.Close()
}

func (as *AppendSuit) TestDoGCTS(c *check.C) {
	var value = make([]byte, 10)
	append := newAppend(c)
	defer cleanAppend(append)

	var i int64
	var n int64 = 1025 * 500
	var batch leveldb.Batch
	for i = 1; i < n; i++ {
		batch.Put(encodeTSKey(i), value)
	}
	err := append.metadata.Write(&batch, nil)
	c.Assert(err, check.IsNil)

	var gcTS int64 = 1024 * 300
	append.doGCTS(gcTS)

	for i = 1; i < n; i++ {
		_, err := append.metadata.Get(encodeTSKey(i), nil)
		if i <= gcTS {
			c.Assert(err, check.Equals, leveldb.ErrNotFound, check.Commentf("after gc still found ts: %v", i))
		} else {
			c.Assert(err, check.IsNil, check.Commentf("can't found ts: %v", i))
		}
	}
}

func (as *AppendSuit) TestReadWritePointer(c *check.C) {
	append := newAppend(c)
	defer cleanAppend(append)

	// check return zero valuePointer when the key not exist
	var readVP valuePointer
	var err error
	readVP, err = append.readPointer([]byte("no_exist_key"))
	c.Assert(err, check.IsNil)
	c.Assert(readVP, check.Equals, valuePointer{})

	// test with random key and valuePointer value
	fuzz := fuzz.New().NilChance(0)
	for i := 0; i < 100; i++ {
		var vp valuePointer
		var key []byte
		fuzz.Fuzz(&vp)
		// offset should >= 0, so just take abs(vp.Offset), when the random value is negative
		if vp.Offset < 0 {
			vp.Offset = -vp.Offset
		}
		fuzz.Fuzz(&key)

		err = append.savePointer(key, vp)
		c.Assert(err, check.IsNil)

		readVP, err = append.readPointer(key)
		c.Assert(err, check.IsNil)
		c.Assert(readVP, check.Equals, vp)
	}
}

func (as *AppendSuit) TestReadWriteGCTS(c *check.C) {
	append := newAppend(c)
	defer cleanAppend(append)

	ts, err := append.readGCTSFromDB()
	c.Assert(err, check.IsNil)
	c.Assert(ts, check.Equals, int64(0))

	err = append.saveGCTSToDB(100)
	c.Assert(err, check.IsNil)

	ts, err = append.readGCTSFromDB()
	c.Assert(err, check.IsNil)
	c.Assert(ts, check.Equals, int64(100))

	// close and open read again
	append.Close()
	append, err = NewAppend(append.dir, append.options)
	c.Assert(err, check.IsNil)

	c.Assert(append.gcTS.Load(), check.Equals, int64(100))
	append.Close()
}

// test helper to write binlogNum binlog to append
// If `prewriteValueSize` is less than or equal to 0, the size of prewriteValue for
// binlogs will be random integers between [1, 1024].
func populateBinlog(b Log, append *Append, prewriteValueSize int, binlogNum int) {
	fixedValueSize := prewriteValueSize > 0
	var prewriteValue []byte
	if fixedValueSize {
		prewriteValue = make([]byte, prewriteValueSize)
	} else {
		prewriteValue = make([]byte, 1024)
	}
	var ts int64

	seq := make(chan struct{}, 10)
	go func() {
		for i := 0; i < binlogNum; i++ {
			seq <- struct{}{}
		}
		close(seq)
	}()
	getTS := func() int64 {
		return atomic.AddInt64(&ts, 1)
	}

	nWorkers := 256
	if binlogNum < nWorkers {
		nWorkers = binlogNum
	}
	var wg sync.WaitGroup

	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range seq {
				startTS := getTS()

				// write P binlog
				binlog := pb.Binlog{
					Tp:      pb.BinlogType_Prewrite,
					StartTs: startTS,
				}
				if fixedValueSize {
					binlog.PrewriteValue = prewriteValue
				} else {
					size := 1 + rand.Intn(len(prewriteValue))
					binlog.PrewriteValue = prewriteValue[:size]
					b.Log("Setting prewrite value of length", size)
				}

				if err := append.WriteBinlog(&binlog); err != nil {
					b.Fatal(err)
				}

				// write C binlog
				binlog = pb.Binlog{
					Tp:       pb.BinlogType_Commit,
					StartTs:  startTS,
					CommitTs: getTS(),
				}
				if err := append.WriteBinlog(&binlog); err != nil {
					b.Fatal(err)
				}
			}
		}()
	}

	// wait finish populate data
	wg.Wait()
}

func (as *AppendSuit) TestNoSpace(c *check.C) {
	// TODO test when no space left
}

func (as *AppendSuit) TestResolve(c *check.C) {
	// TODO test the case we query tikv to know weather a txn a commit
	// is there a fake or mock kv.Storage and tikv.LockResolver to easy the test?
}

func (as *AppendSuit) TestWriteCBinlog(c *check.C) {
	a := newAppend(c)
	defer cleanAppend(a)

	pBinlog := pb.Binlog{StartTs: 42}
	var commitTs int64 = 50
	err := a.writeCBinlog(&pBinlog, commitTs)
	c.Assert(err, check.IsNil)

	cBinlog, err := a.readBinlogByTS(commitTs)
	c.Assert(err, check.IsNil)
	c.Assert(cBinlog.StartTs, check.Equals, pBinlog.StartTs)
	c.Assert(cBinlog.CommitTs, check.Equals, commitTs)
	c.Assert(cBinlog.Tp, check.Equals, pb.BinlogType_Commit)
}

func (as *AppendSuit) TestFeedPreWriteValue(c *check.C) {
	a := newAppend(c)
	defer cleanAppend(a)

	expectPBinlog := &pb.Binlog{
		Tp:             pb.BinlogType_Prewrite,
		StartTs:        42,
		PrewriteKey:    []byte("PrewriteKey"),
		PrewriteValue:  []byte("PrewriteValue"),
		DdlQuery:       []byte("create table t(a int);"),
		DdlJobId:       6,
		DdlSchemaState: 5,
	}

	req := a.writeBinlog(expectPBinlog)
	c.Assert(req.err, check.IsNil)

	cBinlog := &pb.Binlog{
		Tp:       pb.BinlogType_Commit,
		StartTs:  42,
		CommitTs: 50,
	}
	req = a.writeBinlog(cBinlog)
	c.Assert(req.err, check.IsNil)

	err := a.feedPreWriteValue(cBinlog)
	c.Assert(err, check.IsNil)

	c.Assert(cBinlog.StartTs, check.Equals, expectPBinlog.StartTs)
	c.Assert(cBinlog.CommitTs, check.Equals, int64(50))
	c.Assert(cBinlog.Tp, check.Equals, pb.BinlogType_Commit)
	c.Assert(cBinlog.PrewriteKey, check.IsNil)
	c.Assert(cBinlog.PrewriteValue, check.BytesEquals, expectPBinlog.PrewriteValue)
	c.Assert(cBinlog.DdlQuery, check.BytesEquals, expectPBinlog.DdlQuery)
	c.Assert(cBinlog.DdlJobId, check.Equals, expectPBinlog.DdlJobId)
	c.Assert(cBinlog.DdlSchemaState, check.Equals, expectPBinlog.DdlSchemaState)
}

type OpenDBSuit struct {
	dir string
}

var _ = check.Suite(&OpenDBSuit{})

func (s *OpenDBSuit) SetUpTest(c *check.C) {
	s.dir = c.MkDir()
}

func (s *OpenDBSuit) TestWhenConfigIsNotProvided(c *check.C) {
	_, err := openMetadataDB(s.dir, nil)
	c.Assert(err, check.IsNil)
}

func (s *OpenDBSuit) TestProvidedConfigValsNotOverwritten(c *check.C) {
	cf := KVConfig{
		BlockRestartInterval: 32,
		WriteL0PauseTrigger:  12,
	}
	_, err := openMetadataDB(s.dir, &cf)
	c.Assert(err, check.IsNil)
	c.Assert(cf.BlockRestartInterval, check.Equals, 32)
	c.Assert(cf.WriteL0PauseTrigger, check.Equals, 12)
	c.Assert(cf.BlockCacheCapacity, check.Equals, defaultStorageKVConfig.BlockCacheCapacity)
}

func createDummyReqs(n int) []*request {
	reqs := make([]*request, 0, n)
	for i := 0; i < n; i++ {
		ts := int64(i) + 1
		binlog := pb.Binlog{
			StartTs: ts,
			Tp:      pb.BinlogType_Prewrite,
		}
		payload, err := binlog.Marshal()
		if err != nil {
			panic(err)
		}
		r := &request{
			startTS: ts,
			payload: payload,
			tp:      pb.BinlogType_Prewrite,
		}
		reqs = append(reqs, r)
	}
	return reqs
}
