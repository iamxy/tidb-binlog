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

package pump

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/etcd"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb-binlog/pkg/security"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tipb/go-binlog"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/integration"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var testEtcdCluster *integration.ClusterV3

func TestPump(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	TestingT(t)
}

type writeBinlogSuite struct{}

var _ = Suite(&writeBinlogSuite{})

func (s *writeBinlogSuite) TestReturnErrIfClusterIDMismatched(c *C) {
	server := &Server{clusterID: 42}
	req := &binlog.WriteBinlogReq{}
	req.ClusterID = 53
	resp, err := server.writeBinlog(context.Background(), req, false)
	c.Assert(resp, IsNil)
	c.Assert(err, ErrorMatches, ".*mismatch.*")
}

func (s *writeBinlogSuite) TestIgnoreReqWithInvalidPayload(c *C) {
	server := &Server{clusterID: 42}
	req := &binlog.WriteBinlogReq{ClusterID: 42, Payload: []byte("invalid")}
	resp, err := server.writeBinlog(context.Background(), req, false)
	c.Assert(resp.Errmsg, Equals, "unexpected EOF")
	c.Assert(err, NotNil)
}

type fakeNode struct{}

func (n *fakeNode) ID() string                                                   { return "fakenode-long" }
func (n *fakeNode) ShortID() string                                              { return "fakenode" }
func (n *fakeNode) RefreshStatus(ctx context.Context, status *node.Status) error { return nil }
func (n *fakeNode) Heartbeat(ctx context.Context) <-chan error                   { return make(chan error) }
func (n *fakeNode) Notify(ctx context.Context) error                             { return nil }
func (n *fakeNode) NodeStatus() *node.Status                                     { return &node.Status{State: node.Paused} }
func (n *fakeNode) NodesStatus(ctx context.Context) ([]*node.Status, error) {
	return []*node.Status{}, nil
}
func (n *fakeNode) Quit() error { return nil }

func (s *writeBinlogSuite) TestDetectNoOnline(c *C) {
	server := &Server{clusterID: 42, node: &fakeNode{}}

	log := new(binlog.Binlog)
	data, err := log.Marshal()
	if err != nil {
		c.Fatal("Fail to marshal binlog")
	}
	req := &binlog.WriteBinlogReq{ClusterID: 42, Payload: data}
	_, err = server.writeBinlog(context.Background(), req, false)
	c.Assert(err, ErrorMatches, ".*no online.*")
}

type pullBinlogsSuite struct{}

var _ = Suite(&pullBinlogsSuite{})

type fakePullBinlogsServer struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*binlog.PullBinlogResp
}

func newFakePullBinlogsServer() *fakePullBinlogsServer {
	return &fakePullBinlogsServer{
		ctx:  context.Background(),
		sent: []*binlog.PullBinlogResp{},
	}
}

func (x *fakePullBinlogsServer) Context() context.Context { return x.ctx }
func (x *fakePullBinlogsServer) Send(m *binlog.PullBinlogResp) error {
	x.sent = append(x.sent, m)
	return nil
}

func (s *pullBinlogsSuite) TestReturnErrIfClusterIDMismatched(c *C) {
	server := &Server{clusterID: 42}
	req := &binlog.PullBinlogReq{ClusterID: 43}
	err := server.PullBinlogs(req, newFakePullBinlogsServer())
	c.Assert(err, ErrorMatches, ".*mismatch.*")
}

type noOpStorage struct{}

func (s *noOpStorage) AllMatched() bool                            { return true }
func (s *noOpStorage) WriteBinlog(binlogItem *binlog.Binlog) error { return nil }
func (s *noOpStorage) GetGCTS() int64                              { return 0 }
func (s *noOpStorage) GC(ts int64)                                 {}
func (s *noOpStorage) MaxCommitTS() int64                          { return 0 }
func (s *noOpStorage) GetBinlog(ts int64) (*binlog.Binlog, error)  { return nil, nil }
func (s *noOpStorage) PullCommitBinlog(ctx context.Context, last int64) (<-chan []byte, <-chan error) {
	return make(chan []byte), make(chan error)
}
func (s *noOpStorage) Close() error { return nil }

type fakePullable struct{ noOpStorage }

func (s *fakePullable) PullCommitBinlog(ctx context.Context, last int64) (<-chan []byte, <-chan error) {
	chl := make(chan []byte)
	errs := make(chan error)
	go func() {
		for i := 0; i < 3; i++ {
			chl <- []byte(fmt.Sprintf("payload_%d", i))
		}
		close(chl)
	}()
	return chl, errs
}

func (s *pullBinlogsSuite) TestPullBinlogFromStorage(c *C) {
	ctx := context.Background()
	server := &Server{clusterID: 42, storage: &fakePullable{}, ctx: ctx}
	req := &binlog.PullBinlogReq{
		ClusterID: 42,
		StartFrom: binlog.Pos{
			Suffix: 1,
			Offset: 97,
		},
	}
	stream := newFakePullBinlogsServer()
	err := server.PullBinlogs(req, stream)
	c.Assert(err, IsNil)
	c.Assert(stream.sent, HasLen, 3)
	for i, resp := range stream.sent {
		c.Assert(string(resp.Entity.Payload), Equals, fmt.Sprintf("payload_%d", i))
	}
}

type genForwardBinlogSuite struct{}

var _ = Suite(&genForwardBinlogSuite{})

func (s *genForwardBinlogSuite) TestShouldExitWhenCanceled(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{
		clusterID: 42,
		storage:   &fakePullable{},
		ctx:       ctx,
		cfg: &Config{
			GenFakeBinlogInterval: 1,
		},
	}
	signal := make(chan struct{})
	server.wg.Add(1)
	go func() {
		server.genForwardBinlog()
		close(signal)
	}()
	cancel()
	select {
	case <-signal:
	case <-time.After(100 * time.Millisecond):
		c.Fatal("Can't be canceled")
	}
}

type fakeWritable struct {
	noOpStorage
	binlogs []binlog.Binlog
}

func (s *fakeWritable) WriteBinlog(binlogItem *binlog.Binlog) error {
	s.binlogs = append(s.binlogs, *binlogItem)
	return nil
}

func (s *genForwardBinlogSuite) TestSendFakeBinlog(c *C) {
	origGetTSO := utilGetTSO
	utilGetTSO = func(cli pd.Client) (int64, error) {
		return 42, nil
	}
	defer func() {
		utilGetTSO = origGetTSO
	}()
	ctx, cancel := context.WithCancel(context.Background())
	storage := fakeWritable{binlogs: make([]binlog.Binlog, 1)}
	server := &Server{
		clusterID: 42,
		storage:   &storage,
		ctx:       ctx,
		cfg: &Config{
			GenFakeBinlogInterval: 1,
		},
	}
	server.wg.Add(1)
	go func() {
		server.genForwardBinlog()
	}()
	time.Sleep(time.Duration(server.cfg.GenFakeBinlogInterval*2) * time.Second)
	cancel()
	server.wg.Wait()
	c.Assert(len(storage.binlogs) > 0, IsTrue)
	var foundFake bool
	for _, bl := range storage.binlogs {
		if bl.Tp == binlog.BinlogType_Rollback && bl.CommitTs == 42 && bl.StartTs == 42 {
			foundFake = true
		}
	}
	c.Assert(foundFake, IsTrue)
}

type startHeartbeatSuite struct{}

var _ = Suite(&startHeartbeatSuite{})

type heartbeartNode struct {
	fakeNode
	errChl chan error
}

func (n *heartbeartNode) Heartbeat(ctx context.Context) <-chan error {
	return n.errChl
}

func (s *startHeartbeatSuite) TestOnlyLogNonCancelErr(c *C) {
	var hook util.LogHook
	hook.SetUp()
	defer hook.TearDown()

	errChl := make(chan error)
	node := heartbeartNode{errChl: errChl}
	server := &Server{node: &node, ctx: context.Background()}
	server.startHeartbeat()
	errChl <- errors.New("test")
	errChl <- context.Canceled
	close(errChl)

	c.Assert(hook.Entrys, HasLen, 1)
	c.Assert(hook.Entrys[0].Message, Matches, ".*send heartbeat failed.*")
}

type printServerInfoSuite struct{}

var _ = Suite(&printServerInfoSuite{})

type dummyStorage struct {
	noOpStorage
	gcTS        int64
	maxCommitTS int64
}

func (ds *dummyStorage) MaxCommitTS() int64 {
	return ds.maxCommitTS
}

func (ds *dummyStorage) GetGCTS() int64 {
	return ds.gcTS
}

func (ds *dummyStorage) GC(ts int64) {
	ds.gcTS = ts
}

func (s *printServerInfoSuite) TestReturnWhenServerIsDone(c *C) {
	originalInterval := serverInfoOutputInterval
	serverInfoOutputInterval = 50 * time.Millisecond

	defer func() {
		serverInfoOutputInterval = originalInterval
	}()

	var hook util.LogHook
	hook.SetUp()
	defer hook.TearDown()

	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{storage: &dummyStorage{}, ctx: ctx}
	signal := make(chan struct{})

	server.wg.Add(1)
	go func() {
		server.printServerInfo()
		close(signal)
	}()

	time.Sleep(3 * serverInfoOutputInterval)
	cancel()

	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Fail to return when server's done")
	}

	for i, entry := range hook.Entrys {
		if i == len(hook.Entrys)-1 {
			c.Assert(entry.Message, Matches, ".*printServerInfo exit.*")
		} else {
			c.Assert(entry.Message, Matches, ".*server info tick.*")
		}
	}
}

type pumpStatusSuite struct{}

var _ = Suite(&pumpStatusSuite{})

type failStatusNode struct {
	node.Node
}

func (n failStatusNode) NodesStatus(ctx context.Context) ([]*node.Status, error) {
	return nil, errors.New("Query Status Failed")
}

func (s *pumpStatusSuite) TestShouldRetErrIfFailToGetStatus(c *C) {
	server := &Server{node: failStatusNode{}}
	status := server.PumpStatus()
	c.Assert(status.ErrMsg, Equals, "Query Status Failed")
}

type nodeWithStatus struct {
	node.Node
}

func (n nodeWithStatus) NodesStatus(ctx context.Context) (sts []*node.Status, err error) {
	sts = []*node.Status{
		{NodeID: "pump1"},
		{NodeID: "pump4"},
		{NodeID: "pump2"},
	}
	return
}

func (s *pumpStatusSuite) TestShouldRetErrIfFailToGetCommitTS(c *C) {
	origGetTSO := utilGetTSO
	utilGetTSO = func(cli pd.Client) (int64, error) { return -1, errors.New("PD Failure") }
	defer func() { utilGetTSO = origGetTSO }()

	server := &Server{node: nodeWithStatus{}}
	status := server.PumpStatus()
	c.Assert(status.ErrMsg, Equals, "PD Failure")
}

func (s *pumpStatusSuite) TestCorrectlyGetStatusAndCommitTS(c *C) {
	origGetTSO := utilGetTSO
	utilGetTSO = func(cli pd.Client) (int64, error) { return 1024, nil }
	defer func() { utilGetTSO = origGetTSO }()

	server := &Server{node: nodeWithStatus{}}
	status := server.PumpStatus()
	c.Assert(status.ErrMsg, Equals, "")
	c.Assert(status.CommitTS, Equals, int64(1024))
	c.Assert(status.StatusMap, HasLen, 3)
	c.Assert(status.StatusMap, HasKey, "pump1")
	c.Assert(status.StatusMap, HasKey, "pump2")
	c.Assert(status.StatusMap, HasKey, "pump4")
}

type commitStatusSuite struct{}

var _ = Suite(commitStatusSuite{})

type nodeWithState struct {
	node.Node
	state    string
	newState string
}

func (n *nodeWithState) NodeStatus() *node.Status {
	return &node.Status{
		State:  n.state,
		NodeID: "test",
		Addr:   "192.168.1.1",
	}
}
func (n *nodeWithState) RefreshStatus(ctx context.Context, status *node.Status) error {
	n.newState = status.State
	return nil
}

func (s commitStatusSuite) TestShouldChangeToCorrectState(c *C) {
	tests := map[string]string{
		node.Pausing: node.Paused,
		node.Online:  node.Paused,
		"unknown":    "unknown",
	}
	for from, to := range tests {
		server := &Server{
			node:    &nodeWithState{state: from},
			storage: &dummyStorage{},
		}
		server.commitStatus()
		c.Assert(server.node.(*nodeWithState).newState, Equals, to)
	}
}

type closeSuite struct{}

var _ = Suite(&closeSuite{})

func (s *closeSuite) TestSkipIfAlreadyClosed(c *C) {
	var hook util.LogHook
	hook.SetUp()
	defer hook.TearDown()

	server := Server{isClosed: 1}
	server.Close()

	c.Assert(len(hook.Entrys), Less, 2)
}

type gcBinlogFileSuite struct{}

var _ = Suite(&gcBinlogFileSuite{})

func (s *gcBinlogFileSuite) TestShouldGCMinDrainerTSO(c *C) {
	storage := dummyStorage{}

	ctx, cancel := context.WithCancel(context.Background())

	cli := etcd.NewClient(testEtcdCluster.RandClient(), "drainers")
	registry := node.NewEtcdRegistry(cli, time.Second)
	server := Server{
		ctx:        ctx,
		storage:    &storage,
		node:       &pumpNode{EtcdRegistry: registry},
		gcDuration: time.Hour,
	}

	millisecond := time.Now().Add(-server.gcDuration).UnixNano() / 1000 / 1000
	gcTS := int64(oracle.EncodeTSO(millisecond))

	inAlertGCMS := millisecond + 10*time.Minute.Nanoseconds()/1000/1000
	inAlertGCTS := int64(oracle.EncodeTSO(inAlertGCMS))

	outAlertGCMS := millisecond + (earlyAlertGC+10*time.Minute).Nanoseconds()/1000/1000
	outAlertGCTS := int64(oracle.EncodeTSO(outAlertGCMS))

	mustUpdateNode(ctx, registry, "drainers/1", &node.Status{MaxCommitTS: inAlertGCTS, State: node.Online})
	mustUpdateNode(ctx, registry, "drainers/2", &node.Status{MaxCommitTS: 1002, State: node.Online})
	// drainers/3 is set to be offline, so its MaxCommitTS is expected to be ignored
	mustUpdateNode(ctx, registry, "drainers/3", &node.Status{MaxCommitTS: outAlertGCTS, State: node.Offline})

	// Set a shorter interval because we don't really want to wait 1 hour
	origInterval := gcInterval
	gcInterval = 100 * time.Microsecond
	defer func() {
		gcInterval = origInterval
	}()

	server.wg.Add(1)
	go server.gcBinlogFile()

	// Give the GC goroutine some time to do the job,
	// the latency of the underlying etcd query can be much larger than gcInterval
	time.Sleep(1000 * gcInterval)
	cancel()

	c.Assert(storage.gcTS, GreaterEqual, gcTS)
	// todo: add in and out of alert test while binlog has failpoint
}

func mustUpdateNode(pctx context.Context, r *node.EtcdRegistry, prefix string, status *node.Status) {
	if err := r.UpdateNode(pctx, prefix, status); err != nil {
		panic(err)
	}
}

type waitCommitTSSuite struct{}

var _ = Suite(&waitCommitTSSuite{})

func (s *waitCommitTSSuite) TestShouldStoppedWhenDone(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	storage := dummyStorage{maxCommitTS: 1024}
	server := Server{
		storage: &storage,
		ctx:     ctx,
	}
	signal := make(chan struct{})
	go func() {
		err := server.waitUntilCommitTSSaved(ctx, int64(2000), time.Millisecond)
		close(signal)
		c.Assert(err, ErrorMatches, "context canceled")
	}()
	cancel()
	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Doesn't stop in time when done")
	}
}

func (s *waitCommitTSSuite) TestShouldWaitUntilTs(c *C) {
	storage := dummyStorage{maxCommitTS: 1024}
	server := Server{
		storage: &storage,
		ctx:     context.Background(),
	}
	signal := make(chan struct{})
	go func() {
		err := server.waitUntilCommitTSSaved(server.ctx, int64(2000), time.Millisecond)
		close(signal)
		c.Assert(err, IsNil)
	}()
	storage.maxCommitTS = 2000
	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Doesn't stop in time after ts is reached")
	}
}

type mockPdCli struct {
	pd.Client
}

func (pc *mockPdCli) GetClusterID(ctx context.Context) uint64 {
	return 8012
}

func (pc *mockPdCli) Close() {}

type newServerSuite struct {
	origGetPdClientFn         func(string, security.Config) (pd.Client, error)
	origNewKVStoreFn          func(string) (kv.Storage, error)
	origNewTiKVLockResolverFn func([]string, config.Security, ...pd.ClientOption) (*tikv.LockResolver, error)
	cfg                       *Config
}

var _ = Suite(&newServerSuite{})

func (s *newServerSuite) SetUpTest(c *C) {
	s.origGetPdClientFn = getPdClientFn
	s.origNewKVStoreFn = newKVStoreFn
	s.origNewTiKVLockResolverFn = newTiKVLockResolverFn

	// build config
	etcdClient := testEtcdCluster.RandClient()
	s.cfg = &Config{
		ListenAddr:        "http://192.168.199.100:8260",
		AdvertiseAddr:     "http://192.168.199.100:8260",
		EtcdURLs:          strings.Join(etcdClient.Endpoints(), ","),
		EtcdDialTimeout:   defaultEtcdDialTimeout,
		DataDir:           path.Join(c.MkDir(), "pump"),
		HeartbeatInterval: 1500,
		LogLevel:          "debug",
		MetricsAddr:       "192.168.199.100:5000",
		MetricsInterval:   15,
		GC:                "7",
		Security: security.Config{
			SSLCA:   "/path/to/ca.pem",
			SSLCert: "/path/to/drainer.pem",
			SSLKey:  "/path/to/drainer-key.pem"},
	}
}

func (s *newServerSuite) TearDownTest(c *C) {
	getPdClientFn = s.origGetPdClientFn
	newKVStoreFn = s.origNewKVStoreFn
	newTiKVLockResolverFn = s.origNewTiKVLockResolverFn
	s.cfg = nil
}

func (s *newServerSuite) TestCreateNewPumpServerWithInvalidPDClient(c *C) {
	getPdClientFn = func(string, security.Config) (pd.Client, error) {
		return nil, errors.New("invalid client")
	}
	p, err := NewServer(s.cfg)
	c.Assert(p, IsNil)
	c.Assert(err, ErrorMatches, "invalid client")
}

func (s *newServerSuite) TestCreateNewPumpServerWithInvalidEtcdURLs(c *C) {
	getPdClientFn = func(string, security.Config) (pd.Client, error) {
		return &mockPdCli{}, nil
	}
	s.cfg.EtcdURLs = "testInvalidUrls"
	p, err := NewServer(s.cfg)
	c.Assert(p, IsNil)
	c.Assert(err, ErrorMatches, "URL.*")
}

func (s *newServerSuite) TestCreateNewPumpServer(c *C) {
	getPdClientFn = func(string, security.Config) (pd.Client, error) {
		return &mockPdCli{}, nil
	}
	newTiKVLockResolverFn = func([]string, config.Security, ...pd.ClientOption) (*tikv.LockResolver, error) {
		return nil, nil
	}
	newKVStoreFn = func(path string) (kv.Storage, error) {
		return nil, nil
	}

	p, err := NewServer(s.cfg)
	c.Assert(err, IsNil)
	c.Assert(p.clusterID, Equals, uint64(8012))
}

type startNode struct {
	status *node.Status
}

func (n *startNode) ID() string                                                   { return "startnode-long" }
func (n *startNode) ShortID() string                                              { return "startnode" }
func (n *startNode) RefreshStatus(ctx context.Context, status *node.Status) error { return nil }
func (n *startNode) Heartbeat(ctx context.Context) <-chan error                   { return make(chan error) }
func (n *startNode) Notify(ctx context.Context) error                             { return nil }
func (n *startNode) NodeStatus() *node.Status {
	return n.status
}
func (n *startNode) NodesStatus(ctx context.Context) ([]*node.Status, error) {
	return []*node.Status{n.status}, nil
}
func (n *startNode) Quit() error { return nil }

type startStorage struct {
	sig chan struct{} // use sig to notify that the closing work has been done
}

func (s *startStorage) AllMatched() bool                            { return true }
func (s *startStorage) WriteBinlog(binlogItem *binlog.Binlog) error { return nil }
func (s *startStorage) GetGCTS() int64                              { return 0 }
func (s *startStorage) GC(ts int64)                                 {}
func (s *startStorage) MaxCommitTS() int64                          { return 0 }
func (s *startStorage) GetBinlog(ts int64) (*binlog.Binlog, error) {
	return nil, errors.New("server_test")
}
func (s *startStorage) PullCommitBinlog(ctx context.Context, last int64) (<-chan []byte, <-chan error) {
	return make(chan []byte), make(<-chan error)
}
func (s *startStorage) Close() error {
	<-s.sig
	// wait for pump server to change node back to startNode, or test etcd server might be closed
	time.Sleep(20 * time.Microsecond)
	return nil
}

type startServerSuite struct {
	origUtilGetTSO func(pd.Client) (int64, error)
}

var _ = Suite(&startServerSuite{})

func (s *startServerSuite) SetUpTest(c *C) {
	s.origUtilGetTSO = utilGetTSO
}

func (s *startServerSuite) TearDownTest(c *C) {
	utilGetTSO = s.origUtilGetTSO
}

func (s *startServerSuite) TestStartPumpServer(c *C) {
	utilGetTSO = func(pd.Client) (int64, error) {
		return 0, nil
	}
	etcdClient := testEtcdCluster.RandClient()
	r := rand.New(rand.NewSource(time.Now().Unix()))
	randPort := int64(r.Intn(5536) + 60000)
	cfg := &Config{
		ListenAddr:        "http://127.0.0.1:8250",
		AdvertiseAddr:     "http://127.0.0.1:8260",
		Socket:            "unix://127.0.0.1:" + strconv.FormatInt(randPort, 10) + "/hello/world",
		EtcdURLs:          strings.Join(etcdClient.Endpoints(), ","),
		EtcdDialTimeout:   defaultEtcdDialTimeout,
		DataDir:           path.Join(c.MkDir(), "pump"),
		HeartbeatInterval: 1500,
		LogLevel:          "debug",
	}
	ctx, cancel := context.WithCancel(context.Background())
	grpcOpts := []grpc.ServerOption{grpc.MaxRecvMsgSize(GlobalConfig.maxMsgSize)}
	sig := make(chan struct{})
	startNodeImpl := &startNode{status: &node.Status{State: node.Online, NodeID: "startnode-long", Addr: "http://192.168.199.100:8260"}}
	p := &Server{
		dataDir:    cfg.DataDir,
		storage:    &startStorage{sig: sig},
		clusterID:  8012,
		node:       startNodeImpl,
		tcpAddr:    cfg.ListenAddr,
		unixAddr:   cfg.Socket,
		gs:         grpc.NewServer(grpcOpts...),
		ctx:        ctx,
		cancel:     cancel,
		tiStore:    nil,
		gcDuration: 24 * time.Hour,
		pdCli:      nil,
		cfg:        cfg,
		triggerGC:  make(chan time.Time),
		pullClose:  make(chan struct{})}
	defer func() {
		close(sig)
		p.Close()
	}()
	go func() {
		if err := p.Start(); err != nil {
			c.Logf("Pump server stopped in error: %v", err)
		}
	}()

	// wait until the server is online
	timeEnd := time.After(5 * time.Second)
	getInterval := time.NewTicker(500 * time.Microsecond)
WAIT:
	for {
		select {
		case <-getInterval.C:
			resp, err := http.Get("http://127.0.0.1:8250/status")
			if err != nil {
				c.Assert(err, ErrorMatches, ".*connect: connection refused.*")
				continue
			}
			// should receive valid and correct node status info
			c.Assert(resp, NotNil)
			defer resp.Body.Close()
			bodyByte, err := io.ReadAll(resp.Body)
			c.Assert(err, IsNil)

			nodesStatus := &HTTPStatus{}
			err = json.Unmarshal(bodyByte, nodesStatus)
			c.Assert(err, IsNil)
			c.Assert(nodesStatus.ErrMsg, Equals, "")
			c.Assert(nodesStatus.CommitTS, Equals, int64(0))
			fakeNodeStatus, ok := nodesStatus.StatusMap["startnode-long"]
			c.Assert(ok, IsTrue)
			c.Assert(fakeNodeStatus.NodeID, Equals, "startnode-long")
			c.Assert(fakeNodeStatus.Addr, Equals, "http://192.168.199.100:8260")
			c.Assert(fakeNodeStatus.State, Equals, node.Online)
			break WAIT
		case <-timeEnd:
			getInterval.Stop()
			c.Fatal("Wait pump to be online for more than 5 seconds")
		}
	}
	getInterval.Stop()

	// string converted from json may contain char '\n' which can't be matched with '.' but can be matched with `[\s\S]`
	// test AllDrainer
	resultStr := httpRequest(c, http.MethodGet, "http://127.0.0.1:8250/drainers")
	c.Assert(resultStr, Matches, `.*can't provide service[\s\S]*`)
	// test BinlogByTS
	resultStr = httpRequest(c, http.MethodGet, "http://127.0.0.1:8250/debug/binlog/2")
	c.Assert(resultStr, Matches, `.*server_test[\s\S]*`)
	// test triggerGC
	resultStr = httpRequest(c, http.MethodPost, "http://127.0.0.1:8250/debug/gc/trigger")
	c.Assert(resultStr, Matches, `.*trigger gc success[\s\S]*`)

	// change node to pump node
	cli := etcd.NewClient(testEtcdCluster.RandClient(), "drainers")
	registry := node.NewEtcdRegistry(cli, time.Second)
	p.node = &pumpNode{
		EtcdRegistry: registry,
		status:       &node.Status{State: node.Online, NodeID: "startnode-long", Addr: "http://192.168.199.100:8260"},
		getMaxCommitTs: func() int64 {
			return 0
		},
	}
	drainerNodeStatus := &node.Status{
		NodeID:      "start_pump_test",
		State:       node.Online,
		MaxCommitTS: 2,
	}
	err := registry.UpdateNode(context.Background(), "drainers", drainerNodeStatus)
	c.Assert(err, IsNil)
	// test AllDrainer
	resultStr = httpRequest(c, http.MethodGet, "http://127.0.0.1:8250/drainers")
	c.Assert(resultStr, Matches, `.*start_pump_test[\s\S]*`)
	// test close node
	resultStr = httpRequest(c, http.MethodPut, "http://127.0.0.1:8250/state/startnode-long/close")
	c.Assert(resultStr, Matches, `[\s\S]*success[\s\S]*`)

	select {
	case sig <- struct{}{}:
		// change back to start node to avoid closing the whole etcd server
		p.node = startNodeImpl
	case <-time.After(2 * time.Second):
		c.Fatal("Fail to close server in 2s")
	}
}

// change to receive request of get, post, put
func httpRequest(c *C, method, url string) string {
	client := &http.Client{}
	request, err := http.NewRequest(method, url, nil)
	c.Assert(err, IsNil)
	resp, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	defer resp.Body.Close()
	bodyByte, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	bodyStr := string(bodyByte)
	return bodyStr
}
