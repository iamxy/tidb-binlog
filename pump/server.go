package pump

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/soheilhy/cmux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return make([]byte, 1024*1024)
	},
}

var genBinlogInterval = 3 * time.Second
var pullBinlogInterval = 50 * time.Millisecond

const maxMsgSizeForGRPC = 1024 * 1024 * 1024
const slowDist = 30 * time.Millisecond

// use latestBinlogFile to record the latest binlog file the pump works on
var latestBinlogFile = fileName(0)

// Server implements the gRPC interface,
// and maintains pump's status at run time.
type Server struct {
	// RWMutex protects dispatcher
	sync.RWMutex

	// dispatcher keeps all opened binloggers which is indexed by clusterID.
	dispatcher map[string]Binlogger

	// dataDir is the root directory of all pump data
	// |
	// +-- .node
	// |   |
	// |   +-- nodeID
	// |
	// +-- clusters
	//     |
	//     +-- 100
	//     |   |
	//     |   +-- binlog.000001
	//     |   |
	//     |   +-- binlog.000002
	//     |   |
	//     |   +-- ...
	//     |
	//     +-- 200
	//         |
	//         +-- binlog.000001
	//         |
	//         +-- binlog.000002
	//         |
	//         +-- ...
	//
	dataDir string

	clusterID string

	// node maintains the status of this pump and interact with etcd registry
	node Node

	tcpAddr  string
	unixAddr string
	gs       *grpc.Server
	ctx      context.Context
	cancel   context.CancelFunc
	gc       time.Duration
	metrics  *metricClient
	// it would be set false while there are new binlog coming, would be set true every genBinlogInterval
	needGenBinlog AtomicBool
	pdCli         pd.Client
}

func init() {
	// tracing has suspicious leak problem, so disable it here.
	// it must be set before any real grpc operation.
	grpc.EnableTracing = false
}

// NewServer returns a instance of pump server
func NewServer(cfg *Config) (*Server, error) {
	n, err := NewPumpNode(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var metrics *metricClient
	if cfg.MetricsAddr != "" && cfg.MetricsInterval != 0 {
		metrics = &metricClient{
			addr:     cfg.MetricsAddr,
			interval: cfg.MetricsInterval,
		}
	}

	// use tiStore's currentVersion method to get the ts from tso
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// get cluster ID
	pdCli, err := pd.NewClient(urlv.StringSlice())
	if err != nil {
		return nil, errors.Trace(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	clusterID := pdCli.GetClusterID(ctx)
	log.Infof("clusterID of pump server is %v", clusterID)

	return &Server{
		dispatcher: make(map[string]Binlogger),
		dataDir:    cfg.DataDir,
		clusterID:  fmt.Sprintf("%d", clusterID),
		node:       n,
		tcpAddr:    cfg.ListenAddr,
		unixAddr:   cfg.Socket,
		gs:         grpc.NewServer(grpc.MaxMsgSize(maxMsgSizeForGRPC)),
		ctx:        ctx,
		cancel:     cancel,
		metrics:    metrics,
		gc:         time.Duration(cfg.GC) * 24 * time.Hour,
		pdCli:      pdCli,
	}, nil
}

// inits scans the dataDir to find all clusterIDs, and creates binlogger for each,
// then adds them to dispathcer map
func (s *Server) init() error {
	clusterDir := path.Join(s.dataDir, "clusters")
	if !file.Exist(clusterDir) {
		if err := os.MkdirAll(clusterDir, file.PrivateDirMode); err != nil {
			return errors.Trace(err)
		}
	}

	names, err := file.ReadDir(clusterDir)
	if err != nil {
		return errors.Trace(err)
	}

	for _, n := range names {
		binlogDir := path.Join(clusterDir, n)
		binlogger, err := OpenBinlogger(binlogDir)
		if err != nil {
			return errors.Trace(err)
		}
		s.dispatcher[n] = binlogger
	}

	// init cluster data dir if not exist
	s.dispatcher[s.clusterID], err = s.getBinloggerToWrite(s.clusterID)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *Server) getBinloggerToWrite(cid string) (Binlogger, error) {
	s.Lock()
	defer s.Unlock()
	blr, ok := s.dispatcher[cid]
	if ok {
		return blr, nil
	}
	newblr, err := CreateBinlogger(path.Join(s.dataDir, "clusters", cid))
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.dispatcher[cid] = newblr
	return newblr, nil
}

func (s *Server) getBinloggerToRead(cid string) (Binlogger, error) {
	s.RLock()
	defer s.RUnlock()
	blr, ok := s.dispatcher[cid]
	if ok {
		return blr, nil
	}
	return nil, errors.NotFoundf("no binlogger of clusterID: %s", cid)
}

// WriteBinlog implements the gRPC interface of pump server
func (s *Server) WriteBinlog(ctx context.Context, in *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	var err error
	beginTime := time.Now()
	defer func() {
		var label string
		if err != nil {
			label = "fail"
		} else {
			label = "succ"
		}
		rpcHistogram.WithLabelValues("WriteBinlog", label).Observe(time.Since(beginTime).Seconds())
		rpcCounter.WithLabelValues("WriteBinlog", label).Add(1)
	}()

	s.needGenBinlog.Set(false)
	cid := fmt.Sprintf("%d", in.ClusterID)
	ret := &binlog.WriteBinlogResp{}
	binlogger, err1 := s.getBinloggerToWrite(cid)
	if err1 != nil {
		ret.Errmsg = err1.Error()
		err = errors.Trace(err1)
		return ret, err
	}
	if err1 := binlogger.WriteTail(in.Payload); err1 != nil {
		ret.Errmsg = err1.Error()
		err = errors.Trace(err1)
		return ret, err
	}
	return ret, nil
}

// PullBinlogs sends binlogs in the streaming way
func (s *Server) PullBinlogs(in *binlog.PullBinlogReq, stream binlog.Pump_PullBinlogsServer) error {
	cid := fmt.Sprintf("%d", in.ClusterID)
	binlogger, err := s.getBinloggerToRead(cid)
	if err != nil {
		return errors.Trace(err)
	}
	pos := in.StartFrom

	for {
		binlogs, cache, err := binlogger.ReadFrom(pos, 100)
		if err != nil {
			return errors.Trace(err)
		}

		for i, bl := range binlogs {
			pos = bl.Pos
			pos.Offset += int64(len(bl.Payload) + 16)
			resp := &binlog.PullBinlogResp{Entity: bl}
			if err = stream.Send(resp); err != nil {
				log.Errorf("gRPC: pullBinlogs send stream error, %s", errors.ErrorStack(err))
				return errors.Trace(err)
			}
			bufPool.Put(cache[i])

		}
		// sleep 50 ms to prevent cpu occupied
		time.Sleep(pullBinlogInterval)
	}
}

// Start runs Pump Server to serve the listening addr, and maintains heartbeat to Etcd
func (s *Server) Start() error {
	// register this node
	if err := s.node.Register(s.ctx); err != nil {
		return errors.Annotate(err, "fail to register node to etcd")
	}

	// notify all cisterns
	if err := s.node.Notify(s.ctx); err != nil {
		// if fail, unregister this node
		if err := s.node.Unregister(s.ctx); err != nil {
			log.Error(errors.ErrorStack(err))
		}
		return errors.Annotate(err, "fail to notify all living drainer")
	}

	// start heartbeat loop
	errc := s.node.Heartbeat(s.ctx)
	go func() {
		for err := range errc {
			log.Error(err)
		}
	}()

	// init the server
	if err := s.init(); err != nil {
		return errors.Annotate(err, "fail to initialize pump server")
	}

	// start a TCP listener
	tcpURL, err := url.Parse(s.tcpAddr)
	if err != nil {
		return errors.Annotatef(err, "invalid listening tcp addr (%s)", s.tcpAddr)
	}
	tcpLis, err := net.Listen("tcp", tcpURL.Host)
	if err != nil {
		return errors.Annotatef(err, "fail to start TCP listener on %s", tcpURL.Host)
	}

	// start a UNIX listener
	unixURL, err := url.Parse(s.unixAddr)
	if err != nil {
		return errors.Annotatef(err, "invalid listening socket addr (%s)", s.unixAddr)
	}
	unixLis, err := net.Listen("unix", unixURL.Path)
	if err != nil {
		return errors.Annotatef(err, "fail to start UNIX listener on %s", unixURL.Path)
	}
	// start generate binlog if pump doesn't receive new binlogs
	go s.genForwardBinlog()

	// gc old binlog files
	go s.gcBinlogFile()

	// collect metrics to prometheus
	go s.startMetrics()

	// register pump with gRPC server and start to serve listeners
	binlog.RegisterPumpServer(s.gs, s)
	go s.gs.Serve(unixLis)

	// grpc and http will use the same tcp connection
	m := cmux.New(tcpLis)
	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())
	go s.gs.Serve(grpcL)

	http.HandleFunc("/status", s.Status)
	http.Handle("/metrics", prometheus.Handler())
	go http.Serve(httpL, nil)

	return m.Serve()
}

// gennerate rollback binlog can forward the drainer's latestCommitTs, and just be discarded without any side effects
func (s *Server) genFakeBinlog() ([]byte, error) {
	ts, err := s.getTSO()
	if err != nil {
		return nil, errors.Trace(err)
	}

	bl := &binlog.Binlog{
		Tp:       binlog.BinlogType_Rollback,
		CommitTs: ts,
	}
	payload, err := bl.Marshal()
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (s *Server) writeFakeBinlog() {
	// there are only one binlogger for the specified cluster
	// so we can use only one needGrenBinlog flag
	if s.needGenBinlog.Get() {
		for cid := range s.dispatcher {
			binlogger, err := s.getBinloggerToWrite(cid)
			if err != nil {
				log.Errorf("generate forward binlog, get binlogger err %v", err)
				return
			}
			payload, err := s.genFakeBinlog()
			if err != nil {
				log.Errorf("generate forward binlog, generate binlog err %v", err)
				return
			}
			err = binlogger.WriteTail(payload)
			if err != nil {
				log.Errorf("generate forward binlog, write binlog err %v", err)
				return
			}
			log.Info("generate fake binlog successfully")
		}
	}
	s.needGenBinlog.Set(true)
}

// we would generate binlog to forward the pump's latestCommitTs in drainer when there is no binlogs in this pump
func (s *Server) genForwardBinlog() {
	s.needGenBinlog.Set(true)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(genBinlogInterval):
			s.writeFakeBinlog()
		}
	}
}

func (s *Server) gcBinlogFile() {
	if s.gc == 0 {
		return
	}
	for {
		for _, b := range s.dispatcher {
			b.GC(s.gc)
		}
		time.Sleep(time.Hour)
	}
}

func (s *Server) startMetrics() {
	if s.metrics == nil {
		return
	}
	s.metrics.Start(s.ctx, s.node.ID())
}

// Status exposes pumps' status to HTTP handler.
func (s *Server) Status(w http.ResponseWriter, r *http.Request) {
	s.PumpStatus().Status(w, r)
}

// PumpStatus returns all pumps' status.
func (s *Server) PumpStatus() *HTTPStatus {
	status, err := s.node.NodesStatus(s.ctx)
	if err != nil {
		log.Errorf("get pumps' status error %v", err)
		return &HTTPStatus{
			ErrMsg: err.Error(),
		}
	}

	// get all pumps' latest binlog position
	binlogPos := make(map[string]binlog.Pos)
	for _, st := range status {
		seq, err := parseBinlogName(path.Base(st.LatestBinlogFile))
		if err != nil {
			log.Errorf("parse file name, error %v", err)
			return &HTTPStatus{
				ErrMsg: err.Error(),
			}
		}
		binlogPos[st.NodeID] = binlog.Pos{
			Suffix: seq,
		}
	}
	// get ts from pd
	commitTS, err := s.getTSO()
	if err != nil {
		log.Errorf("get ts from pd, error %v", err)
		return &HTTPStatus{
			ErrMsg: err.Error(),
		}
	}

	return &HTTPStatus{
		BinlogPos: binlogPos,
		CommitTS:  commitTS,
	}
}

func (s *Server) getTSO() (int64, error) {
	now := time.Now()
	physical, logical, err := s.pdCli.GetTS(context.Background())
	if err != nil {
		return 0, errors.Trace(err)
	}
	dist := time.Since(now)
	if dist > slowDist {
		log.Warnf("get timestamp too slow: %s", dist)
	}

	return int64(composeTS(physical, logical)), nil
}

// Close gracefully releases resource of pump server
func (s *Server) Close() {
	// unregister this node
	if err := s.node.Unregister(s.ctx); err != nil {
		log.Error(errors.ErrorStack(err))
	}
	// close tiStore
	if s.pdCli != nil {
		s.pdCli.Close()
	}
	// notify other goroutines to exit
	s.cancel()
	// stop the gRPC server
	s.gs.Stop()
}
