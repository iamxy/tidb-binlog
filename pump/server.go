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
	"encoding/json"

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

var genBinlogInterval = 3 * time.Second
var pullBinlogInterval = 50 * time.Millisecond

var maxMsgSize = 1024 * 1024 * 1024

const slowDist = 30 * time.Millisecond

// use latestPos and latestTS to record the latest binlog position and ts the pump works on
var (
	latestKafkaPos binlog.Pos
	latestFilePos  binlog.Pos
	latestTS       int64
)

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
	cfg           *Config
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
		gs:         grpc.NewServer(grpc.MaxMsgSize(maxMsgSize)),
		ctx:        ctx,
		cancel:     cancel,
		metrics:    metrics,
		gc:         time.Duration(cfg.GC) * 24 * time.Hour,
		pdCli:      pdCli,
		cfg:        cfg,
	}, nil
}

// inits scans the dataDir to find all clusterIDs, and creates binlogger for each,
// then adds them to dispathcer map
func (s *Server) init() error {
	// init cluster data dir if not exist
	var err error
	clusterDir := path.Join(s.dataDir, "clusters")
	if !file.Exist(clusterDir) {
		if err := os.MkdirAll(clusterDir, file.PrivateDirMode); err != nil {
			return errors.Trace(err)
		}
	}

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

	// use tiStore's currentVersion method to get the ts from tso
	addrs, err := flags.ParseHostPortAddr(s.cfg.KafkaAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	kb, err := createKafkaBinlogger(cid, s.node.ID(), addrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	find := false
	clusterDir := path.Join(s.dataDir, "clusters")
	names, err := file.ReadDir(clusterDir)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, n := range names {
		if cid == n {
			find = true
			break
		}
	}

	var (
		fb        Binlogger
		binlogDir = path.Join(clusterDir, cid)
	)
	if find {
		fb, err = OpenBinlogger(binlogDir)
	} else {
		fb, err = CreateBinlogger(binlogDir)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	cp, err := newCheckPoint(path.Join(binlogDir, "checkpoint"))
	if err != nil {
		return nil, errors.Trace(err)
	}

	s.dispatcher[cid] = newProxy(fb, kb, cp, s.cfg.enableProxySwitch)
	return s.dispatcher[cid], nil
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

		if len(in.Payload) > 100*1024*1024 {
			binlogSizeHistogram.WithLabelValues(s.node.ID()).Observe(float64(len(in.Payload)))
		}
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
		binlogs, err := binlogger.ReadFrom(pos, 1000)
		if err != nil {
			return errors.Trace(err)
		}

		for _, bl := range binlogs {
			pos = bl.Pos
			pos.Offset += int64(len(bl.Payload) + 16)
			resp := &binlog.PullBinlogResp{Entity: bl}
			if err = stream.Send(resp); err != nil {
				log.Errorf("gRPC: pullBinlogs send stream error, %s", errors.ErrorStack(err))
				return errors.Trace(err)
			}
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
			log.Errorf("unregister pump while pump fails to notify drainer error %v", errors.ErrorStack(err))
		}
		return errors.Annotate(err, "fail to notify all living drainer")
	}

	// start heartbeat loop
	errc := s.node.Heartbeat(s.ctx)
	go func() {
		for err := range errc {
			log.Errorf("send heartbeat error %v", err)
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
	http.HandleFunc("/drainers", s.AllDrainers)
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
		StartTs:  ts,
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
	// so we can use only one needGenBinlog flag
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

			log.Infof("generate fake binlog successfully")
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
			b.GC(s.gc, binlog.Pos{})
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

// AllDrainers exposes drainers' status to HTTP handler.
func (s *Server) AllDrainers(w http.ResponseWriter, r *http.Request) {
	node, ok := s.node.(*pumpNode)
	if !ok {
		json.NewEncoder(w).Encode("")
	}

	pumps, err := node.EtcdRegistry.Nodes(s.ctx, "cisterns")
	if err != nil {
		log.Errorf("get pumps error %v", err)
	}

	json.NewEncoder(w).Encode(pumps)
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
	binlogPos := make(map[string]*LatestPos)
	for _, st := range status {
		binlogPos[st.NodeID] = &LatestPos{
			FilePos:  st.LatestFilePos,
			KafkaPos: st.LatestKafkaPos,
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

	ts := int64(composeTS(physical, logical))
	// update latestTS by the way
	latestTS = ts

	return ts, nil
}

// Close gracefully releases resource of pump server
func (s *Server) Close() {
	for _, bl := range s.dispatcher {
		if err := bl.Close(); err != nil {
			log.Errorf("close binlogger error %v", err)
		}
	}

	// update latest for offline ts in unregister process
	if _, err := s.getTSO(); err != nil {
		log.Errorf("get tso in close error %v", errors.ErrorStack(err))
	}

	// unregister this node
	if err := s.node.Unregister(s.ctx); err != nil {
		log.Errorf("unregister pump error %v", errors.ErrorStack(err))
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
