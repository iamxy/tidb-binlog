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
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/soheilhy/cmux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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

	// node maintain the status of this pump and interact with etcd registry
	node Node

	tcpAddr  string
	unixAddr string
	gs       *grpc.Server
	ctx      context.Context
	cancel   context.CancelFunc
	gc       time.Duration
	metrics  *metricClient
}

// NewServer return a instance of pump server
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
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		dispatcher: make(map[string]Binlogger),
		dataDir:    cfg.DataDir,
		node:       n,
		tcpAddr:    cfg.ListenAddr,
		unixAddr:   cfg.Socket,
		gs:         grpc.NewServer(),
		ctx:        ctx,
		cancel:     cancel,
		metrics:    metrics,
		gc:         time.Duration(cfg.GC) * 24 * time.Hour,
	}, nil
}

// init scan the dataDir to find all clusterIDs, and for each to create binlogger,
// then add them to dispathcer map
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

// PullBinlogs implements the gRPC interface of pump server
func (s *Server) PullBinlogs(ctx context.Context, in *binlog.PullBinlogReq) (*binlog.PullBinlogResp, error) {
	var err error
	// beginTime := time.Now()
	// defer func() {
	// 	var label string
	// 	if err != nil {
	// 		label = "fail"
	// 	} else {
	// 		label = "succ"
	// 	}
	// 	rpcHistogram.WithLabelValues("PullBinlogs", label).Observe(time.Since(beginTime).Seconds())
	// 	rpcCounter.WithLabelValues("PullBinlogs", label).Add(1)
	// }()

	cid := fmt.Sprintf("%d", in.ClusterID)
	ret := &binlog.PullBinlogResp{}
	binlogger, err1 := s.getBinloggerToRead(cid)
	if err1 != nil {
		if errors.IsNotFound(err1) {
			// return an empty slice and a nil error
			ret.Entities = []binlog.Entity{}
			return ret, nil
		}
		ret.Errmsg = err1.Error()
		err = errors.Trace(err1)
		return ret, err
	}
	binlogs, err1 := binlogger.ReadFrom(in.StartFrom, in.Batch)
	if err1 != nil {
		ret.Errmsg = err1.Error()
		err = errors.Trace(err1)
		return ret, err
	}
	ret.Entities = binlogs
	return ret, nil
}

// Start runs Pump Server to serve the listening addr, and maintains heartbeat to Etcd
func (s *Server) Start() error {
	grpc.EnableTracing = false
	// register this node
	if err := s.node.Register(s.ctx); err != nil {
		return errors.Annotate(err, "fail to register node to etcd")
	}

	// start heartbeat
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
	go http.Serve(httpL, nil)

	return m.Serve()
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
	s.metrics.Start(s.ctx)
}

// Close gracefully releases resource of pump server
func (s *Server) Close() {
	// notify other goroutines to exit
	s.cancel()
	// stop the gRPC server
	s.gs.Stop()
}
