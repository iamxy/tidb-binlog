package arbiter

import (
	"context"
	"database/sql"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb-tools/tidb-binlog/driver/reader"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// Server is the server to load data to mysql
type Server struct {
	cfg  *Config
	port int

	load *loader.Loader

	checkpoint  *Checkpoint
	kafkaReader *reader.Reader
	downDB      *sql.DB

	// all txn commitTS <= finishTS has loaded to downstream
	finishTS int64

	metricsCancel context.CancelFunc
	metrics       *metricClient

	closed bool
	mu     sync.Mutex
}

// NewServer creates a Server
func NewServer(cfg *Config) (srv *Server, err error) {
	srv = new(Server)
	srv.cfg = cfg

	_, port, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return nil, errors.Annotatef(err, "wrong ListenAddr: %s", cfg.ListenAddr)
	}

	srv.port, err = strconv.Atoi(port)
	if err != nil {
		return nil, errors.Annotatef(err, "ListenAddr: %s", cfg.ListenAddr)
	}

	up := cfg.Up
	down := cfg.Down

	srv.downDB, err = loader.CreateDB(down.User, down.Password, down.Host, down.Port)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// set checkpoint
	srv.checkpoint, err = NewCheckpoint(srv.downDB, up.Topic)
	if err != nil {
		return nil, errors.Trace(err)
	}

	srv.finishTS = up.InitialCommitTS

	ts, status, err := srv.checkpoint.Load()
	if err != nil {
		if errors.IsNotFound(err) {
			err = nil
		} else {
			return nil, errors.Trace(err)
		}
	} else {
		srv.finishTS = ts
	}

	// set reader to read binlog from kafka
	readerCfg := &reader.Config{
		KafkaAddr: strings.Split(up.KafkaAddrs, ","),
		CommitTS:  srv.finishTS,
		Topic:     up.Topic,
	}

	log.Infof("use kafka binlog reader cfg: %+v", readerCfg)

	srv.kafkaReader, err = reader.NewReader(readerCfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// set loader
	srv.load, err = loader.NewLoader(srv.downDB,
		loader.WorkerCount(cfg.Down.WorkerCount),
		loader.BatchSize(cfg.Down.BatchSize),
		loader.Metrics(&loader.MetricsGroup{
			EventCounterVec:   eventCounter,
			QueryHistogramVec: queryHistogramVec,
		}))
	if err != nil {
		return nil, errors.Trace(err)
	}

	// set safe mode in first 5 min if abnormal quit last time
	if status == StatusRunning {
		log.Info("set safe mode to be true")
		srv.load.SetSafeMode(true)
		go func() {
			time.Sleep(time.Minute * 5)
			srv.load.SetSafeMode(false)
			log.Info("set safe mode to be false")
		}()
	}

	// set metrics
	if cfg.Metrics.Addr != "" && cfg.Metrics.Interval != 0 {
		srv.metrics = &metricClient{
			addr:     cfg.Metrics.Addr,
			interval: cfg.Metrics.Interval,
		}
	}

	return
}

// Close closes the Server
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.kafkaReader.Close()

	s.closed = true
	return nil
}

// Run runs the Server, will quit once encounter error or Server is closed
func (s *Server) Run() error {
	defer s.downDB.Close()

	// push metrics if need
	if s.metrics != nil {
		var ctx context.Context
		ctx, s.metricsCancel = context.WithCancel(context.Background())
		go s.metrics.Start(ctx, s.port)

		defer s.metricsCancel()
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		saveTick := time.NewTicker(time.Second)
		defer saveTick.Stop()

		for {
			select {
			case txn, ok := <-s.load.Successes():
				if !ok {
					log.Info("load successes channel closed")
					return
				}
				msg := txn.Metadata.(*reader.Message)
				log.Debugf("success binlog ts: %d at offset: %d", msg.Binlog.CommitTs, msg.Offset)
				s.finishTS = msg.Binlog.CommitTs

				ms := time.Now().UnixNano()/1000000 - oracle.ExtractPhysical(uint64(s.finishTS))
				txnLatencySecondsHistogram.Observe(float64(ms) / 1000.0)

			case <-saveTick.C:
				// log.Debug("save checkpoint ", s.finishTS)
				err := s.checkpoint.Save(s.finishTS, StatusRunning)
				if err != nil {
					log.Error(err)
					continue
				}

				checkpointTSOGauge.Set(float64(oracle.ExtractPhysical(uint64(s.finishTS))))
			}
		}
	}()

	var syncErr error
	syncCtx, syncCancel := context.WithCancel(context.Background())
	defer syncCancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer s.load.Close()
		syncErr = syncBinlogs(syncCtx, s.kafkaReader.Messages(), s.load)
	}()

	err := s.load.Run()
	if err != nil {
		syncCancel()
		s.Close()
	}

	wg.Wait()

	if err != nil {
		return errors.Trace(err)
	}

	if syncErr != nil {
		return errors.Trace(syncErr)
	}

	err = s.checkpoint.Save(s.finishTS, StatusNormal)
	if err != nil {
		return errors.Trace(err)
	}

	checkpointTSOGauge.Set(float64(oracle.ExtractPhysical(uint64(s.finishTS))))

	return nil
}

func syncBinlogs(ctx context.Context, source <-chan *reader.Message, ld *loader.Loader) (err error) {
	dest := ld.Input()
	defer ld.Close()
	for msg := range source {
		log.Debug("recv msg from kafka reader, ts: %v, offset: %v", msg.Binlog.CommitTs, msg.Offset)
		txn, err := loader.SlaveBinlogToTxn(msg.Binlog)
		if err != nil {
			log.Error("transfer binlog failed, program will stop handling data from loader, err: %s", err.Error())
			return err
		}
		txn.Metadata = msg
		dest <- txn
		// avoid block when no process is handling ld.input
		select {
		case dest <- txn:
		case <-ctx.Done():
			return nil
		}

		queueSizeGauge.WithLabelValues("kafka_reader").Set(float64(len(source)))
		queueSizeGauge.WithLabelValues("loader_input").Set(float64(len(dest)))
	}
	return nil
}
