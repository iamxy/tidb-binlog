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

package drainer

import (
	"crypto/tls"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pb "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	binlogChanSize = 0
)

// Pump holds the connection to a pump node, and keeps the savepoint of binlog last read
type Pump struct {
	nodeID    string
	addr      string
	tlsConfig *tls.Config
	clusterID uint64
	// the latest binlog ts that pump had handled
	latestTS int64

	isClosed int32

	isPaused int32

	errCh chan error

	pullCli  pb.Pump_PullBinlogsClient
	grpcConn *grpc.ClientConn
	logger   *zap.Logger
}

// NewPump returns an instance of Pump
func NewPump(nodeID, addr string, tlsConfig *tls.Config, clusterID uint64, startTs int64, errCh chan error) *Pump {
	nodeID = pump.FormatNodeID(nodeID)
	return &Pump{
		nodeID:    nodeID,
		addr:      addr,
		tlsConfig: tlsConfig,
		clusterID: clusterID,
		latestTS:  startTs,
		errCh:     errCh,
		logger:    log.L().With(zap.String("id", nodeID)),
	}
}

// Close sets isClose to 1, and pull binlog will be exit.
func (p *Pump) Close() {
	p.logger.Info("pump is closing")
	atomic.StoreInt32(&p.isClosed, 1)
}

// Pause sets isPaused to 1, and stop pull binlog from pump. This function is reentrant.
func (p *Pump) Pause() {
	// use CompareAndSwapInt32 to avoid redundant log
	if atomic.CompareAndSwapInt32(&p.isPaused, 0, 1) {
		p.logger.Info("pump pause pull binlog")
	}
}

// Continue sets isPaused to 0, and continue pull binlog from pump. This function is reentrant.
func (p *Pump) Continue(pctx context.Context) {
	// use CompareAndSwapInt32 to avoid redundant log
	if atomic.CompareAndSwapInt32(&p.isPaused, 1, 0) {
		p.logger.Info("pump continue pull binlog")
	}
}

// PullBinlog returns the chan to get item from pump
func (p *Pump) PullBinlog(pctx context.Context, last int64) chan MergeItem {
	// initial log
	pLog := util.NewLog()
	labelReceive := "receive binlog"
	labelCreateConn := "create conn"
	labelPaused := "pump paused"
	pLog.Add(labelReceive, 10*time.Second)
	pLog.Add(labelCreateConn, 10*time.Second)
	pLog.Add(labelPaused, 30*time.Second)

	ret := make(chan MergeItem, binlogChanSize)

	go func() {
		p.logger.Debug("pump start PullBinlog")

		defer func() {
			close(ret)
			if p.grpcConn != nil {
				p.grpcConn.Close()
			}
			p.logger.Debug("pump stop PullBinlog")
		}()

		needReCreateConn := false
		for {
			if atomic.LoadInt32(&p.isClosed) == 1 {
				return
			}

			if atomic.LoadInt32(&p.isPaused) == 1 {
				// this pump is paused, wait until it can pull binlog again
				pLog.Print(labelPaused, func() {
					p.logger.Debug("pump is paused")
				})

				time.Sleep(time.Second)
				continue
			}

			if p.grpcConn == nil || needReCreateConn {
				p.logger.Info("pump create pull binlogs client")
				if err := p.createPullBinlogsClient(pctx, last); err != nil {
					p.logger.Error("pump create pull binlogs client failed", zap.Error(err))
					time.Sleep(time.Second)
					continue
				}

				needReCreateConn = false
			}

			recvBeginTime := time.Now()
			resp, err := p.pullCli.Recv()
			if err != nil {
				if status.Code(err) != codes.Canceled {
					pLog.Print(labelReceive, func() {
						p.logger.Error("pump receive binlog failed", zap.Error(err))
					})
				}

				needReCreateConn = true

				time.Sleep(time.Second)
				// TODO: add metric here
				continue
			}
			binlogRecvDurationHistogram.WithLabelValues(p.nodeID).Observe(time.Since(recvBeginTime).Seconds())

			payloadSize := len(resp.Entity.Payload)
			readBinlogSizeHistogram.WithLabelValues(p.nodeID).Observe(float64(payloadSize))
			if len(resp.Entity.Payload) >= 10*1024*1024 {
				log.Info("receive big size binlog", zap.String("size", humanize.Bytes(uint64(payloadSize))))
			}

			binlog := new(pb.Binlog)
			err = binlog.Unmarshal(resp.Entity.Payload)
			if err != nil {
				errorCount.WithLabelValues("unmarshal_binlog").Add(1)
				p.logger.Error("pump unmarshal binlog failed", zap.Error(err))
				p.reportErr(pctx, err)
				return
			}

			millisecond := time.Now().UnixNano()/1000000 - oracle.ExtractPhysical(uint64(binlog.CommitTs))
			binlogReachDurationHistogram.WithLabelValues(p.nodeID).Observe(float64(millisecond) / 1000.0)

			item := newBinlogItem(binlog, p.nodeID)
			select {
			case ret <- item:
				if binlog.CommitTs > last {
					last = binlog.CommitTs
					p.latestTS = binlog.CommitTs
				} else {
					p.logger.Error("pump receive unsort binlog")
				}
			case <-pctx.Done():
				return
			}
		}
	}()

	return ret
}

func (p *Pump) createPullBinlogsClient(ctx context.Context, last int64) error {
	if p.grpcConn != nil {
		p.grpcConn.Close()
	}

	callOpts := []grpc.CallOption{grpc.MaxCallRecvMsgSize(maxMsgSize)}

	if compressor, ok := getCompressorName(ctx); ok {
		p.logger.Info("pump grpc compression enabled")
		callOpts = append(callOpts, grpc.UseCompressor(compressor))
	}

	dialOpts := []grpc.DialOption{grpc.WithDefaultCallOptions(callOpts...)}
	if p.tlsConfig != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(p.tlsConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(p.addr, dialOpts...)
	if err != nil {
		p.logger.Error("pump create grpc dial failed", zap.Error(err))
		p.pullCli = nil
		p.grpcConn = nil
		return errors.Trace(err)
	}

	cli := pb.NewPumpClient(conn)

	in := &pb.PullBinlogReq{
		ClusterID: p.clusterID,
		StartFrom: pb.Pos{Offset: last},
	}
	pullCli, err := cli.PullBinlogs(ctx, in)
	if err != nil {
		p.logger.Error("pump create PullBinlogs client failed", zap.Error(err))
		conn.Close()
		p.pullCli = nil
		p.grpcConn = nil
		return errors.Trace(err)
	}

	p.pullCli = pullCli
	p.grpcConn = conn

	return nil
}

func (p *Pump) reportErr(ctx context.Context, err error) {
	select {
	case <-ctx.Done():
	case p.errCh <- err:
	}
}

func getCompressorName(ctx context.Context) (string, bool) {
	if compressor, ok := ctx.Value(drainerKeyType("compressor")).(string); ok {
		compressor = strings.TrimSpace(compressor)
		if len(compressor) != 0 {
			return compressor, true
		}
	}
	return "", false
}
