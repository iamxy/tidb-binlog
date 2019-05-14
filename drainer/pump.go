package drainer

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	binlogChanSize = 10
)

// Pump holds the connection to a pump node, and keeps the savepoint of binlog last read
type Pump struct {
	nodeID    string
	addr      string
	clusterID uint64
	// the latest binlog ts that pump had handled
	latestTS int64

	isClosed int32

	isPaused int32

	errCh chan error

	pullCli  pb.Pump_PullBinlogsClient
	grpcConn *grpc.ClientConn
}

// NewPump returns an instance of Pump
func NewPump(nodeID, addr string, clusterID uint64, startTs int64, errCh chan error) *Pump {
	return &Pump{
		nodeID:    pump.FormatNodeID(nodeID),
		addr:      addr,
		clusterID: clusterID,
		latestTS:  startTs,
		errCh:     errCh,
	}
}

// Close sets isClose to 1, and pull binlog will be exit.
func (p *Pump) Close() {
	log.Infof("[pump %s] is closing", p.nodeID)
	atomic.StoreInt32(&p.isClosed, 1)
}

// Pause sets isPaused to 1, and stop pull binlog from pump. This function is reentrant.
func (p *Pump) Pause() {
	// use CompareAndSwapInt32 to avoid redundant log
	if atomic.CompareAndSwapInt32(&p.isPaused, 0, 1) {
		log.Infof("[pump %s] pause pull binlog", p.nodeID)
	}
}

// Continue sets isPaused to 0, and continue pull binlog from pump. This function is reentrant.
func (p *Pump) Continue(pctx context.Context) {
	// use CompareAndSwapInt32 to avoid redundant log
	if atomic.CompareAndSwapInt32(&p.isPaused, 1, 0) {
		log.Infof("[pump %s] continue pull binlog", p.nodeID)
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
		log.Debugf("[pump %s] start PullBinlog", p.nodeID)

		defer func() {
			close(ret)
			if p.grpcConn != nil {
				p.grpcConn.Close()
			}
			log.Debugf("[pump %s] stop PullBinlog", p.nodeID)
		}()

		needReCreateConn := false
		for {
			if atomic.LoadInt32(&p.isClosed) == 1 {
				return
			}

			if atomic.LoadInt32(&p.isPaused) == 1 {
				// this pump is paused, wait until it can pull binlog again
				pLog.Print(labelPaused, func() {
					log.Debugf("[pump %s] is paused", p.nodeID)
				})

				time.Sleep(time.Second)
				continue
			}

			if p.grpcConn == nil || needReCreateConn {
				log.Infof("[pump %s] create pull binlogs client", p.nodeID)
				err := p.createPullBinlogsClient(pctx, last)
				if err != nil {
					log.Errorf("[pump %s] create pull binlogs client error %v", p.nodeID, err)
					time.Sleep(time.Second)
					continue
				}

				needReCreateConn = false
			}

			resp, err := p.pullCli.Recv()
			if err != nil {
				if status.Code(err) != codes.Canceled {
					pLog.Print(labelReceive, func() {
						log.Errorf("[pump %s] receive binlog error %v", p.nodeID, err)
					})
				}

				needReCreateConn = true

				time.Sleep(time.Second)
				// TODO: add metric here
				continue
			}
			readBinlogSizeHistogram.WithLabelValues(p.nodeID).Observe(float64(len(resp.Entity.Payload)))

			binlog := new(pb.Binlog)
			err = binlog.Unmarshal(resp.Entity.Payload)
			if err != nil {
				errorCount.WithLabelValues("unmarshal_binlog").Add(1)
				log.Errorf("[pump %s] unmarshal binlog error: %v", p.nodeID, err)
				p.reportErr(pctx, err)
				return
			}

			millisecond := time.Now().UnixNano()/1000000 - oracle.ExtractPhysical(uint64(binlog.CommitTs))
			binlogReachDurationHistogram.WithLabelValues(p.nodeID).Observe(float64(millisecond) / 1000.0)

			item := &binlogItem{
				binlog: binlog,
				nodeID: p.nodeID,
			}
			select {
			case ret <- item:
				if binlog.CommitTs > last {
					last = binlog.CommitTs
					p.latestTS = binlog.CommitTs
				} else {
					log.Errorf("[pump %s] receive unsort binlog", p.nodeID)
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
		log.Infof("[pump %s] grpc compression enabled", p.nodeID)
		callOpts = append(callOpts, grpc.UseCompressor(compressor))
	}

	conn, err := grpc.Dial(p.addr, grpc.WithInsecure(), grpc.WithDefaultCallOptions(callOpts...))
	if err != nil {
		log.Errorf("[pump %s] create grpc dial error %v", p.nodeID, err)
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
		log.Errorf("[pump %s] create PullBinlogs client error %v", p.nodeID, err)
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
		return
	case p.errCh <- err:
		return
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
