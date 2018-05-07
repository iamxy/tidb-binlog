package pump

import (
	"time"

	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"golang.org/x/net/context"
)

var (
	rpcCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "rpc_counter",
			Help:      "RPC counter for every rpc related operations.",
		}, []string{"method", "label"})

	rpcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "rpc_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of rpc queries.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"method", "label"})

	binlogSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "binlog_size",
			Help:      "binlog size",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"label"})

	lossBinlogCacheCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "binlog_count",
			Help:      "Total loss binlog count",
		})

	writeBinlogHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "write_binlog_duration_time",
			Help:      "Bucketed histogram of write time (s) of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"label"})
)

func init() {
	prometheus.MustRegister(rpcCounter)
	prometheus.MustRegister(rpcHistogram)
	prometheus.MustRegister(binlogSizeHistogram)
	prometheus.MustRegister(lossBinlogCacheCounter)
	prometheus.MustRegister(writeBinlogHistogram)
}

type metricClient struct {
	addr     string
	interval int
}

// Start run a loop of pushing metrics to Prometheus Pushgateway.
func (mc *metricClient) Start(ctx context.Context, pumpID string) {
	log.Debugf("start prometheus metrics client, addr=%s, internal=%ds", mc.addr, mc.interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(mc.interval) * time.Second):
			err := push.AddFromGatherer(
				"binlog",
				map[string]string{"instance": pumpID},
				mc.addr,
				prometheus.DefaultGatherer,
			)
			if err != nil {
				log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
			}
		}
	}
}
