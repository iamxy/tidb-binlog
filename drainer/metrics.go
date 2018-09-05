package drainer

import (
	"time"

	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"golang.org/x/net/context"
)

var (
	windowGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "window",
			Help:      "DepositWindow boundary.",
		}, []string{"marker"})

	offsetGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "offset",
			Help:      "offset for each pump.",
		}, []string{"nodeID"})

	findMatchedBinlogHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "find_matched_binlog_duration_time",
			Help:      "Bucketed histogram of find a matched binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"nodeID"})

	publishBinlogHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "publish_binlog_duration_time",
			Help:      "Bucketed histogram of publish a binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"nodeID"})

	publishBinlogCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "publish_binlog_count",
			Help:      "Total binlog count been stored.",
		}, []string{"nodeID"})

	ddlJobsCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "ddl_jobs_total",
			Help:      "Total ddl jobs count been stored.",
		})

	tikvQueryCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "query_tikv_count",
			Help:      "Total count that queried tikv.",
		})

	errorBinlogCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "error_binlog_count",
			Help:      "Total count of binlog that store too late.",
		})

	eventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "event",
			Help:      "the count of sql event(dml, ddl).",
		}, []string{"type"})

	positionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "position",
			Help:      "save position of drainer.",
		})

	txnHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		})

	readBinlogHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "read_binlog_duration_time",
			Help:      "Bucketed histogram of read time (s) of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"nodeID"})

	readBinlogSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "read_binlog_size",
			Help:      "Bucketed histogram of size of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		}, []string{"nodeID"})
)

var registerer = prometheus.NewRegistry()

func init() {
	registerer.MustRegister(windowGauge)
	registerer.MustRegister(offsetGauge)
	registerer.MustRegister(publishBinlogCounter)
	registerer.MustRegister(ddlJobsCounter)
	registerer.MustRegister(tikvQueryCount)
	registerer.MustRegister(errorBinlogCount)
	registerer.MustRegister(positionGauge)
	registerer.MustRegister(eventCounter)
	registerer.MustRegister(txnHistogram)
	registerer.MustRegister(readBinlogHistogram)
	registerer.MustRegister(readBinlogSizeHistogram)
	registerer.MustRegister(publishBinlogHistogram)
	registerer.MustRegister(findMatchedBinlogHistogram)
}

type metricClient struct {
	addr     string
	interval int
}

// Start run a loop of pushing metrics to Prometheus Pushgateway.
func (mc *metricClient) Start(ctx context.Context, drainerID string) {
	log.Debugf("start prometheus metrics client, addr=%s, internal=%ds", mc.addr, mc.interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(mc.interval) * time.Second):
			err := push.AddFromGatherer(
				"binlog",
				map[string]string{"instance": drainerID},
				mc.addr,
				registerer,
			)
			if err != nil {
				log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
			}
		}
	}
}
