package storage

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	gcTSGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "gc_ts",
			Help:      "gc ts of storage",
		})

	doneGcTSGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "done_gc_ts",
			Help:      "the metadata and vlog after this gc ts has been collected",
		})

	deletedKv = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "deleted_kv_total",
			Help:      "deleted kv number",
		})

	storageSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "storage_size_bytes",
			Help:      "storage size info",
		}, []string{"type"})

	maxCommitTSGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "max_commit_ts",
			Help:      "max commit ts of storage",
		})

	tikvQueryCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "query_tikv_count",
			Help:      "Total count that queried tikv.",
		})

	errorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "error_count",
			Help:      "Total error count in storage",
		}, []string{"type"})

	writeBinlogSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "write_binlog_size",
			Help:      "write binlog size",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 25),
		}, []string{"type"})

	writeBinlogTimeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "write_binlog_duration_time",
			Help:      "Bucketed histogram of write time (s) of  binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 20),
		}, []string{"type"})
)

// InitMetircs register the metrics to registry
func InitMetircs(registry *prometheus.Registry) {
	registry.MustRegister(gcTSGauge)
	registry.MustRegister(doneGcTSGauge)
	registry.MustRegister(deletedKv)
	registry.MustRegister(maxCommitTSGauge)
	registry.MustRegister(tikvQueryCount)
	registry.MustRegister(errorCount)
	registry.MustRegister(writeBinlogSizeHistogram)
	registry.MustRegister(writeBinlogTimeHistogram)
	registry.MustRegister(storageSizeGauge)
}
