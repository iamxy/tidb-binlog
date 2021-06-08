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
	"github.com/pingcap/tidb-binlog/drainer/sync"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pumpPositionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "pump_position",
			Help:      "position for each pump.",
		}, []string{"nodeID"})

	ddlJobsCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "ddl_jobs_total",
			Help:      "Total ddl jobs count been stored.",
		})

	errorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "error_count",
			Help:      "Total count of error type",
		}, []string{"type"})

	disorderBinlogCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "disorder_binlog_count",
			Help:      "Total count of binlog which is disorder.",
		})

	eventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "event",
			Help:      "the count of sql event(dml, ddl).",
		}, []string{"type"})

	checkpointTSOGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "checkpoint_tso",
			Help:      "save checkpoint tso of drainer.",
		})

	checkpointDelayHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "checkpoint_delay_seconds",
			Help:      "How much the downstream checkpoint lag behind",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 22),
		})

	executeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "execute_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a execute to sync data to downstream.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		})

	queryHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "query_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a query to sync data to downstream.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"type"})

	binlogReachDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "binlog_reach_duration_time",
			Help:      "Bucketed histogram of how long the binlog take to reach drainer since it's committed",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 30),
		}, []string{"nodeID"})

	binlogRecvDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "binlog_recv_duration_time",
			Help:      "Bucketed histogram of how long the binlog take to receive",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 30),
		}, []string{"nodeID"})

	readBinlogSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "read_binlog_size",
			Help:      "Bucketed histogram of size of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 30),
		}, []string{"nodeID"})

	queueSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "queue_size",
			Help:      "the size of queue",
		}, []string{"name"})
)

var registry = prometheus.NewRegistry()

func init() {
	sync.QueueSizeGauge = queueSizeGauge

	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(pumpPositionGauge)
	registry.MustRegister(ddlJobsCounter)
	registry.MustRegister(errorCount)
	registry.MustRegister(checkpointTSOGauge)
	registry.MustRegister(checkpointDelayHistogram)
	registry.MustRegister(eventCounter)
	registry.MustRegister(executeHistogram)
	registry.MustRegister(binlogReachDurationHistogram)
	registry.MustRegister(binlogRecvDurationHistogram)
	registry.MustRegister(readBinlogSizeHistogram)
	registry.MustRegister(queryHistogramVec)
	registry.MustRegister(queueSizeGauge)

	// for pb using it
	bf.InitMetircs(registry)
}
