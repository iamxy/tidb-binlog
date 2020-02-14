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

package sync

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/util"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	"go.uber.org/zap"
)

var maxWaitTimeToSendMSG = time.Second * 30
var stallWriteSize = 90 * 1024 * 1024

var _ Syncer = &KafkaSyncer{}

// PartitionMode Kafka partition mode
type PartitionMode byte

const (
	// PartitionFixed choose fixed partition 0
	PartitionFixed PartitionMode = iota + 1
	// PartitionBySchema choose partition by schema
	PartitionBySchema
	// PartitionByTable choose partition by table
	PartitionByTable
)

// KafkaSyncer sync data to kafka
type KafkaSyncer struct {
	addr     []string
	producer sarama.AsyncProducer
	topic    string

	toBeAckCommitTSMu      sync.Mutex
	toBeAckCommitTS        map[int64]int
	toBeAckTotalSize       int
	resumeProduce          chan struct{}
	resumeProduceCloseOnce sync.Once

	lastSuccessTime time.Time

	shutdown chan struct{}
	*baseSyncer

	partitionMode PartitionMode
}

func partitionMode(mode string) PartitionMode {
	switch strings.ToLower(mode) {
	case "schema":
		return PartitionBySchema
	case "table":
		return PartitionByTable
	default:
		return PartitionFixed
	}
}

// newAsyncProducer will only be changed in unit test for mock
var newAsyncProducer = sarama.NewAsyncProducer

// NewKafka returns a instance of KafkaSyncer
func NewKafka(cfg *DBConfig, tableInfoGetter translator.TableInfoGetter) (*KafkaSyncer, error) {
	var topic string
	if len(cfg.TopicName) == 0 {
		clusterIDStr := strconv.FormatUint(cfg.ClusterID, 10)
		topic = clusterIDStr + "_obinlog"
	} else {
		topic = cfg.TopicName
	}

	executor := &KafkaSyncer{
		addr:            strings.Split(cfg.KafkaAddrs, ","),
		topic:           topic,
		toBeAckCommitTS: make(map[int64]int),
		shutdown:        make(chan struct{}),
		baseSyncer:      newBaseSyncer(tableInfoGetter),
		partitionMode:   partitionMode(cfg.KafkaPartitionMode),
	}

	config, err := util.NewSaramaConfig(cfg.KafkaVersion, "kafka.")
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.Producer.Flush.MaxMessages = cfg.KafkaMaxMessages

	// maintain minimal set that has been necessary so far
	// this also avoid take too much time in NewAsyncProducer if kafka is down
	// because it will fetch metadata right away if setting Full = true, and we set
	// config.Metadata.Retry.Max to be a pretty hight value
	// maybe when this issue if fixed: https://github.com/Shopify/sarama/issues/1145
	// we can avoid setting Metadata.Retry to be a pretty hight value too
	config.Metadata.Full = false
	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond

	if executor.partitionMode == PartitionFixed {
		config.Producer.Partitioner = sarama.NewManualPartitioner
	} else {
		config.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	}
	config.Producer.MaxMessageBytes = 1 << 30
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	// just set to a pretty high retry num, so we will not drop some msg and
	// continue to push the laster msg, we will quit if we find msg fail to push after `maxWaitTimeToSendMSG`
	// aim to avoid not continuity msg sent to kafka.. see: https://github.com/Shopify/sarama/issues/838
	config.Producer.Retry.Max = 10000
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	executor.producer, err = newAsyncProducer(executor.addr, config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go executor.run()

	return executor, nil
}

// Sync implements Syncer interface
func (p *KafkaSyncer) Sync(item *Item) error {
	slaveBinlog, err := translator.TiBinlogToSlaveBinlog(p.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}
	switch p.partitionMode {
	case PartitionFixed:
		return errors.Trace(p.saveBinlog(slaveBinlog, item, nil))
	case PartitionBySchema:
		if slaveBinlog.Type == obinlog.BinlogType_DML {
			return errors.Trace(p.syncDMLPartitionBySchema(slaveBinlog, item))
		}
		partitionKey := sarama.StringEncoder(*slaveBinlog.DdlData.SchemaName)
		return errors.Trace(p.saveBinlog(slaveBinlog, item, partitionKey))
	case PartitionByTable:
		if slaveBinlog.Type == obinlog.BinlogType_DML {
			return errors.Trace(p.syncDMLPartitionByTable(slaveBinlog, item))
		}
		partitionKey := sarama.StringEncoder(*slaveBinlog.DdlData.SchemaName + "." + *slaveBinlog.DdlData.TableName)
		return errors.Trace(p.saveBinlog(slaveBinlog, item, partitionKey))
	}
	return errors.Errorf("Not supported partition mode: %v", p.partitionMode)
}

func (p *KafkaSyncer) syncDMLPartitionBySchema(binlog *obinlog.Binlog, item *Item) error {
	tables := binlog.DmlData.Tables
	if len(tables) == 1 {
		return errors.Trace(p.saveBinlog(binlog, item, sarama.StringEncoder(*tables[0].SchemaName)))
	}

	tablesBySchema := make(map[string][]*obinlog.Table)
	for _, table := range tables {
		schemaName := *table.SchemaName
		tablesBySchema[schemaName] = append(tablesBySchema[schemaName], table)
	}

	for schema, tables := range tablesBySchema {
		binlog.DmlData.Tables = tables
		if err := p.saveBinlog(binlog, item, sarama.StringEncoder(schema)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (p *KafkaSyncer) syncDMLPartitionByTable(binlog *obinlog.Binlog, item *Item) error {
	tables := binlog.DmlData.Tables
	if len(tables) == 1 {
		table := tables[0]
		return errors.Trace(p.saveBinlog(binlog, item, sarama.StringEncoder(*table.SchemaName+"."+*table.TableName)))
	}

	binlog.DmlData.Tables = make([]*obinlog.Table, 1)
	for _, table := range tables {
		binlog.DmlData.Tables[0] = table
		if err := p.saveBinlog(binlog, item, sarama.StringEncoder(*table.SchemaName+"."+*table.TableName)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Close implements Syncer interface
func (p *KafkaSyncer) Close() error {
	close(p.shutdown)

	err := <-p.Error()

	return err
}

func (p *KafkaSyncer) saveBinlog(binlog *obinlog.Binlog, item *Item, key sarama.Encoder) error {
	// log.Debug("save binlog: ", binlog.String())
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	msg := &sarama.ProducerMessage{Topic: p.topic, Key: key, Value: sarama.ByteEncoder(data), Partition: 0}
	msg.Metadata = item

	waitResume := false

	p.toBeAckCommitTSMu.Lock()
	if len(p.toBeAckCommitTS) == 0 {
		p.lastSuccessTime = time.Now()
	}
	p.toBeAckCommitTS[binlog.CommitTs] = len(data)
	p.toBeAckTotalSize += len(data)
	if p.toBeAckTotalSize >= stallWriteSize && len(p.toBeAckCommitTS) > 1 {
		p.resumeProduce = make(chan struct{})
		p.resumeProduceCloseOnce = sync.Once{}
		waitResume = true
	}
	p.toBeAckCommitTSMu.Unlock()

	if waitResume {
		select {
		case <-p.resumeProduce:
		case <-p.errCh:
			return errors.Trace(p.err)
		}
	}

	select {
	case p.producer.Input() <- msg:
		return nil
	case <-p.errCh:
		return errors.Trace(p.err)
	}
}

func (p *KafkaSyncer) run() {
	var wg sync.WaitGroup

	// handle successes from producer
	wg.Add(1)
	go func() {
		defer wg.Done()

		for msg := range p.producer.Successes() {
			item := msg.Metadata.(*Item)
			commitTs := item.Binlog.GetCommitTs()
			log.Debug("get success msg from producer", zap.Int64("ts", commitTs))

			p.toBeAckCommitTSMu.Lock()
			p.lastSuccessTime = time.Now()
			size := p.toBeAckCommitTS[commitTs]
			p.toBeAckTotalSize -= size
			if p.toBeAckTotalSize < stallWriteSize && p.resumeProduce != nil {
				p.resumeProduceCloseOnce.Do(func() {
					close(p.resumeProduce)
				})
			}
			delete(p.toBeAckCommitTS, commitTs)
			p.toBeAckCommitTSMu.Unlock()

			p.success <- item
		}
		close(p.success)
	}()

	// handle errors from producer
	wg.Add(1)
	go func() {
		defer wg.Done()

		for err := range p.producer.Errors() {
			log.Fatal("fail to produce message to kafka, please check the state of kafka server", zap.Error(err))
		}
	}()

	checkTick := time.NewTicker(time.Second)
	defer checkTick.Stop()

	for {
		select {
		case <-checkTick.C:
			p.toBeAckCommitTSMu.Lock()
			if len(p.toBeAckCommitTS) > 0 && time.Since(p.lastSuccessTime) > maxWaitTimeToSendMSG {
				log.Debug("fail to push to kafka")
				err := errors.Errorf("fail to push msg to kafka after %v, check if kafka is up and working", maxWaitTimeToSendMSG)
				p.setErr(err)
				p.toBeAckCommitTSMu.Unlock()
				return
			}
			p.toBeAckCommitTSMu.Unlock()
		case <-p.shutdown:
			err := p.producer.Close()
			p.setErr(err)

			wg.Wait()
			return
		}
	}
}
