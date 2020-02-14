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
	"testing"

	"github.com/gogo/protobuf/proto"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	ti "github.com/pingcap/tipb/go-binlog"
)

var bytes = make([]byte, 5*(1<<10))
var table = &obinlog.Table{
	SchemaName: proto.String("test"),
	TableName:  proto.String("test"),
	ColumnInfo: []*obinlog.ColumnInfo{
		{Name: "id", MysqlType: "int"},
		{Name: "a1", MysqlType: "blob"},
	},
	Mutations: []*obinlog.TableMutation{
		{
			Type: obinlog.MutationType_Insert.Enum(),
			Row: &obinlog.Row{
				Columns: []*obinlog.Column{
					{
						Int64Value: proto.Int64(1),
					},
					{
						BytesValue: bytes,
					},
				},
			},
		},
	},
}

// with bytes = 5KB
// BenchmarkBinlogMarshal-4          100000            573941 ns/op
// means only 1742 op/second
func BenchmarkBinlogMarshal(b *testing.B) {
	binlog := &obinlog.Binlog{
		DmlData: &obinlog.DMLData{
			Tables: []*obinlog.Table{table},
		},
	}
	var s string
	for i := 0; i < b.N; i++ {
		s = binlog.String()
	}
	if len(s) == 0 {
		b.Fail()
	}
}

// with bytes = 5KB
// BenchmarkKafka-4         1000000             42384 ns/op
// means 23593 op/second
func BenchmarkKafka(b *testing.B) {
	cfg := &DBConfig{
		KafkaAddrs:   "127.0.0.1:9092",
		KafkaVersion: "0.8.2.0",
		ClusterID:    99900,
	}

	binlog := &obinlog.Binlog{
		DmlData: &obinlog.DMLData{
			Tables: []*obinlog.Table{table},
		},
	}

	item := &Item{Binlog: &ti.Binlog{}}

	syncer, err := NewKafka(cfg, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	// Just drain is, or may be block if the buffer is full
	go func() {
		for range syncer.Successes() {
		}
	}()

	for i := 0; i < b.N; i++ {
		err = syncer.saveBinlog(binlog, item, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}
