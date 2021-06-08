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

package storage

import (
	"context"
	"os"
	"runtime"
	"sync/atomic"
	"testing"

	pb "github.com/pingcap/tipb/go-binlog"
)

func BenchmarkWriteSync128B(b *testing.B) {
	benchmarkWrite(b, 128, 100, true)
}

func BenchmarkWriteSync1K(b *testing.B) {
	benchmarkWrite(b, 1024, 100, true)
}

func BenchmarkWriteSync10K(b *testing.B) {
	benchmarkWrite(b, 10*1024, 100, true)
}

func BenchmarkWriteSync100K(b *testing.B) {
	benchmarkWrite(b, 100*1024, 100, true)
}

func BenchmarkWriteNoSync128B(b *testing.B) {
	benchmarkWrite(b, 128, 100, false)
}

func BenchmarkWriteNoSync1K(b *testing.B) {
	benchmarkWrite(b, 1024, 100, false)
}

func BenchmarkWriteNoSync10K(b *testing.B) {
	benchmarkWrite(b, 10*1024, 100, false)
}

func BenchmarkWriteNoSync100K(b *testing.B) {
	benchmarkWrite(b, 100*1024, 100, false)
}

func BenchmarkPull128B(b *testing.B) {
	benchmarkPull(b, 128, b.N)
}

func BenchmarkPull1K(b *testing.B) {
	benchmarkPull(b, 1024, b.N)
}

func BenchmarkPull10K(b *testing.B) {
	benchmarkPull(b, 10*1024, b.N)
}

func BenchmarkPull100K(b *testing.B) {
	benchmarkPull(b, 100*1024, b.N)
}

func benchmarkPull(b *testing.B, prewriteValueSize int, binlogNum int) {
	append := newAppend(b)
	defer os.RemoveAll(append.dir)

	populateBinlog(b, append, prewriteValueSize, binlogNum)

	runtime.GC()
	b.ResetTimer()

	pulller, errs := append.PullCommitBinlog(context.Background(), 0)

	cnt := 0
	for b := range pulller {
		_ = b
		cnt++
		if cnt == binlogNum {
			break
		}
	}

	if cnt != binlogNum {
		b.Fatalf("only get %d binlog, should has %d", cnt, binlogNum)
	}

	b.StopTimer()
	append.Close()

	// just count the prewriteValueSize
	b.SetBytes(int64(prewriteValueSize))
	b.ReportAllocs()
	if len(errs) > 0 {
		b.Fatalf("pull binlog got some errors")
	}
}

func benchmarkWrite(b *testing.B, prewriteValueSize int, parallelism int, sync bool) {
	b.SetParallelism(parallelism)

	prewriteValue := make([]byte, prewriteValueSize)

	append := newAppendWithOptions(b, DefaultOptions().WithSync(sync))
	defer os.RemoveAll(append.dir)

	b.ResetTimer()

	var ts int64
	getTS := func() int64 {
		return atomic.AddInt64(&ts, 1)
	}
	b.RunParallel(func(pbench *testing.PB) {
		for pbench.Next() {
			// write P binlog
			binlog := new(pb.Binlog)
			binlog.Tp = pb.BinlogType_Prewrite
			startTS := getTS()
			binlog.StartTs = startTS
			binlog.PrewriteValue = prewriteValue

			err := append.WriteBinlog(binlog)
			if err != nil {
				b.Fatal(err)
			}

			// write C binlog
			binlog = new(pb.Binlog)
			binlog.Tp = pb.BinlogType_Commit
			binlog.StartTs = startTS
			binlog.CommitTs = getTS()
			err = append.WriteBinlog(binlog)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	append.Close()

	b.StopTimer()

	// just count the prewriteValueSize
	b.SetBytes(int64(prewriteValueSize))
	b.ReportAllocs()
}
