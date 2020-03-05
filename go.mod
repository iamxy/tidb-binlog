module github.com/pingcap/tidb-binlog

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.0
	github.com/Shopify/sarama v1.24.1
	github.com/dustin/go-humanize v1.0.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.2.0
	github.com/golang/protobuf v1.3.3
	github.com/google/gofuzz v1.0.0
	github.com/gorilla/mux v1.6.2
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/kvproto v0.0.0-20200221125103-35b65c96516e
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/parser v0.0.0-20200218113622-517beb2e39c2
	github.com/pingcap/pd v1.1.0-beta.0.20200106144140-f5a7aa985497
	github.com/pingcap/tidb v1.1.0-beta.0.20200303051834-efe3b8f56baa
	github.com/pingcap/tidb-tools v4.0.0-beta.1.0.20200305062924-4acaa3834b43+incompatible
	github.com/pingcap/tipb v0.0.0-20200212061130-c4d518eb1d60
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a
	github.com/samuel/go-zookeeper v0.0.0-20170815201139-e6b59f6144be
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/sirupsen/logrus v1.4.1 // indirect
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20190625010220-02440ea7a285
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/unrolled/render v0.0.0-20180914162206-b9786414de4d
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.13.0
	golang.org/x/net v0.0.0-20191002035440-2ec189313ef0
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	golang.org/x/sys v0.0.0-20191210023423-ac6580df4449
	google.golang.org/grpc v1.25.1
)

go 1.13
