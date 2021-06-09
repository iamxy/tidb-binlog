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
	"crypto/tls"

	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb-binlog/pkg/security"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string          `toml:"host" json:"host"`
	User     string          `toml:"user" json:"user"`
	Password string          `toml:"password" json:"password"`
	Security security.Config `toml:"security" json:"security"`
	TLS      *tls.Config     `toml:"-" json:"-"`
	// if EncryptedPassword is not empty, Password will be ignore.
	EncryptedPassword       string            `toml:"encrypted_password" json:"encrypted_password"`
	SyncMode                int               `toml:"sync-mode" json:"sync-mode"`
	Port                    int               `toml:"port" json:"port"`
	Checkpoint              CheckpointConfig  `toml:"checkpoint" json:"checkpoint"`
	BinlogFileDir           string            `toml:"dir" json:"dir"`
	BinlogFileRetentionTime int               `toml:"retention-time" json:"retention-time"`
	Params                  map[string]string `toml:"params" json:"params"`

	Merge bool `toml:"merge" json:"merge"`

	ZKAddrs             string `toml:"zookeeper-addrs" json:"zookeeper-addrs"`
	KafkaAddrs          string `toml:"kafka-addrs" json:"kafka-addrs"`
	KafkaVersion        string `toml:"kafka-version" json:"kafka-version"`
	KafkaMaxMessages    int    `toml:"kafka-max-messages" json:"kafka-max-messages"`
	KafkaClientID       string `toml:"kafka-client-id" json:"kafka-client-id"`
	KafkaMaxMessageSize int    `toml:"kafka-max-message-size" json:"kafka-max-message-size"`
	TopicName           string `toml:"topic-name" json:"topic-name"`

	// get it from pd
	ClusterID uint64 `toml:"-" json:"-"`
}

// CheckpointConfig is the Checkpoint configuration.
type CheckpointConfig struct {
	Type     string `toml:"type" json:"type"`
	Schema   string `toml:"schema" json:"schema"`
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	// if EncryptedPassword is not empty, Password will be ignore.
	EncryptedPassword string          `toml:"encrypted_password" json:"encrypted_password"`
	Port              int             `toml:"port" json:"port"`
	Security          security.Config `toml:"security" json:"security"`
	TLS               *tls.Config     `toml:"-" json:"-"`
}

type baseError struct {
	err   error
	errCh chan struct{}
}

func newBaseError() *baseError {
	return &baseError{
		errCh: make(chan struct{}),
	}
}

func (b *baseError) error() <-chan error {
	ret := make(chan error, 1)
	go func() {
		<-b.errCh
		ret <- b.err
	}()

	return ret
}

func (b *baseError) setErr(err error) {
	b.err = err
	close(b.errCh)
}
