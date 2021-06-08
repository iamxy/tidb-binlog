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

import "sync"

// GCTS wraps a int64 TS value with a mutex to protect/detect binlog purging when fetching binlogs.
type GCTS struct {
	mux sync.RWMutex
	ts  int64
}

// Store stores or updates the current TS value.
func (g *GCTS) Store(ts int64) {
	g.mux.Lock()
	defer g.mux.Unlock()
	g.ts = ts
}

// Load loads or reads the current TS value.
func (g *GCTS) Load() int64 {
	g.mux.RLock()
	defer g.mux.RUnlock()
	return g.ts
}

// LoadAndLock loads the current TS value and hold the read-lock.
func (g *GCTS) LoadAndLock() int64 {
	g.mux.RLock()
	return g.ts
}

// ReleaseLoadLock releases the read-lock acquired by LoadAndLock.
func (g *GCTS) ReleaseLoadLock() {
	g.mux.RUnlock()
}
