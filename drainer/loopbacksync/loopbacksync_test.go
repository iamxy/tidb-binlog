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

package loopbacksync

import "testing"

//TestNewLoopBackSyncInfo test loopBackSyncInfo alloc
func TestNewLoopBackSyncInfo(t *testing.T) {
	var ChannelID int64 = 1
	var LoopbackControl = true
	var SyncDDL = false
	l := NewLoopBackSyncInfo(ChannelID, LoopbackControl, SyncDDL, "", nil, false)
	if l == nil {
		t.Error("alloc loopBackSyncInfo objec failed ")
	}
}
