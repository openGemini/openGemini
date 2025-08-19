// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import "time"

const (
	DefaultRaftMsgTimeout    = 15 * time.Second
	DefaultElectionTick      = 10
	DefaultHeartbeatTick     = 1
	DefaultRaftMsgCacheSize  = 1000
	DefaultFileWrapSize      = 128
	DefaultWaitCommitTimeout = 20 * time.Second
	DefaultEntryFileRWType   = 2
)

var RaftMsgTimeout = DefaultRaftMsgTimeout
var ElectionTick = DefaultElectionTick
var HeartbeatTick = DefaultHeartbeatTick
var RaftMsgCacheSize = DefaultRaftMsgCacheSize
var WaitCommitTimeout = DefaultWaitCommitTimeout

var FileWrapSize = DefaultFileWrapSize
var EntryFileRWType = DefaultEntryFileRWType

func SetEntryFileRWType(typ int) {
	EntryFileRWType = typ
}

func SetRaftMsgTimeout(timeout time.Duration) {
	RaftMsgTimeout = timeout
}

func SetElectionTick(tick int) {
	ElectionTick = tick
}

func SetHeartbeatTick(tick int) {
	HeartbeatTick = tick
}

func SetRaftMsgCacheSize(size int) {
	RaftMsgCacheSize = size
}

func SetFileWrapSize(size int) {
	FileWrapSize = size
}

func SetWaitCommitTimeout(timeout time.Duration) {
	WaitCommitTimeout = timeout
}
