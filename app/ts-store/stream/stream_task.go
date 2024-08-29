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

package stream

import (
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	streamLib "github.com/openGemini/openGemini/lib/stream"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type BaseTask struct {
	// store startWindow id, for ring store structure
	startWindowID int64
	offset        int
	// current window start time
	start          time.Time
	startTimeStamp int64
	// current window end time
	end          time.Time
	endTimeStamp int64
	maxTimeStamp int64

	// metadata, not change
	src        *meta2.StreamMeasurementInfo
	des        *meta2.StreamMeasurementInfo
	info       *meta2.MeasurementInfo
	fieldCalls []*streamLib.FieldCall

	// chan for process
	abort        chan struct{}
	err          error
	updateWindow chan struct{}

	indexKeyPool []byte

	// config
	id        uint64
	name      string
	windowNum int64
	window    time.Duration
	maxDelay  time.Duration

	// tmp data, reuse
	fieldCallsLen int
	rows          []influx.Row
	validNum      int
	maxDuration   int64

	// tools
	stats  *statistics.StreamWindowStatItem
	store  Storage
	Logger Logger
	cli    MetaClient
}
