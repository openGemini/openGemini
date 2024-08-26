// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: statistics_test.tmpl

// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package statistics_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

func TestRecord(t *testing.T) {
	stat := statistics.NewRecordStatistics()
	tags := map[string]string{"hostname": "127.0.0.1:8866", "mst": "record"}
	stat.Init(tags)
	stat.AddIntervalRecordPoolInUse(2)
	stat.AddIntervalRecordPoolGet(2)
	stat.AddIntervalRecordPoolGetReUse(2)
	stat.AddIntervalRecordPoolAbort(2)
	stat.AddFileCursorPoolInUse(2)
	stat.AddFileCursorPoolGet(2)
	stat.AddFileCursorPoolGetReUse(2)
	stat.AddFileCursorPoolAbort(2)
	stat.AddFileLoopCursorPoolInUse(2)
	stat.AddFileLoopCursorPoolGet(2)
	stat.AddFileLoopCursorPoolGetReUse(2)
	stat.AddFileLoopCursorPoolAbort(2)
	stat.AddFileCursorValidRowPoolInUse(2)
	stat.AddFileCursorValidRowPoolGet(2)
	stat.AddFileCursorValidRowPoolGetReUse(2)
	stat.AddFileCursorValidRowPoolAbort(2)
	stat.AddFileCursorFilterRecordPoolInUse(2)
	stat.AddFileCursorFilterRecordPoolGet(2)
	stat.AddFileCursorFilterRecordPoolGetReUse(2)
	stat.AddFileCursorFilterRecordPoolAbort(2)
	stat.AddAggPoolInUse(2)
	stat.AddAggPoolGet(2)
	stat.AddAggPoolGetReUse(2)
	stat.AddAggPoolAbort(2)
	stat.AddTsmMergePoolInUse(2)
	stat.AddTsmMergePoolGet(2)
	stat.AddTsmMergePoolGetReUse(2)
	stat.AddTsmMergePoolAbort(2)
	stat.AddTsspSequencePoolInUse(2)
	stat.AddTsspSequencePoolGet(2)
	stat.AddTsspSequencePoolGetReUse(2)
	stat.AddTsspSequencePoolAbort(2)
	stat.AddSequenceAggPoolInUse(2)
	stat.AddSequenceAggPoolGet(2)
	stat.AddSequenceAggPoolGetReUse(2)
	stat.AddSequenceAggPoolAbort(2)
	stat.AddCircularRecordPool(2)
	stat.AddSeriesPoolInUse(2)
	stat.AddSeriesPoolGet(2)
	stat.AddSeriesPoolAbort(2)
	stat.AddSeriesPoolGetReUse(2)
	stat.AddSeriesLoopPoolInUse(2)
	stat.AddSeriesLoopPoolGet(2)
	stat.AddSeriesLoopPoolAbort(2)
	stat.AddSeriesLoopPoolGetReUse(2)
	stat.AddLogstoreInUse(2)
	stat.AddLogstoreGet(2)
	stat.AddLogstoreAbort(2)
	stat.AddLogstoreReUse(2)

	fields := map[string]interface{}{
		"IntervalRecordPoolInUse":            int64(2),
		"IntervalRecordPoolGet":              int64(2),
		"IntervalRecordPoolGetReUse":         int64(2),
		"IntervalRecordPoolAbort":            int64(2),
		"FileCursorPoolInUse":                int64(2),
		"FileCursorPoolGet":                  int64(2),
		"FileCursorPoolGetReUse":             int64(2),
		"FileCursorPoolAbort":                int64(2),
		"FileLoopCursorPoolInUse":            int64(2),
		"FileLoopCursorPoolGet":              int64(2),
		"FileLoopCursorPoolGetReUse":         int64(2),
		"FileLoopCursorPoolAbort":            int64(2),
		"FileCursorValidRowPoolInUse":        int64(2),
		"FileCursorValidRowPoolGet":          int64(2),
		"FileCursorValidRowPoolGetReUse":     int64(2),
		"FileCursorValidRowPoolAbort":        int64(2),
		"FileCursorFilterRecordPoolInUse":    int64(2),
		"FileCursorFilterRecordPoolGet":      int64(2),
		"FileCursorFilterRecordPoolGetReUse": int64(2),
		"FileCursorFilterRecordPoolAbort":    int64(2),
		"AggPoolInUse":                       int64(2),
		"AggPoolGet":                         int64(2),
		"AggPoolGetReUse":                    int64(2),
		"AggPoolAbort":                       int64(2),
		"TsmMergePoolInUse":                  int64(2),
		"TsmMergePoolGet":                    int64(2),
		"TsmMergePoolGetReUse":               int64(2),
		"TsmMergePoolAbort":                  int64(2),
		"TsspSequencePoolInUse":              int64(2),
		"TsspSequencePoolGet":                int64(2),
		"TsspSequencePoolGetReUse":           int64(2),
		"TsspSequencePoolAbort":              int64(2),
		"SequenceAggPoolInUse":               int64(2),
		"SequenceAggPoolGet":                 int64(2),
		"SequenceAggPoolGetReUse":            int64(2),
		"SequenceAggPoolAbort":               int64(2),
		"CircularRecordPool":                 int64(2),
		"SeriesPoolInUse":                    int64(2),
		"SeriesPoolGet":                      int64(2),
		"SeriesPoolAbort":                    int64(2),
		"SeriesPoolGetReUse":                 int64(2),
		"SeriesLoopPoolInUse":                int64(2),
		"SeriesLoopPoolGet":                  int64(2),
		"SeriesLoopPoolAbort":                int64(2),
		"SeriesLoopPoolGetReUse":             int64(2),
		"LogstoreInUse":                      int64(2),
		"LogstoreGet":                        int64(2),
		"LogstoreAbort":                      int64(2),
		"LogstoreReUse":                      int64(2),
	}
	statistics.NewTimestamp().Init(time.Second)
	buf, err := stat.Collect(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if err := compareBuffer("record", tags, fields, buf); err != nil {
		t.Fatalf("%v", err)
	}
}
