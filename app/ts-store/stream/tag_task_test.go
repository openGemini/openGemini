// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"encoding/json"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	streamLib "github.com/openGemini/openGemini/lib/stream"
	"github.com/openGemini/openGemini/lib/stringinterner"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func Test_ConsumeDataAbort(t *testing.T) {
	task := &TagTask{values: sync.Map{}, TaskDataPool: NewTaskDataPool(), BaseTask: &BaseTask{updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0)}}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		task.consumeDataAndUpdateMeta()
	}()
	// wait run
	time.Sleep(time.Second)
	task.updateWindow <- struct{}{}
	close(task.abort)
	// wait abort
	wg.Wait()
}

func Test_ConsumeDataClean(t *testing.T) {
	task := &TagTask{values: sync.Map{}, TaskDataPool: NewTaskDataPool(),
		cleanPreWindow: make(chan struct{}), BaseTask: &BaseTask{Logger: MockLogger{t}, updateWindow: make(chan struct{}),
			abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0)}}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		task.consumeDataAndUpdateMeta()
	}()
	// wait run
	time.Sleep(1 * time.Second)
	task.updateWindow <- struct{}{}
	<-task.cleanPreWindow
	close(task.abort)
	wg.Wait()
}

func Test_FlushAbort(t *testing.T) {
	task := &TagTask{values: sync.Map{}, TaskDataPool: NewTaskDataPool(),
		cleanPreWindow: make(chan struct{}), BaseTask: &BaseTask{Logger: MockLogger{t}, updateWindow: make(chan struct{}),
			abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0)}}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		task.flush()
	}()
	// wait abort
	close(task.abort)
	wg.Wait()
}

func Test_FlushUpdate(t *testing.T) {
	task := &TagTask{values: sync.Map{}, TaskDataPool: NewTaskDataPool(),
		cleanPreWindow: make(chan struct{}), BaseTask: &BaseTask{Logger: MockLogger{t}, updateWindow: make(chan struct{}),
			abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0)}}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		task.flush()
	}()
	// wait update
	<-task.updateWindow
	wg.Wait()
}

func Test_GenerateGroupKey(t *testing.T) {
	task := &TagTask{
		stringDict: stringinterner.NewStringDict(),
		BaseTask:   &BaseTask{Logger: logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream"))},
		bp:         strings2.NewBuilderPool(),
	}
	groupByKeyNum := 5
	var keys []string
	for i := 0; i < groupByKeyNum; i++ {
		keys = append(keys, "tagkey"+strconv.Itoa(i))
	}
	r := buildRow("1", "xx", false)
	groupByKey2 := task.generateGroupKeyUint(keys, &r)
	rightKey := ""
	for i := 1; i <= groupByKeyNum; i++ {
		rightKey = rightKey + strconv.Itoa(i)
		if i != groupByKeyNum {
			rightKey = rightKey + string(config.StreamGroupValueSeparator)
		}
	}
	if groupByKey2 != rightKey {
		t.Fatal("unexpect", groupByKey2, "expect", rightKey)
	}
}

func Test_GenerateGroupKeyUint_NullTag(t *testing.T) {
	task := &TagTask{
		stringDict: stringinterner.NewStringDict(),
		BaseTask:   &BaseTask{Logger: logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream"))},
		bp:         strings2.NewBuilderPool(),
	}
	groupByKeyNum := 5
	offset := 30
	var keys []string
	for i := offset; i < groupByKeyNum+offset; i++ {
		keys = append(keys, "tagkey"+strconv.Itoa(i))
	}
	r := buildRow("1", "xx", false)
	groupByKey := task.generateGroupKeyUint(keys, &r)
	rightKey := ""
	for i := 1; i <= groupByKeyNum; i++ {
		if i != groupByKeyNum {
			rightKey = rightKey + string(config.StreamGroupValueSeparator)
		}
	}
	if groupByKey != rightKey {
		t.Fatal("unexpect", groupByKey, "expect", rightKey)
	}
	splits := strings.Split(rightKey, string(config.StreamGroupValueSeparator))
	for i := 0; i < groupByKeyNum; i++ {
		if splits[i] != "" {
			t.Fatal("unexpect", splits[i])
		}
		str, err := task.unCompressDictKey(splits[i])
		if err != nil {
			t.Fatal(err)
		}
		if str != "" {
			t.Fatal("unexpect", str)
		}
	}
}

func Test_GenerateGroupKeyUint_EmptyKeys(t *testing.T) {
	task := &TagTask{
		stringDict: stringinterner.NewStringDict(),
		BaseTask:   &BaseTask{Logger: logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream"))},
		bp:         strings2.NewBuilderPool(),
	}
	var keys []string
	r := buildRow("1", "xx", false)
	groupByKey := task.generateGroupKeyUint(keys, &r)
	rightKey := ""
	if groupByKey != rightKey {
		t.Fatal("unexpect", groupByKey, "expect", rightKey)
	}
}

func Test_ReplayData(t *testing.T) {
	now := time.Now()
	task := &TagTask{
		ptLoadStatus: map[uint32]*flushStatus{0: {Timestamp: now.Add(-20 * time.Second).UnixNano()}},
		nodePts:      []uint32{0},

		BaseTask: &BaseTask{Logger: MockLogger{t},
			initTime: now.Add(20 * time.Second),
			window:   10 * time.Second,
			rows:     []influx.Row{},
			info:     &meta2.MeasurementInfo{Name: "bps"},
			des:      &meta2.StreamMeasurementInfo{Database: "test", RetentionPolicy: "autogen"},
			store:    &MockStorage{},
			cli:      MockMetaclient{},
		},
	}

	fieldCalls := make([]*streamLib.FieldCall, 0, 3)
	call, _ := streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "bps", "bps", "max", true)
	fieldCalls = append(fieldCalls, call)
	call, _ = streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "bps", "bps", "min", true)
	fieldCalls = append(fieldCalls, call)
	call, _ = streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "bps", "bps", "count", true)
	fieldCalls = append(fieldCalls, call)
	task.fieldCalls = fieldCalls
	task.fieldCallsLen = 3

	t.Run("1", func(t *testing.T) {
		task.initReplayVar()
		replayRow := &ReplayRow{
			db:      "test",
			rp:      "autogen",
			ptID:    0,
			shardID: 0,
			rows:    buildReplayRows(2),
		}

		task.replayWalRow(replayRow)
		expShards := make([]*uint64, 4)
		expShards[2] = &replayRow.shardID

		if ok := slices.Equal(expShards, task.replayShardIds[0]); !ok {
			t.Errorf("Expected %v, got %v", expShards, task.replayShardIds)
		}

		task.flushReplayData()
		s := task.store.(*MockStorage)
		assertEqual(t, s.count, 1)
		assertEqual(t, s.value, float64(2))
	})

	t.Run("2", func(t *testing.T) {
		task.initReplayVar()
		task.BaseTask.store = &MockStorage{}
		FlushParallelMinRowNum = 0
		replayRow := &ReplayRow{
			db:      "test",
			rp:      "autogen",
			ptID:    0,
			shardID: 0,
			rows:    buildReplayRows(30),
		}

		goPool, err := ants.NewPool(1)
		require.NoError(t, err)
		task.goPool = goPool
		task.concurrency = 1
		task.replayWalRow(replayRow)

		task.flushReplayData()

		s := task.store.(*MockStorage)
		assertEqual(t, s.count, 1)
		assertEqual(t, s.value, float64(30))
	})

}

func Test_ReplayWalRow_Error(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		task := &TagTask{}

		err := task.replayWalRow(nil)
		require.NotEmpty(t, err)
	})
	t.Run("2", func(t *testing.T) {
		now := time.Now()
		task := &TagTask{
			ptLoadStatus: map[uint32]*flushStatus{0: {Timestamp: now.Add(-2000 * time.Second).UnixNano()}},
			nodePts:      []uint32{0},
			BaseTask: &BaseTask{Logger: MockLogger{t},
				initTime: now.Add(2000 * time.Second),
				window:   10 * time.Second,
				rows:     []influx.Row{},
				info:     &meta2.MeasurementInfo{Name: "bps"},
				des:      &meta2.StreamMeasurementInfo{Database: "test", RetentionPolicy: "autogen"},
				store:    &MockStorage{},
			},
		}

		task.initReplayVar()
		if task.replayWindowNum != int64(maxReplayWindowNum) {
			t.Fatal("task.replayWindowNum and maxReplayWindowNum not equal")
		}
	})
}

func Test_FlushReplayData(t *testing.T) {
	var shardID uint64 = 1
	task := &TagTask{
		values:         sync.Map{},
		TaskDataPool:   NewTaskDataPool(),
		cleanPreWindow: make(chan struct{}),
		replayShardIds: map[uint32][]*uint64{1: []*uint64{&shardID}},
		BaseTask: &BaseTask{Logger: MockLogger{t},
			updateWindow: make(chan struct{}),
			abort:        make(chan struct{}),
			windowNum:    10,
			stats:        statistics.NewStreamWindowStatItem(0),
		},
	}
	task.flushReplayData()
}

func Test_ResetReplayVar(t *testing.T) {
	task := &TagTask{
		BaseTask: &BaseTask{Logger: MockLogger{t},
			window: 10 * time.Second,
			rows:   []influx.Row{},
			info:   &meta2.MeasurementInfo{Name: "bps"},
			des:    &meta2.StreamMeasurementInfo{Database: "test", RetentionPolicy: "autogen"},
			store:  &MockStorage{},
			cli:    MockMetaclient{},
			id:     1,
		},
	}
	task.dataPath = t.TempDir()
	p := path.Join(task.dataPath, "data", task.des.Database, strconv.Itoa(0), task.des.RetentionPolicy)

	st := &flushStatus{Timestamp: time.Now().UnixNano()}
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal()
	}
	if err := os.MkdirAll(p, 0700); err == nil {
		if _, err := os.Stat(p); err == nil {
			os.WriteFile(path.Join(p, strconv.FormatUint(task.id, 10)), b, 0640)
		}
	}
	task.shardIds = make(map[uint32][]*uint64)
	task.resetReplayVar()
	os.RemoveAll(p)
	if _, ok := task.replayShardIds[0]; !ok {
		t.Fatal()
	}
}

func Benchmark_GenerateGroupKeyUint(t *testing.B) {
	task := &TagTask{
		stringDict: stringinterner.NewStringDict(),
		BaseTask:   &BaseTask{Logger: logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream"))},
		bp:         strings2.NewBuilderPool(),
	}
	groupByKeyNum := 5
	var keys []string
	for i := 0; i < groupByKeyNum; i++ {
		keys = append(keys, "tagkey"+strconv.Itoa(i*2))
	}
	r := buildRow("1", "xx", false)
	rightKey := ""
	for i := 1; i <= groupByKeyNum; i++ {
		rightKey = rightKey + strconv.Itoa(i)
		if i != groupByKeyNum {
			rightKey = rightKey + string(config.StreamGroupValueSeparator)
		}
	}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		groupByKey := task.generateGroupKeyUint(keys, &r)
		if groupByKey != rightKey {
			t.Fatal("unexpect", groupByKey, "expect", rightKey)
		}
	}
}

func Benchmark_Count(t *testing.B) {
	var c uint64
	var max uint64 = 10000000000
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		for {
			{
				c = c + 100
			}
			{
				if c > max {
					break
				}
			}
		}
	}
}

func Benchmark_Atomic_Count(t *testing.B) {
	var c uint64
	var max uint64 = 10000000000
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		for {
			{
				atomic.AddUint64(&c, 100)
			}
			{
				if c > max {
					break
				}
			}
		}
	}
}

func Benchmark_FlushRow(t *testing.B) {
	task := &TagTask{
		values:     sync.Map{},
		stringDict: stringinterner.NewStringDict(),
		bp:         strings2.NewBuilderPool(),
		BaseTask: &BaseTask{Logger: logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
			des: &meta2.StreamMeasurementInfo{
				Name:            "test",
				Database:        "db",
				RetentionPolicy: "autogen",
			}},
	}
	groupByKeyNum := 5
	var keys []string
	row := influx.Row{Name: "test"}
	for i := 0; i < groupByKeyNum; i++ {
		keys = append(keys, "tagkey"+strconv.Itoa(i))
		row.Tags = append(row.Tags, influx.Tag{
			Key:     keys[i],
			Value:   "xx" + strconv.Itoa(i),
			IsArray: false,
		})
	}
	task.groupKeys = keys
	fieldCalls := []*streamLib.FieldCall{}
	call, err := streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "bps", "bps", "sum", true)
	if err != nil {
		t.Fatal(err)
	}
	fieldCalls = append(fieldCalls, call)
	task.fieldCalls = fieldCalls
	groupNum := 100000
	vs := make([]*float64, len(task.fieldCalls)*1)
	for i := 0; i < groupNum; i++ {
		row.Tags[0].Value = "vv" + strconv.Itoa(i)
		key := task.generateGroupKeyUint(task.groupKeys, &row)
		task.values.Store(key, vs)
	}
	t.N = 1000
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		task.indexKeyPool = bufferpool.GetPoints()
		task.generateRows(task.startTimeStamp)
		bufferpool.Put(task.indexKeyPool)
		if task.validNum == 0 {
			continue
		}
		task.validNum = 0
		task.rows = task.rows[:0]
	}
}

func Benchmark_FlushAntPool(t *testing.B) {
	concurrency := 8
	goPool, err := ants.NewPool(concurrency)
	if err != nil {
		t.Fatal(err)
	}
	wg := sync.WaitGroup{}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		for j := 0; j < concurrency; j++ {
			wg.Add(1)
			goPool.Submit(func() {
				wg.Done()
			})
		}
		wg.Wait()
	}
}

func Benchmark_Flush(t *testing.B) {
	concurrency := 8
	wg := sync.WaitGroup{}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		for j := 0; j < concurrency; j++ {
			wg.Add(1)
			go func() {
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

// Benchmark_SyncMap proves that the performance deteriorates superlinearly as the map size increases.
func Benchmark_SyncMap(t *testing.B) {
	num := 10000000
	m := sync.Map{}
	for i := 0; i < num; i++ {
		m.Store(i, "")
	}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		m.Range(func(key, value any) bool {
			return true
		})
	}
}

func Benchmark_Map(t *testing.B) {
	num := 10000000
	m := make(map[string]string)
	for i := 0; i < num; i++ {
		m[strconv.Itoa(i)] = ""
	}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		for k, v := range m {
			_ = k
			_ = v
		}
	}
}

func TestTagTask_FilterRowsByCond_FilterAgg(t *testing.T) {
	task := &TagTask{
		TaskDataPool:    &TaskDataPool{cache: make(chan ChanData, 2)},
		windowCachePool: NewTaskCachePool(),
		BaseTask: &BaseTask{
			condition: &influxql.BinaryExpr{},
			window:    1,
		},
	}

	patch1 := gomonkey.ApplyFunc(filterRowsByExpr, func(rows []influx.Row, expr influxql.Expr) []int {
		return []int{0, 1, 2}
	})
	patch2 := gomonkey.ApplyFunc(splitMatchedRows,
		func(srcRows []influx.Row, matchedIndexes []int, dstMstInfo *meta2.MeasurementInfo, isSelectAll bool) []influx.Row {
			return []influx.Row{{SeriesId: 1}, {SeriesId: 2}, {SeriesId: 3}}
		})
	defer func() {
		patch1.Reset()
		patch2.Reset()
	}()

	res, err := task.FilterRowsByCond(task.windowCachePool.Get())
	require.NoError(t, err)
	require.Equal(t, true, res)
	require.Equal(t, int64(1), task.TaskDataPool.Len())
}
