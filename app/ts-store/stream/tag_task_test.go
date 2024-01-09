/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stream

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

func Test_WindowDataPool(t *testing.T) {
	pool := NewWindowDataPool()
	pool.Put(nil)
	timer := time.NewTicker(1 * time.Second)
	kk := make(chan *WindowCache)
	select {
	case <-timer.C:
		t.Log("timer occur")
	case kk <- pool.Get():
		t.Fatal("should be block")
	}
}

func Test_CompressDictKey(t *testing.T) {
	task := &TagTask{
		corpus:        sync.Map{},
		corpusIndexes: []string{""},
		corpusIndex:   0,
		Logger:        logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
	}
	key := "ty"
	vv, err := task.compressDictKeyUint(key)
	if err != nil {
		t.Fatal(err)
	}
	vvv := strconv.FormatUint(vv, 10)
	v, err := task.unCompressDictKey(vvv)
	if err != nil {
		t.Fatal(err)
	}
	if key != v {
		t.Error(fmt.Sprintf("expect %v ,got %v", key, v))
	}
}

func Test_CompressDictKeyUint(t *testing.T) {
	task := &TagTask{
		corpus:        sync.Map{},
		corpusIndexes: []string{""},
		corpusIndex:   0,
		Logger:        logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
	}
	key := "ty"
	vv, err := task.compressDictKeyUint(key)
	if err != nil {
		t.Fatal(err)
	}
	v, err := task.UnCompressDictKeyUint(vv)
	if err != nil {
		t.Fatal(err)
	}
	if key != v {
		t.Error(fmt.Sprintf("expect %v ,got %v", key, v))
	}
}

func Benchmark_CompressDictKey(t *testing.B) {
	task := &TagTask{
		corpus:        sync.Map{},
		corpusIndexes: []string{""},
		corpusIndex:   0,
		Logger:        logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
	}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		key := "testKey" + strconv.Itoa(i)
		vv, err := task.compressDictKeyUint(key)
		if err != nil {
			t.Fatal(err)
		}
		vvv := strconv.FormatUint(vv, 10)
		v, err := task.unCompressDictKey(vvv)
		if err != nil {
			t.Fatal(err)
		}
		if key != v {
			t.Error(fmt.Sprintf("expect %v ,got %v", key, v))
		}
	}
}

func Benchmark_CompressDictKeyUint(t *testing.B) {
	task := &TagTask{
		corpus:        sync.Map{},
		corpusIndexes: []string{""},
		corpusIndex:   0,
		Logger:        logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
	}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		key := "testKey" + strconv.Itoa(i)
		vv, err := task.compressDictKeyUint(key)
		if err != nil {
			t.Fatal(err)
		}
		v, err := task.UnCompressDictKeyUint(vv)
		if err != nil {
			t.Fatal(err)
		}
		if key != v {
			t.Error(fmt.Sprintf("expect %v ,got %v", key, v))
		}
	}
}

func Test_ConsumeDataAbort(t *testing.T) {
	task := &TagTask{values: sync.Map{}, WindowDataPool: NewWindowDataPool(), updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0)}
	go task.consumeDataAndUpdateMeta()
	// wait run
	time.Sleep(3 * time.Second)
	task.updateWindow <- struct{}{}
	close(task.abort)
	// wait abort
	time.Sleep(3 * time.Second)
}

func Test_ConsumeDataClean(t *testing.T) {
	task := &TagTask{values: sync.Map{}, WindowDataPool: NewWindowDataPool(), updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0),
		cleanPreWindow: make(chan struct{})}
	go task.consumeDataAndUpdateMeta()
	// wait run
	time.Sleep(3 * time.Second)
	task.updateWindow <- struct{}{}
	<-task.cleanPreWindow
	// wait clean consume
	time.Sleep(3 * time.Second)
}

func Test_FlushAbort(t *testing.T) {
	task := &TagTask{values: sync.Map{}, WindowDataPool: NewWindowDataPool(), updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0),
		cleanPreWindow: make(chan struct{}), Logger: MockLogger{t}}
	go task.flush()
	// wait abort
	close(task.abort)
	time.Sleep(3 * time.Second)
}

func Test_FlushUpdate(t *testing.T) {
	task := &TagTask{values: sync.Map{}, WindowDataPool: NewWindowDataPool(), updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0),
		cleanPreWindow: make(chan struct{}), Logger: MockLogger{t}}
	go task.flush()
	// wait update
	<-task.updateWindow
	time.Sleep(3 * time.Second)
}

func Test_GenerateGroupKey(t *testing.T) {
	task := &TagTask{
		corpus:        sync.Map{},
		corpusIndexes: []string{""},
		corpusIndex:   0,
		Logger:        logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
		bp:            NewBuilderPool(),
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
		corpus:        sync.Map{},
		corpusIndexes: []string{""},
		corpusIndex:   0,
		Logger:        logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
		bp:            NewBuilderPool(),
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
		corpus:        sync.Map{},
		corpusIndexes: []string{""},
		corpusIndex:   0,
		Logger:        logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
		bp:            NewBuilderPool(),
	}
	var keys []string
	r := buildRow("1", "xx", false)
	groupByKey := task.generateGroupKeyUint(keys, &r)
	rightKey := ""
	if groupByKey != rightKey {
		t.Fatal("unexpect", groupByKey, "expect", rightKey)
	}
}

func Benchmark_GenerateGroupKeyUint(t *testing.B) {
	task := &TagTask{
		corpus:        sync.Map{},
		corpusIndexes: []string{""},
		corpusIndex:   0,
		Logger:        logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
		bp:            NewBuilderPool(),
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
		values:        sync.Map{},
		corpus:        sync.Map{},
		corpusIndexes: []string{""},
		corpusIndex:   0,
		Logger:        logger.NewLogger(errno.ModuleStream).With(zap.String("service", "stream")),
		bp:            NewBuilderPool(),
		des: &meta2.StreamMeasurementInfo{
			Name:            "test",
			Database:        "db",
			RetentionPolicy: "autogen",
		},
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
	fieldCalls := []*FieldCall{}
	fieldCalls = append(fieldCalls, &FieldCall{
		name:         "bps",
		alias:        "bps",
		call:         "sum",
		tagFunc:      nil,
		inFieldType:  influx.Field_Type_Float,
		outFieldType: influx.Field_Type_Float,
	})
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
		task.generateRows()
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
