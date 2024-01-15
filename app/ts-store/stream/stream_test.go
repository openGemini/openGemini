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
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	stream2 "github.com/openGemini/openGemini/services/stream"
	"go.uber.org/zap"
)

func Benchmark_Stream_10000(t *testing.B) {
	t.N = 10
	Bench_Stream_POINT(t, 10000)
}

type MockStorage struct {
	count     int
	value     float64
	timestamp time.Time
}

func (m *MockStorage) WriteRows(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte) error {
	fmt.Println("flush WriteRows", time.Now())
	m.count = len(rows) + m.count
	m.value = rows[0].Fields[0].NumValue
	m.timestamp = time.Unix(0, rows[0].Timestamp)
	return nil
}

func (m *MockStorage) Count() int {
	return m.count
}

func (m *MockStorage) Value() float64 {
	return m.value
}

func (m *MockStorage) TimeStamp() time.Time {
	return m.timestamp
}

func buildRow(tagValue, eipValue string, fieldIndex bool) influx.Row {
	tags := &[]influx.Tag{}
	if len(*tags) == 26 {
		for i := 0; i < 26; i++ {
			(*tags)[i].Key = fmt.Sprintf("tagkey%v", i)
			(*tags)[i].Value = fmt.Sprintf("127.0.0.1%v%v", i, tagValue)
		}
	} else {
		for i := 0; i < 26; i++ {
			*tags = append(*tags, influx.Tag{
				Key:   fmt.Sprintf("tagkey%v", i),
				Value: fmt.Sprintf("127.0.0.1%v%v", i, tagValue),
			})
		}
	}
	sort.Slice(*tags, func(i, j int) bool {
		return (*tags)[i].Key < (*tags)[j].Key
	})
	var fields []influx.Field
	fields = append(fields, influx.Field{
		Key:      "bps",
		NumValue: 1,
		StrValue: "",
		Type:     influx.Field_Type_Float,
	})
	fields = append(fields, influx.Field{
		Key:      "eip",
		NumValue: 0,
		StrValue: fmt.Sprintf("127.0.0.1%v", eipValue),
		Type:     influx.Field_Type_String,
	})
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Key < fields[j].Key
	})
	r := &influx.Row{}
	r.Name = "flow"
	r.Tags = *tags
	r.Fields = fields
	r.Timestamp = time.Now().UnixNano()
	r.StreamId = append(r.StreamId, 1)
	if fieldIndex {
		tmpOpt := influx.IndexOption{
			IndexList: []uint16{0},
			Oid:       uint32(0),
		}
		tmpIndexOptions := make([]influx.IndexOption, 0)
		tmpIndexOptions = append(tmpIndexOptions, tmpOpt)
		r.IndexOptions = tmpIndexOptions
	}
	return *r
}

func buildRows(length int) []influx.Row {
	fieldRows := &[]influx.Row{}
	for j := 0; j < length; j++ {
		r := buildRow("'贵阳''西南''公有云'", "", false)
		if j%2 == 0 {
			r.Name = fmt.Sprintf("%v1", r.Name)
		}
		*fieldRows = append(*fieldRows, r)
	}
	return *fieldRows
}

type TestLogger interface {
	Log(args ...any)
}

type MockLogger struct {
	t TestLogger
}

func (m MockLogger) Info(msg string, fields ...zap.Field) {
	m.t.Log(msg)
}

func (m MockLogger) Debug(msg string, fields ...zap.Field) {
	m.t.Log(msg)
}

func (m MockLogger) Error(msg string, fields ...zap.Field) {
	m.t.Log(msg)
}

type MockMetaclient struct {
	getInfoFail bool
}

func (m MockMetaclient) GetMeasurementInfoStore(dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
	if m.getInfoFail {
		return nil, errors.New("get info fail")
	}
	schema := map[string]int32{}
	schema["bps"] = influx.Field_Type_Float
	for i := 0; i < 8; i++ {
		schema[fmt.Sprintf("tagkey%v", i)] = influx.Field_Type_Tag
	}
	return &meta.MeasurementInfo{
		Name:          "flow",
		ShardKeys:     nil,
		Schema:        schema,
		IndexRelation: influxql.IndexRelation{},
		MarkDeleted:   false,
	}, nil
}

func (m MockMetaclient) Database(name string) (di *meta.DatabaseInfo, err error) {
	panic("implement me")
}

func (m MockMetaclient) GetStreamInfosStore() map[string]*meta.StreamInfo {
	calls := []string{"sum", "min", "max", "count"}
	streamInfos := buildStreamInfo(time.Second*1, 0, calls)
	return streamInfos
}

var writePointsWorkPool sync.Pool

type MockWritePointsWork struct {
	rows []influx.Row
}

func GetWritePointsWork() *MockWritePointsWork {
	v := writePointsWorkPool.Get()
	if v == nil {
		return &MockWritePointsWork{}
	}
	return v.(*MockWritePointsWork)
}

func PutWritePointsWork(ww *MockWritePointsWork) {
	ww.reset()
	writePointsWorkPool.Put(ww)
}

func (ww *MockWritePointsWork) GetRows() []influx.Row {
	return ww.rows
}

func (ww *MockWritePointsWork) SetRows(rows []influx.Row) {
	ww.rows = rows
}

func (ww *MockWritePointsWork) PutWritePointsWork() {
	PutWritePointsWork(ww)
}

func (ww *MockWritePointsWork) reset() {
	ww.rows = ww.rows[:0]
}

func Test_Call(t *testing.T) {
	m := &MockStorage{}
	l := &MockLogger{t}
	metaClient := &MockMetaclient{}
	conf := stream2.NewConfig()
	stream, err := NewStream(m, l, metaClient, conf)
	if err != nil {
		t.Fatal(err)
	}
	FlushParallelMinRowNum = 10
	dataBlock := &MockWritePointsWork{}
	fieldRows := buildRows(30)
	fieldRows2 := buildRows(1)
	window := time.Second * 1
	now := time.Now()
	t.Log("now", now)
	go stream.Run()
	//wait stream fresh task
	time.Sleep(10 * time.Second)
	next := now.Truncate(window).Add(window)
	after := time.NewTicker(next.Sub(now))
	select {
	case <-after.C:
	}
	after.Reset(window)
	select {
	case <-after.C:
	}
	start := time.Now()
	for k := range fieldRows {
		fieldRows[k].Timestamp = start.UnixNano()
		fieldRows[k].Tags[0].Value = fmt.Sprintf("%vkk%v", k, 1)
	}
	dataBlock.SetRows(fieldRows)

	stream.WriteRows("test", "auto", 0, 0, map[uint64]uint64{}, dataBlock)
	//for test reuse object
	stream.Drain()
	for k := range fieldRows2 {
		fieldRows2[k].Timestamp = start.UnixNano()
		fieldRows2[k].Tags[0].Value = fmt.Sprintf("%vkk%v", k, 1)
	}
	dataBlock2 := &MockWritePointsWork{}
	dataBlock.SetRows(fieldRows2)
	stream.WriteRows("test", "auto", 0, 0, map[uint64]uint64{}, dataBlock2)
	t.Log("write rows ", start, time.Now())
	stream.Drain()
	select {
	case <-after.C:
	}
	//wait flush
	time.Sleep(window)
	t.Log("m count", m.Count(), "m value", m.Value(), time.Now())
	if m.Value() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, m.Value()))
	}
	ex := start.Truncate(time.Second)
	if ex != m.TimeStamp().Truncate(time.Second) {
		t.Error(fmt.Sprintf("expect %v ,got %v", ex, m.TimeStamp().Truncate(time.Second)))
	}
	stream.Close()
	stream.DeleteTask(1)
	// wait stream filter close
	time.Sleep(time.Second * 3)
}

func Test_RegisterFail(t *testing.T) {
	m := &MockStorage{}
	l := &MockLogger{t}
	metaClient := &MockMetaclient{getInfoFail: true}
	conf := stream2.NewConfig()
	stream, err := NewStream(m, l, metaClient, conf)
	if err != nil {
		t.Fatal(err)
	}
	go stream.Run()
	groupKeys := []string{}
	for i := 0; i < 8; i++ {
		groupKeys = append(groupKeys, fmt.Sprintf("tagkey%v", i))
	}
	fieldCalls := []*FieldCall{}
	fieldCalls = append(fieldCalls, &FieldCall{
		name:         "bps",
		alias:        "bps",
		call:         "sum",
		tagFunc:      nil,
		inFieldType:  influx.Field_Type_Float,
		outFieldType: influx.Field_Type_Float,
	})
	window := time.Second * 3
	maxDelay := time.Second * 10
	calls := []string{"sum"}
	streamInfos := buildStreamInfo(window, maxDelay, calls)
	now := time.Now()
	t.Log("now", now)
	stream.RegisterTask(streamInfos["tt"], fieldCalls)
	//Truncate time
	next := now.Truncate(window).Add(window)
	after := time.NewTicker(next.Sub(now))
	select {
	case <-after.C:
		after.Reset(window)
	}
	stream.Close()
}

func Test_RegisterTimeTask(t *testing.T) {
	m := &MockStorage{}
	l := &MockLogger{t}
	metaClient := &MockMetaclient{getInfoFail: true}
	conf := stream2.NewConfig()
	stream, err := NewStream(m, l, metaClient, conf)
	if err != nil {
		t.Fatal(err)
	}
	go stream.Run()
	groupKeys := []string{}
	for i := 0; i < 8; i++ {
		groupKeys = append(groupKeys, fmt.Sprintf("tagkey%v", i))
	}
	fieldCalls := []*FieldCall{}
	fieldCalls = append(fieldCalls, &FieldCall{
		name:         "bps",
		alias:        "bps",
		call:         "sum",
		tagFunc:      nil,
		inFieldType:  influx.Field_Type_Float,
		outFieldType: influx.Field_Type_Float,
	})
	window := time.Second * 3
	maxDelay := time.Second * 10
	calls := []string{"sum"}
	streamInfos := buildStreamInfoGroup(window, maxDelay, calls, 0)
	now := time.Now()
	t.Log("now", now)
	stream.RegisterTask(streamInfos["tt"], fieldCalls)
	//Truncate time
	next := now.Truncate(window).Add(window)
	after := time.NewTicker(next.Sub(now))
	select {
	case <-after.C:
		after.Reset(window)
	}
	stream.Close()
}

func Test_MaxDelay(t *testing.T) {
	m := &MockStorage{}
	l := &MockLogger{t}
	metaClient := &MockMetaclient{}
	conf := stream2.NewConfig()
	stream, err := NewStream(m, l, metaClient, conf)
	if err != nil {
		t.Fatal(err)
	}
	groupKeys := []string{}
	for i := 0; i < 8; i++ {
		groupKeys = append(groupKeys, fmt.Sprintf("tagkey%v", i))
	}
	fieldCalls := []*FieldCall{}
	fieldCalls = append(fieldCalls, &FieldCall{
		name:         "bps",
		alias:        "bps",
		call:         "sum",
		tagFunc:      nil,
		inFieldType:  influx.Field_Type_Float,
		outFieldType: influx.Field_Type_Float,
	})
	fieldRows := buildRows(100)
	window := time.Second * 3
	maxDelay := time.Second * 10
	calls := []string{"sum"}
	streamInfos := buildStreamInfo(window, maxDelay, calls)
	now := time.Now()
	t.Log("now", now)
	stream.RegisterTask(streamInfos["tt"], fieldCalls)
	//Truncate time
	next := now.Truncate(window).Add(window)
	after := time.NewTicker(next.Sub(now))
	select {
	case <-after.C:
		after.Reset(window)
	}
	select {
	case <-after.C:
		after.Reset(maxDelay)
	}
	start := time.Now()
	now = start
	for k := range fieldRows {
		fieldRows[k].Timestamp = now.UnixNano()
		fieldRows[k].Tags[0].Value = fmt.Sprintf("%vkk%v", k, 1)
	}
	dataBlock := &MockWritePointsWork{}
	dataBlock.SetRows(fieldRows)
	stream.WriteRows("test", "auto", 0, 0, map[uint64]uint64{}, dataBlock)
	t.Log("write rows ", now, time.Now())
	stream.Drain()
	select {
	case <-after.C:
		after.Reset(window)
	}
	//wait flush
	time.Sleep(1 * time.Second)
	t.Log("m count", m.Count(), "m value", m.Value(), time.Now())
	if m.Value() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, m.Value()))
	}
	select {
	case <-after.C:
	}
	//wait flush
	time.Sleep(1 * time.Second)
	t.Log("m count", m.Count(), "m value", m.Value(), time.Now())
	if m.Value() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, m.Value()))
	}
	ex := start.Truncate(time.Second)
	if ex != m.TimeStamp().Truncate(time.Second) {
		t.Error(fmt.Sprintf("expect %v ,got %v", ex, m.TimeStamp().Truncate(time.Second)))
	}

}

func buildStreamInfo(window, maxDelay time.Duration, calls []string) map[string]*meta.StreamInfo {
	return buildStreamInfoGroup(window, maxDelay, calls, 8)
}

func buildStreamInfoGroup(window, maxDelay time.Duration, calls []string, groupNum int) map[string]*meta.StreamInfo {
	groupKeys := []string{}
	for i := 0; i < groupNum; i++ {
		groupKeys = append(groupKeys, fmt.Sprintf("tagkey%v", i))
	}
	src := meta.StreamMeasurementInfo{
		Name:            "flow",
		Database:        "test",
		RetentionPolicy: "auto",
	}
	des := meta.StreamMeasurementInfo{
		Name:            "flow1",
		Database:        "test",
		RetentionPolicy: "auto",
	}
	var mFieldCalls []*meta.StreamCall
	for _, c := range calls {
		mFieldCalls = append(mFieldCalls, &meta.StreamCall{
			Field: "bps",
			Alias: "bps",
			Call:  c,
		})
	}
	streamInfos := map[string]*meta.StreamInfo{}
	streamInfos["tt"] = &meta.StreamInfo{
		Name:     "tt",
		ID:       1,
		SrcMst:   &src,
		DesMst:   &des,
		Interval: window,
		Dims:     groupKeys,
		Calls:    mFieldCalls,
		Delay:    maxDelay,
	}
	return streamInfos
}

func Bench_Stream_POINT(t *testing.B, pointNum int) {
	m := &MockStorage{}
	l := &MockLogger{t}
	metaClient := &MockMetaclient{}
	conf := stream2.NewConfig()
	conf.FilterConcurrency = 1
	stream, err := NewStream(m, l, metaClient, conf)
	if err != nil {
		t.Fatal(err)
	}
	groupKeys := []string{}
	for i := 0; i < 8; i++ {
		groupKeys = append(groupKeys, fmt.Sprintf("tagkey%v", i))
	}
	window := time.Second * 10
	maxDelay := time.Second * 1

	fieldCalls := []*FieldCall{}
	fieldCalls = append(fieldCalls, &FieldCall{
		name:         "bps",
		alias:        "bps",
		call:         "sum",
		tagFunc:      nil,
		inFieldType:  influx.Field_Type_Float,
		outFieldType: influx.Field_Type_Float,
	})

	calls := []string{"sum"}
	streamInfos := buildStreamInfo(window, maxDelay, calls)
	stream.RegisterTask(streamInfos["tt"], fieldCalls)
	now := time.Now()
	next := now.Truncate(window).Add(window)
	after := time.NewTicker(next.Sub(now))
	select {
	case <-after.C:
		after.Reset(window)
	}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		now = time.Now()
		rows := make([]influx.Rows, 20)
		for j := 0; j < 20; j++ {
			fieldRows := buildRows(5000)
			rows[j] = fieldRows
		}
		tt := time.Now().UnixNano()
		for j := 0; j < 20; j++ {
			fieldRows := &rows[j]
			for k := range *fieldRows {
				(*fieldRows)[k].Timestamp = tt
				(*fieldRows)[k].Tags[0].Value = fmt.Sprintf("%vkk", k)
				(*fieldRows)[k].Tags[1].Value = fmt.Sprintf("%vkk", j)
			}
		}
		fmt.Println("build cost ", time.Now().Sub(now))

		for j := 0; j < 20; j++ {
			dataBlock := &MockWritePointsWork{}
			dataBlock.SetRows(rows[j])
			stream.WriteRows("test", "auto", 0, 0, map[uint64]uint64{}, dataBlock)
		}
		stream.Drain()
		t.StopTimer()
		<-after.C
		t.Log("m count", m.Count(), "m value", m.Value())
		if m.Value() != 0 && int(m.Value()) != 1 {
			t.Error(fmt.Sprintf("expect %v ,got %v", 1, m.Value()))
		}
	}
	t.Log("m count", m.Count(), "m value", m.Value())
}

func Test_Release(t *testing.T) {
	m := &MockRowCache{}
	cache1 := m.GetPool()
	cache2 := m.GetPool()
	cache1.release()
	if m.res != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, m.res))
	}
	cache2.release()
	if m.res != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, m.res))
	}
}

type MockRowCache struct {
	refCount int64
	res      int64
}

func (r *MockRowCache) GetPool() *WindowCache {
	windowCachePool := NewWindowCachePool()
	release := func() bool {
		cur := atomic.AddInt64(&r.refCount, -1)
		r.res = cur
		return true
	}
	r.refCount++
	cache := windowCachePool.Get()
	cache.release = release
	return cache
}
