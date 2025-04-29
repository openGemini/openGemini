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
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/record"
	streamLib "github.com/openGemini/openGemini/lib/stream"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	stream2 "github.com/openGemini/openGemini/services/stream"
	"github.com/stretchr/testify/require"
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
	for i := 0; i < rows[0].Fields.Len(); i++ {
		if m.value < rows[0].Fields[i].NumValue {
			m.value = rows[0].Fields[i].NumValue
		}
	}
	m.timestamp = time.Unix(0, rows[0].Timestamp)
	return nil
}

func (m *MockStorage) WriteRec(db, rp, mst string, ptId uint32, shardID uint64, rec *record.Record, binaryRec []byte) error {
	fmt.Println("flush WriteRec", time.Now())
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

func (m *MockStorage) RegisterOnPTOffload(id uint64, f func(ptID uint32)) {
}

func (m *MockStorage) UninstallOnPTOffload(id uint64) {
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
		Key:      "float",
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

func buildTestRecordNil(size int) *record.Record {
	s := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	var rec record.Record
	rec.Schema = append(rec.Schema, s...)
	for range rec.Schema {
		var colVal record.ColVal
		rec.ColVals = append(rec.ColVals, colVal)
	}
	for i := 0; i < size/4; i++ {
		rec.Column(0).AppendIntegers([]int64{12, 20, 3}...)
		rec.Column(1).AppendFloats([]float64{70.0, 80.0, 90.0}...)
		rec.Column(2).AppendStrings([]string{"shenzhen", "shanghai", "beijin"}...)
		rec.Column(3).AppendBooleans([]bool{true, true, true}...)
		rec.Column(4).AppendIntegers([]int64{time.Now().UnixNano(), time.Now().UnixNano(), time.Now().UnixNano(), time.Now().UnixNano()}...)
		rec.Column(0).AppendIntegerNull()
		rec.Column(1).AppendFloatNull()
		rec.Column(2).AppendStringNull()
		rec.Column(3).AppendBooleanNull()
	}
	return &rec
}

func buildTestRecord(size int) *record.Record {
	s := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	var rec record.Record
	rec.Schema = append(rec.Schema, s...)
	for range rec.Schema {
		var colVal record.ColVal
		rec.ColVals = append(rec.ColVals, colVal)
	}
	for i := 0; i < size/4; i++ {
		rec.Column(0).AppendIntegers([]int64{12, 20, 3, 30}...)
		rec.Column(1).AppendFloats([]float64{70.0, 80.0, 90.0, 121.0}...)
		rec.Column(2).AppendStrings([]string{"shenzhen", "shanghai", "beijin", "guangzhou"}...)
		rec.Column(3).AppendBooleans([]bool{true, true, true, false}...)
		rec.Column(4).AppendIntegers([]int64{time.Now().UnixNano(), time.Now().UnixNano(), time.Now().UnixNano(), time.Now().UnixNano()}...)
	}
	return &rec
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
	for _, f := range fields {
		msg += ", "
		msg += f.String
		msg += "\n"
	}
	m.t.Log(msg)
}

type MockMetaclient struct {
	getInfoFail bool
}

func (m MockMetaclient) GetNodePT(database string) []uint32 {
	//TODO implement me
	//panic("implement me")
	return []uint32{0}
}

func (m MockMetaclient) Measurement(dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
	if m.getInfoFail {
		return nil, errors.New("get info fail")
	}
	schema := meta.CleanSchema{}
	schema["float"] = meta.SchemaVal{Typ: influx.Field_Type_Float}
	for i := 0; i < 8; i++ {
		schema[fmt.Sprintf("tagkey%v", i)] = meta.SchemaVal{Typ: influx.Field_Type_Tag}
	}
	return &meta.MeasurementInfo{
		Name:          "flow",
		ShardKeys:     nil,
		Schema:        &schema,
		IndexRelation: influxql.IndexRelation{},
		MarkDeleted:   false,
	}, nil
}

func (m MockMetaclient) Database(name string) (di *meta.DatabaseInfo, err error) {
	panic("implement me")
}

func (m MockMetaclient) GetStreamInfos() map[string]*meta.StreamInfo {
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

func (ww *MockWritePointsWork) Ref() {

}

func (ww *MockWritePointsWork) UnRef() int64 {
	return 0
}

func Test_Call(t *testing.T) {
	m := &MockStorage{}
	l := &MockLogger{t}
	metaClient := &MockMetaclient{}
	conf := stream2.NewConfig()
	stream, err := NewStream(m, l, metaClient, conf, "/tmp", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	FlushParallelMinRowNum = 10
	dataBlock := &MockWritePointsWork{}
	fieldRows := buildRows(30)
	fieldRows2 := buildRows(1)
	fieldRows3 := buildRows(2)
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

	stream.WriteRows(&WriteStreamRowsCtx{
		"test", "auto", 0, 0, map[uint64]uint64{}, dataBlock, nil})
	//for test reuse object
	stream.Drain()
	for k := range fieldRows2 {
		fieldRows2[k].Timestamp = start.UnixNano()
		fieldRows2[k].Tags[0].Value = fmt.Sprintf("%vkk%v", k, 1)
	}
	dataBlock2 := &MockWritePointsWork{}
	dataBlock.SetRows(fieldRows2)
	stream.WriteRows(&WriteStreamRowsCtx{"test", "auto", 0, 0, map[uint64]uint64{}, dataBlock2, nil})
	t.Log("write rows ", start, time.Now())
	stream.Drain()

	for k := range fieldRows3 {
		fieldRows3[k].Timestamp = start.UnixNano()
		fieldRows3[k].Tags[0].Value = fmt.Sprintf("%vkk%v", k, 1)
	}
	dataBlock3 := &MockWritePointsWork{}
	dataBlock3.SetRows(fieldRows3)
	stream.WriteReplayRows("test", "auto", 0, 0, dataBlock3)
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

func Test_RecCall(t *testing.T) {
	m := &MockStorage{}
	l := &MockLogger{t}
	metaClient := &MockMetaclient{}
	conf := stream2.NewConfig()
	stream, err := NewStream(m, l, metaClient, conf, "/tmp", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	FlushParallelMinRowNum = 10
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
	fieldRec := buildTestRecord(30)
	stream.WriteRec("test", "auto", "flow", 0, 0, fieldRec, nil)
	//for test reuse object
	stream.Drain()
	fieldRec2 := buildTestRecord(1)
	stream.WriteRec("test", "test", "test", 0, 0, fieldRec2, nil)
	t.Log("write rec ", start, time.Now())
	stream.Drain()
	select {
	case <-after.C:
	}
	//wait flush
	time.Sleep(window)
	t.Log("m count", m.Count(), "m value", m.Value(), time.Now())
	if m.Value() != 2527 {
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
	stream, err := NewStream(m, l, metaClient, conf, "/tmp", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	go stream.Run()
	groupKeys := []string{}
	for i := 0; i < 8; i++ {
		groupKeys = append(groupKeys, fmt.Sprintf("tagkey%v", i))
	}
	fieldCalls := []*streamLib.FieldCall{}
	call, err := streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "float", "float", "sum", true)
	if err != nil {
		t.Fatal(err)
	}
	fieldCalls = append(fieldCalls, call)
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
	stream, err := NewStream(m, l, metaClient, conf, "/tmp", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	go stream.Run()
	groupKeys := []string{}
	for i := 0; i < 8; i++ {
		groupKeys = append(groupKeys, fmt.Sprintf("tagkey%v", i))
	}
	fieldCalls := []*streamLib.FieldCall{}
	call, err := streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "float", "float", "sum", false)
	if err != nil {
		t.Fatal(err)
	}
	fieldCalls = append(fieldCalls, call)
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
	stream, err := NewStream(m, l, metaClient, conf, "/tmp", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	groupKeys := []string{}
	for i := 0; i < 8; i++ {
		groupKeys = append(groupKeys, fmt.Sprintf("tagkey%v", i))
	}
	fieldCalls := []*streamLib.FieldCall{}
	call, err := streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "float", "float", "sum", true)
	if err != nil {
		t.Fatal(err)
	}
	fieldCalls = append(fieldCalls, call)
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
	stream.WriteRows(&WriteStreamRowsCtx{"test", "auto", 0, 0, map[uint64]uint64{}, dataBlock, nil})
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
	stream.DeleteTask(streamInfos["tt"].ID)
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
			Field: "float",
			Alias: "float",
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
	stream, err := NewStream(m, l, metaClient, conf, "/tmp", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	groupKeys := []string{}
	for i := 0; i < 8; i++ {
		groupKeys = append(groupKeys, fmt.Sprintf("tagkey%v", i))
	}
	window := time.Second * 10
	maxDelay := time.Second * 1

	fieldCalls := []*streamLib.FieldCall{}
	call, err := streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "float", "float", "sum", true)
	if err != nil {
		t.Fatal(err)
	}
	fieldCalls = append(fieldCalls, call)

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
			stream.WriteRows(&WriteStreamRowsCtx{"test", "auto", 0, 0, map[uint64]uint64{}, dataBlock, nil})
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

func (r *MockRowCache) GetPool() *TaskCache {
	windowCachePool := NewTaskCachePool()
	release := func() {
		cur := atomic.AddInt64(&r.refCount, -1)
		r.res = cur
	}
	r.refCount++
	cache := windowCachePool.Get()
	cache.release = release
	return cache
}

func TestCacheRecord(t *testing.T) {
	r := &CacheRecord{
		db:      "db",
		rp:      "rp",
		mst:     "mst",
		ptId:    0,
		shardID: 0,
	}
	r.Wait()
	r.Retain()
	r.Release()
	r.Wait()
	r.RetainNum(2)
	r.Release()
	r.Release()
	r.Wait()
}

func buildReplayRows(length int) []influx.Row {
	fieldRows := &[]influx.Row{}
	for j := 0; j < length; j++ {
		r := buildRow("'贵阳''西南''公有云'", "", false)
		r.Name = fmt.Sprintf("%v_0000", r.Name)
		*fieldRows = append(*fieldRows, r)
	}
	return *fieldRows
}

func TestStreamHandler(t *testing.T) {
	m := &MockStorage{}
	l := &MockLogger{t}
	metaClient := &MockMetaclient{}
	conf := stream2.NewConfig()
	stream, err := NewStream(m, l, metaClient, conf, "/tmp", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	s := stream.(*Stream)

	start := time.Now()
	fieldRows3 := buildReplayRows(2)
	for k := range fieldRows3 {
		fieldRows3[k].Timestamp = start.UnixNano()
		fieldRows3[k].Tags[0].Value = fmt.Sprintf("%vkk%v", k, 1)
	}
	dataBlock3 := &MockWritePointsWork{}
	dataBlock3.SetRows(fieldRows3)
	db, rp := "test", "auto"
	pt, shardID := 0, 0
	fileNames := []string{fmt.Sprintf("xxx/wal/%s/%d/%s/%d_0_0_0/xxx.wal", db, pt, rp, shardID)}

	err = s.StreamHandler(dataBlock3.GetRows(), fileNames)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestStreamHandlerError(t *testing.T) {
	s := &Stream{
		Logger: MockLogger{
			t: t,
		},
	}
	t.Run("1", func(t *testing.T) {
		err := s.StreamHandler(influx.Rows{}, []string{"xxx"})
		require.NoError(t, err)
	})

	t.Run("2", func(t *testing.T) {
		err := s.StreamHandler(influx.Rows{influx.Row{}}, []string{"xxx"})
		require.NotEmpty(t, err)
	})
}

func TestParseFileName(t *testing.T) {
	t.Run("parse should be success", func(t *testing.T) {
		expDB, expRP := "db", "rp"
		expPT, expSID := uint32(0), uint64(0)
		fileName := fmt.Sprintf("xxx/wal/%s/%d/%s/%d_0_0_0/xxx.wal", expDB, expPT, expRP, expSID)
		db, rp, pt, shardID, err := ParseFileName(fileName, "xxx")
		if err != nil {
			t.Fatal(err.Error())
		}
		assertEqual(t, expDB, db)
		assertEqual(t, expRP, rp)
		assertEqual(t, expPT, pt)
		assertEqual(t, expSID, shardID)
	})

	t.Run("parse should be failed", func(t *testing.T) {
		fileName := "xxx/wal/db/0/rp/0/xxx.wal"
		_, _, _, _, err := ParseFileName(fileName, "xxx")
		if err == nil {
			t.Fatal(err.Error())
		}
	})

	t.Run("parse should be failed2", func(t *testing.T) {
		fileName := "xxx/wal/db/s/rp/0_0_0_0/xxx.wal"
		_, _, _, _, err := ParseFileName(fileName, "xxx")
		if err == nil {
			t.Fatal(err.Error())
		}
	})
}

func assertEqual(t *testing.T, expected, actual interface{}) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}
