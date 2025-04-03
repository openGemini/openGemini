// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	streamLib "github.com/openGemini/openGemini/lib/stream"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Time_ConsumeData(t *testing.T) {
	l := &MockLogger{t}
	m := &MockStorage{}
	metaClient := &MockMetaclient{}

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
	interval := 5 * time.Second
	start := time.Now().Truncate(interval).Add(-interval)
	var fieldCalls []*streamLib.FieldCall
	calls := []string{"sum", "min", "max", "count"}
	for _, c := range calls {
		call, err := streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "float", "float", c, false)
		if err != nil {
			t.Fatal(err)
		}
		fieldCalls = append(fieldCalls, call)
	}
	task := &TimeTask{TaskDataPool: NewTaskDataPool(), windowCachePool: NewTaskCachePool(), BaseTask: &BaseTask{updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0),
		Logger: l, store: m, cli: metaClient, src: &src, des: &des, window: interval,
		start: start, end: start.Add(interval), fieldCalls: fieldCalls}}
	task.run()
	// wait run
	fieldRows := buildRows(1000)
	now := time.Now()
	for i := 0; i < 10000; i++ {
		cache := task.windowCachePool.Get()
		cache.ptId = 0
		cache.shardId = 0
		cache.rows = fieldRows
		cache.release = nil
		task.Put(cache)
	}
	task.Drain()
	t.Log("cost", time.Now().Sub(now))
	if task.stats.WindowIn != 10000000 {
		t.Fatal("unexpect in", task.stats.WindowIn)
	}
	if task.stats.WindowSkip != 0 {
		t.Fatal("unexpect skip", task.stats.WindowSkip)
	}
	if task.stats.WindowProcess != 10000000 {
		t.Fatal("unexpect process", task.stats.WindowProcess)
	}
	time.Sleep(interval)
	time.Sleep(interval)
	if task.stats.WindowIn != 0 {
		t.Fatal("unexpect in", task.stats.WindowIn)
	}
	if task.stats.WindowSkip != 0 {
		t.Fatal("unexpect skip", task.stats.WindowSkip)
	}
	if task.stats.WindowProcess != 0 {
		t.Fatal("unexpect process", task.stats.WindowProcess)
	}
	time.Sleep(interval)
	if m.count != 1 {
		t.Fatal("unexpect flush count", m.count)
	}
	err := task.stop()
	if err != nil {
		t.Fatal(err)
	}
}

type invalidChanData struct {
}

func Test_Time_ConsumeRecDataNil(t *testing.T) {
	l := &MockLogger{t}
	m := &MockStorage{}
	metaClient := &MockMetaclient{}

	src := meta.StreamMeasurementInfo{
		Name:            "test",
		Database:        "test",
		RetentionPolicy: "test",
	}
	des := meta.StreamMeasurementInfo{
		Name:            "test1",
		Database:        "test",
		RetentionPolicy: "test",
	}
	interval := 5 * time.Second
	start := time.Now().Truncate(interval).Add(-interval)
	fieldCalls := []*streamLib.FieldCall{}
	calls := []string{"sum", "min", "max", "count"}
	for _, c := range calls {
		call, err := streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "xx1", "xx1", c, false)
		if err != nil {
			t.Fatal(err)
		}
		fieldCalls = append(fieldCalls, call)
	}
	for _, c := range calls {
		call, err := streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "float", "float", c, false)
		if err != nil {
			t.Fatal(err)
		}
		fieldCalls = append(fieldCalls, call)
	}
	for _, c := range calls {
		call, err := streamLib.NewFieldCall(influx.Field_Type_Int, influx.Field_Type_Int, "int", "int", c, false)
		if err != nil {
			t.Fatal(err)
		}
		fieldCalls = append(fieldCalls, call)
	}
	task := &TimeTask{TaskDataPool: NewTaskDataPool(), windowCachePool: NewTaskCachePool(), BaseTask: &BaseTask{updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0),
		Logger: l, store: m, cli: metaClient, src: &src, des: &des, window: interval,
		start: start, end: start.Add(interval), fieldCalls: fieldCalls}}
	task.run()
	// wait run
	fieldRec := buildTestRecordNil(1000)
	now := time.Now()
	for i := 0; i < 10000; i++ {
		cache := &CacheRecord{}
		cache.ptId = 0
		cache.shardID = 0
		cache.db = "test"
		cache.rp = "test"
		cache.mst = "test"
		cache.rec = fieldRec
		cache.Retain()
		task.Put(cache)
	}
	fieldRec1 := buildTestRecordNil(12)
	for i := 0; i < 1; i++ {
		cache := &CacheRecord{}
		cache.ptId = 0
		cache.shardID = 0
		cache.db = "test"
		cache.rp = "test"
		cache.mst = "test"
		cache.rec = fieldRec1
		cache.Retain()
		task.Put(cache)
	}
	task.Put(&invalidChanData{})

	task.Drain()
	t.Log("cost", time.Now().Sub(now))
	if task.stats.WindowIn != 10000012 {
		t.Fatal("unexpect in", task.stats.WindowIn)
	}
	if task.stats.WindowSkip != 0 {
		t.Fatal("unexpect skip", task.stats.WindowSkip)
	}
	if task.stats.WindowProcess != 10000012 {
		t.Fatal("unexpect process", task.stats.WindowProcess)
	}
	time.Sleep(interval)
	time.Sleep(interval)
	if task.stats.WindowIn != 0 {
		t.Fatal("unexpect in", task.stats.WindowIn)
	}
	if task.stats.WindowSkip != 0 {
		t.Fatal("unexpect skip", task.stats.WindowSkip)
	}
	if task.stats.WindowProcess != 0 {
		t.Fatal("unexpect process", task.stats.WindowProcess)
	}
	time.Sleep(interval)
	if m.count <= 0 {
		t.Fatal("unexpect flush count", m.count)
	}
	err := task.stop()
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Time_ConsumeRecData(t *testing.T) {
	l := &MockLogger{t}
	m := &MockStorage{}
	metaClient := &MockMetaclient{}

	src := meta.StreamMeasurementInfo{
		Name:            "test",
		Database:        "test",
		RetentionPolicy: "test",
	}
	des := meta.StreamMeasurementInfo{
		Name:            "test1",
		Database:        "test",
		RetentionPolicy: "test",
	}
	interval := 5 * time.Second
	start := time.Now().Truncate(interval).Add(-interval)
	fieldCalls := []*streamLib.FieldCall{}
	calls := []string{"sum", "min", "max", "count"}
	for _, c := range calls {
		call, err := streamLib.NewFieldCall(influx.Field_Type_Float, influx.Field_Type_Float, "float", "float", c, false)
		if err != nil {
			t.Fatal(err)
		}
		fieldCalls = append(fieldCalls, call)
	}
	for _, c := range calls {
		call, err := streamLib.NewFieldCall(influx.Field_Type_Int, influx.Field_Type_Int, "int", "int", c, false)
		if err != nil {
			t.Fatal(err)
		}
		fieldCalls = append(fieldCalls, call)
	}
	task := &TimeTask{TaskDataPool: NewTaskDataPool(), windowCachePool: NewTaskCachePool(), BaseTask: &BaseTask{updateWindow: make(chan struct{}),
		abort: make(chan struct{}), windowNum: 10, stats: statistics.NewStreamWindowStatItem(0),
		Logger: l, store: m, cli: metaClient, src: &src, des: &des, window: interval,
		start: start, end: start.Add(interval), fieldCalls: fieldCalls}}
	task.run()
	// wait run
	fieldRows := buildTestRecord(1000)
	now := time.Now()
	for i := 0; i < 10000; i++ {
		cache := &CacheRecord{}
		cache.ptId = 0
		cache.shardID = 0
		cache.db = "test"
		cache.rp = "test"
		cache.mst = "test"
		cache.rec = fieldRows
		cache.Retain()
		task.Put(cache)
	}
	task.Drain()
	t.Log("cost", time.Now().Sub(now))
	if task.stats.WindowIn != 10000000 {
		t.Fatal("unexpect in", task.stats.WindowIn)
	}
	if task.stats.WindowSkip != 0 {
		t.Fatal("unexpect skip", task.stats.WindowSkip)
	}
	if task.stats.WindowProcess != 10000000 {
		t.Fatal("unexpect process", task.stats.WindowProcess)
	}
	time.Sleep(interval)
	time.Sleep(interval)
	if task.stats.WindowIn != 0 {
		t.Fatal("unexpect in", task.stats.WindowIn)
	}
	if task.stats.WindowSkip != 0 {
		t.Fatal("unexpect skip", task.stats.WindowSkip)
	}
	if task.stats.WindowProcess != 0 {
		t.Fatal("unexpect process", task.stats.WindowProcess)
	}
	time.Sleep(interval)
	if m.count != 1 {
		t.Fatal("unexpect flush count", m.count)
	}
	err := task.stop()
	if err != nil {
		t.Fatal(err)
	}
}

func TestTimeTask_run_consumeDataFilterOnly(t *testing.T) {
	task := &TimeTask{}
	patch1 := gomonkey.ApplyPrivateMethod(task, "initVar", func(_ *TimeTask) error {
		return nil
	})

	cli := &metaclient.Client{}
	task.BaseTask = &BaseTask{
		cli:        cli,
		fieldCalls: []*streamLib.FieldCall{{Alias: "k1"}, {Alias: "k2"}, {Alias: "k3"}, {Alias: "k4"}},
		des:        &meta.StreamMeasurementInfo{},
	}
	patch3 := gomonkey.ApplyMethod(cli, "Measurement",
		func(_ *metaclient.Client, dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
			return &meta.MeasurementInfo{
				Schema: &meta.CleanSchema{
					"k1": {Typ: influx.Field_Type_Float},
					"k2": {Typ: influx.Field_Type_Int},
					"k3": {Typ: influx.Field_Type_Boolean},
					"k4": {Typ: influx.Field_Type_String},
				},
			}, nil
		})

	defer func() {
		patch1.Reset()
		patch3.Reset()
	}()

	err := task.run()
	require.NoError(t, err)

}

func BenchmarkXX(b *testing.B) {
	a := make([]bool, 10)
	a[5] = true
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100000000; j++ {
			a[5] = true
		}
	}
}

func BenchmarkXX1(b *testing.B) {
	a := make([]bool, 10)
	a[5] = true
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100000000; j++ {
			if !a[5] {
				a[5] = true
			}
		}
	}
}

func TestTimeTask_filterRowsByExprTestTimeTask_filterRowsByExpr(t *testing.T) {
	rs := make([]influx.Row, 0)
	for j := 0; j < 1000; j++ {
		r := &influx.Row{
			Name:   "testmst",
			Tags:   make(influx.PointTags, 0),
			Fields: make(influx.Fields, 0),
		}
		for i := 0; i < 3; i++ {
			r.Tags = append(r.Tags, influx.Tag{Key: fmt.Sprintf("tagk_%d", i), Value: strconv.Itoa(i)})
		}
		for i := 0; i < 5000; i++ {
			r.Fields = append(r.Fields, influx.Field{
				Key:      fmt.Sprintf("fieldk_%d", i),
				NumValue: float64(i + j),
				Type:     influx.Field_Type_Float,
			})
		}
		sort.Stable(&r.Fields)
		rs = append(rs, *r)
	}

	for _, testcase := range []struct {
		cond string
		want []int
	}{
		{
			cond: "fieldk_566 = 566.0",
			want: []int{0},
		},
		{
			cond: `566.0 = "fieldk_566"`,
			want: []int{0},
		},
		{
			cond: "fieldk_566 <= 566.0",
			want: []int{0},
		},
		{
			cond: `566.0 >= "fieldk_566"`,
			want: []int{0},
		},
		{
			cond: "fieldk_566 < 567.0",
			want: []int{0},
		},
		{
			cond: `567.0 > "fieldk_566"`,
			want: []int{0},
		},
		{
			cond: "fieldk_566 >= 1565.0",
			want: []int{999},
		},
		{
			cond: `1565.0 <= "fieldk_566"`,
			want: []int{999},
		},
		{
			cond: "fieldk_566 > 1564.0",
			want: []int{999},
		},
		{
			cond: `1564.0 < "fieldk_566"`,
			want: []int{999},
		},
		{
			cond: `567.0 > "fieldk_566" OR fieldk_566 >= 1565.0`,
			want: []int{0, 999},
		},
		{
			cond: "fieldk_566 = 566.0 or fieldk_566 = 576.0 or fieldk_2323 = 2423.0",
			want: []int{0, 10, 100},
		},
		{
			cond: "(fieldk_2323 = 2423.0 and fieldk_3325 = 3425.0) or fieldk_566 = 566.0 or (fieldk_2323 = 2424.0 and fieldk_3325 = 3426.0)",
			want: []int{0, 100, 101},
		},
		{
			cond: "(fieldk_2323 = 2423.0 and fieldk_3325 = 3425.0)",
			want: []int{100},
		},
		{
			cond: "fieldk_2323 = 2423.0 and fieldk_2323 = 3425.0",
			want: []int{},
		},
		{
			cond: "fieldk_2323::float",
			want: []int{},
		},
	} {
		cond, _ := influxql.ParseExpr(testcase.cond)
		res := filterRowsByExpr(rs, cond)
		assert.Equal(t, testcase.want, res)
	}
}

func BenchmarkTimeTask_isMatchCond(b *testing.B) {
	r := &influx.Row{
		Name:          "testmst",
		Tags:          make(influx.PointTags, 0),
		Fields:        make(influx.Fields, 0),
		ColumnToIndex: map[string]int{},
	}
	for i := 0; i < 3; i++ {
		r.Tags = append(r.Tags, influx.Tag{Key: fmt.Sprintf("tagk_%d", i), Value: strconv.Itoa(i)})
		r.ColumnToIndex[fmt.Sprintf("tagk_%d", i)] = i
	}
	for i := 0; i < 10000; i++ {
		r.Fields = append(r.Fields, influx.Field{
			Key:      fmt.Sprintf("fieldk_%d", i),
			NumValue: float64(i),
			Type:     influx.Field_Type_Float,
		})
		sort.Stable(&r.Fields)
		r.ColumnToIndex[fmt.Sprintf("fieldk_%d", i)] = i + 3
	}

	cond, _ := influxql.ParseExpr("fieldk_566 = 566.0")
	condBinary := cond.(*influxql.BinaryExpr)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := isMatchCond(r, condBinary.LHS.(*influxql.VarRef), condBinary.RHS, condBinary.Op)
		if !res {
			b.Fatal("not match")
		}
	}
}

func BenchmarkTimeTask_filterRowsByExpr(b *testing.B) {
	rs := make([]influx.Row, 0)
	for j := 0; j < 1000; j++ {
		r := &influx.Row{
			Name:          "testmst",
			Tags:          make(influx.PointTags, 0),
			Fields:        make(influx.Fields, 0),
			ColumnToIndex: map[string]int{},
		}
		for i := 0; i < 3; i++ {
			r.Tags = append(r.Tags, influx.Tag{Key: fmt.Sprintf("tagk_%d", i), Value: strconv.Itoa(i)})
			r.ColumnToIndex[fmt.Sprintf("tagk_%d", i)] = i
		}
		for i := 0; i < 5000; i++ {
			r.Fields = append(r.Fields, influx.Field{
				Key:      fmt.Sprintf("fieldk_%d", i),
				NumValue: float64(i + j),
				Type:     influx.Field_Type_Float,
			})
			r.ColumnToIndex[fmt.Sprintf("fieldk_%d", i)] = i + 3
		}
		sort.Stable(&r.Fields)
		rs = append(rs, *r)
	}

	cond, _ := influxql.ParseExpr("fieldk_566 = 566.0 or fieldk_566 = 576.0")
	condBinary := cond.(*influxql.BinaryExpr)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := filterRowsByExpr(rs, condBinary)
		if len(res) != 2 {
			b.Fatal("not match")
		}
	}
}

func TestTimeTask_IsMatchCond(t *testing.T) {
	r := &influx.Row{
		Fields: []influx.Field{
			{Key: "field_int", NumValue: 1, Type: influx.Field_Type_Int},
			{Key: "field_float", NumValue: 1.0, Type: influx.Field_Type_Float},
			{Key: "field_bool", NumValue: 1, Type: influx.Field_Type_Boolean},
			{Key: "field_string", StrValue: "test_value", Type: influx.Field_Type_String},
		},
	}
	sort.Stable(&r.Fields)
	for _, testcase := range []struct {
		expr   string
		expect bool
	}{
		{"field_int::integer = 1", true},
		{"field_int::integer != 2", true},
		{"field_int::integer >= 1", true},
		{"field_int::integer <= 1", true},
		{"field_int::integer < 2", true},
		{"field_int::integer > 0", true},

		{"field_int::integer = 2", false},
		{"field_int::integer != 1", false},
		{"field_int::integer > 3", false},
		{"field_int::integer >= 3", false},
		{"field_int::integer < 0", false},
		{"field_int::integer <= 0", false},

		{"field_float::float = 1.0", true},
		{"field_float::float >= 1.0", true},
		{"field_float::float <= 1.0", true},
		{"field_float::float < 2.0", true},
		{"field_float::float > 0", true},

		{"field_float::float = 2.0", false},
		{"field_float::float != 1.0", false},
		{"field_float::float > 3.0", false},
		{"field_float::float >= 3.0", false},
		{"field_float::float < 0", false},
		{"field_float::float <= 0", false},

		{"field_bool::boolean = true", true},
		{"field_bool::boolean != false", true},
		{"field_bool::boolean = false", false},
		{"field_bool::boolean != true", false},

		{"field_string::string = 'test_value'", true},
		{"field_string::string != 'test_value2'", true},
		{"field_string::string = 'test_value2'", false},
		{"field_string::string != 'test_value'", false},

		{"field_unknown::string != 'test_value'", false},
	} {
		expr, err := influxql.ParseExpr(testcase.expr)
		if err != nil {
			t.Fatal(err)
		}
		exprBinary, ok := expr.(*influxql.BinaryExpr)
		if !ok {
			t.Fatal("not a binary expression")
		}
		res := isMatchCond(r, exprBinary.LHS.(*influxql.VarRef), exprBinary.RHS, exprBinary.Op)
		require.Equal(t, testcase.expect, res, fmt.Sprintf("expr: %s", testcase.expr))
	}
}

func TestTimeTask_CalculateFilterOnly(t *testing.T) {
	task := &TimeTask{
		windowCachePool: NewTaskCachePool(),
		BaseTask: &BaseTask{
			info: &meta.MeasurementInfo{Schema: &meta.CleanSchema{
				"field_int":    {Typ: influx.Field_Type_Int},
				"field_bool":   {Typ: influx.Field_Type_Boolean},
				"field_string": {Typ: influx.Field_Type_String},
			}},
			des:          &meta.StreamMeasurementInfo{},
			indexKeyPool: []byte{1, 1, 1, 1},
		},
	}
	expr, err := influxql.ParseExpr("field_int::integer = 1")
	if err != nil {
		t.Fatal(err)
	}
	task.condition = expr.(*influxql.BinaryExpr)
	rs := []influx.Row{
		{
			Fields: []influx.Field{
				{Key: "field_int", NumValue: 1, Type: influx.Field_Type_Int},
				{Key: "field_float", NumValue: 1.0, Type: influx.Field_Type_Float},
				{Key: "field_bool", NumValue: 1, Type: influx.Field_Type_Boolean},
				{Key: "field_string", StrValue: "test_value", Type: influx.Field_Type_String},
			},
		},
	}
	sort.Stable(&rs[0].Fields)
	cache := &TaskCache{rows: rs}

	var rowsWritten []influx.Row
	patch1 := gomonkey.ApplyPrivateMethod(task, "doWrite",
		func(_ *TimeTask, shardId uint64, ptId uint32, db, rp string, rows []influx.Row) error {
			rowsWritten = append(rowsWritten, rows...)
			return nil
		})
	r := &influx.Row{}
	patch2 := gomonkey.ApplyPrivateMethod(r, "UnmarshalIndexKeys",
		func(_ *influx.Row, indexkeypool []byte) []byte {
			return indexkeypool
		})
	defer func() {
		patch1.Reset()
		patch2.Reset()
	}()

	err = task.calculateFilterOnly(cache)
	require.NoError(t, err)
	require.Equal(t, 1, len(rowsWritten))
	require.Equal(t, 3, len(rowsWritten[0].Fields))

	task.isSelectAll = true
	rs = []influx.Row{
		{
			Fields: []influx.Field{
				{Key: "field_int", NumValue: 1, Type: influx.Field_Type_Int},
				{Key: "field_float", NumValue: 1.0, Type: influx.Field_Type_Float},
				{Key: "field_bool", NumValue: 1, Type: influx.Field_Type_Boolean},
				{Key: "field_string", StrValue: "test_value", Type: influx.Field_Type_String},
			},
		},
	}
	sort.Stable(&rs[0].Fields)
	cache.rows = rs
	rowsWritten = make([]influx.Row, 0)
	err = task.calculateFilterOnly(cache)
	require.NoError(t, err)
	require.Equal(t, 1, len(rowsWritten))
	require.Equal(t, 4, len(rowsWritten[0].Fields))

	err = task.calculateFilterOnly(nil)
	require.ErrorIs(t, ErrEmptyCache, err)
}

func TestTimeTask_consumeData(t *testing.T) {
	ticker := time.NewTicker(100 * time.Millisecond)
	patch1 := gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
		return ticker
	})
	defer func() {
		patch1.Reset()
	}()
	abort := make(chan struct{})
	task := &TimeTask{
		BaseTask: &BaseTask{
			updateWindow: make(chan struct{}),
			abort:        abort,
		},
		TaskDataPool: &TaskDataPool{cache: make(chan ChanData)},
	}
	go task.consumeData()
	close(abort)
	time.Sleep(1 * time.Millisecond)
	task.updateWindow <- struct{}{}
	task.cache <- &TaskCache{}
	time.Sleep(110 * time.Millisecond)
	assert.Equal(t, 0, len(task.cache))
	assert.Equal(t, 0, len(task.updateWindow))
}

func TestSplitMatchedRows_SelectAll(t *testing.T) {
	src := []influx.Row{
		{SeriesId: 1}, {SeriesId: 2}, {SeriesId: 3},
	}
	got := splitMatchedRows(src, []int{0, 2}, &meta.MeasurementInfo{}, true)
	require.Equal(t, 2, len(got))
	require.Equal(t, uint64(1), got[0].SeriesId)
	require.Equal(t, uint64(3), got[1].SeriesId)
}

func TestSplitMatchedRows(t *testing.T) {
	src := []influx.Row{
		{SeriesId: 1, Fields: []influx.Field{{Key: "f1"}, {Key: "f2"}, {Key: "f3"}, {Key: "f4"}}},
		{SeriesId: 2, Fields: []influx.Field{{Key: "f11"}, {Key: "f33"}, {Key: "f44"}}},
		{SeriesId: 3, Fields: []influx.Field{{Key: "f1"}, {Key: "f2"}}},
	}
	got := splitMatchedRows(src, []int{0, 1, 2}, &meta.MeasurementInfo{Name: "name", Schema: &meta.CleanSchema{
		"f1": {Typ: influx.Field_Type_Float},
		"f2": {Typ: influx.Field_Type_Float},
		"f4": {Typ: influx.Field_Type_Float},
	}}, false)
	require.Equal(t, 2, len(got))
	require.Equal(t, got[0].SeriesId, uint64(1))
	require.Equal(t, len(got[0].Fields), 3)

	require.Equal(t, got[1].SeriesId, uint64(3))
	require.Equal(t, len(got[1].Fields), 2)
}

func TestTimeTask_FilterRowsByCond_NoneOp(t *testing.T) {
	task := &TimeTask{
		TaskDataPool:    &TaskDataPool{cache: make(chan ChanData, 2)},
		windowCachePool: NewTaskCachePool(),
		BaseTask:        &BaseTask{},
	}
	cache := task.windowCachePool.Get()
	res, err := task.FilterRowsByCond(cache)
	require.NoError(t, err)
	require.Equal(t, false, res)
	require.Equal(t, int64(0), task.TaskDataPool.Len())
}

func TestTimeTask_FilterRowsByCond_AggOnly(t *testing.T) {
	task := &TimeTask{
		TaskDataPool:    &TaskDataPool{cache: make(chan ChanData, 2)},
		windowCachePool: NewTaskCachePool(),
		BaseTask: &BaseTask{
			window: 1,
		},
	}
	cache := task.windowCachePool.Get()
	res, err := task.FilterRowsByCond(cache)
	require.NoError(t, err)
	require.Equal(t, true, res)
	require.Equal(t, int64(1), task.TaskDataPool.Len())
}

func TestTimeTask_FilterRowsByCond_FilterOnly(t *testing.T) {
	task := &TimeTask{
		TaskDataPool:    &TaskDataPool{cache: make(chan ChanData, 2)},
		windowCachePool: NewTaskCachePool(),
		BaseTask: &BaseTask{
			condition: &influxql.BinaryExpr{},
		},
	}

	patch1 := gomonkey.ApplyFunc(filterRowsByExpr, func(rows []influx.Row, expr influxql.Expr) []int {
		return []int{0, 1, 2}
	})
	patch2 := gomonkey.ApplyFunc(splitMatchedRows,
		func(srcRows []influx.Row, matchedIndexes []int, dstMstInfo *meta.MeasurementInfo, isSelectAll bool) []influx.Row {
			return []influx.Row{{SeriesId: 1}, {SeriesId: 2}, {SeriesId: 3}}
		})
	defer func() {
		patch1.Reset()
		patch2.Reset()
	}()

	streamRows := make([]influx.Row, 0)
	cache := task.windowCachePool.Get()
	cache.streamRows = &streamRows
	res, err := task.FilterRowsByCond(cache)
	require.NoError(t, err)
	require.Equal(t, false, res)
	require.Equal(t, 3, len(streamRows))
	require.Equal(t, int64(0), task.TaskDataPool.Len())
}

func TestTimeTask_FilterRowsByCond_FilterAgg(t *testing.T) {
	task := &TimeTask{
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
		func(srcRows []influx.Row, matchedIndexes []int, dstMstInfo *meta.MeasurementInfo, isSelectAll bool) []influx.Row {
			return []influx.Row{{SeriesId: 1}, {SeriesId: 2}, {SeriesId: 3}}
		})
	defer func() {
		patch1.Reset()
		patch2.Reset()
	}()

	streamRows := make([]influx.Row, 0)
	cache := task.windowCachePool.Get()
	cache.streamRows = &streamRows
	res, err := task.FilterRowsByCond(cache)
	require.NoError(t, err)
	require.Equal(t, true, res)
	require.Equal(t, 0, len(streamRows))
	require.Equal(t, int64(1), task.TaskDataPool.Len())
}
