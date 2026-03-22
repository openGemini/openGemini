// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package executor

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/lastrowcache"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestNewIndexScanTransformRunning(t *testing.T) {
	cfg := config.NewLastRowCacheConfig()
	config.SetLastRowCacheConfig(cfg)
	lastrowcache.SetLastRowCacheEnabled(true)

	err := lastrowcache.InitCache()
	if err != nil {
		t.Fatal(err)
	}
	trans := IndexScanTransform{
		inputPort:       &ChunkPort{State: make(chan Chunk)},
		isLastPerfQuery: true,
		rowCnt:          1,
		output:          &ChunkPort{State: make(chan Chunk)},
	}
	sources := []influxql.Source{&influxql.Measurement{Database: "test"}, &influxql.SubQuery{}}
	info := &IndexScanExtraInfo{Req: &RemoteQuery{Opt: query.ProcessorOptions{Sources: sources}}}
	trans.info = info
	mapping := make(map[influxql.Expr]influxql.VarRef)
	mapping[&influxql.VarRef{Val: "field1"}] = influxql.VarRef{Val: "field1"}
	fakeSchema := &QuerySchema{mapping: mapping}
	trans.schema = fakeSchema
	trans.opt.SeriesKey = []byte("test,t1=1")
	ctx, cancel := context.WithCancel(context.Background())

	p := gomonkey.ApplyMethod(fakeSchema, "IsLastRowQuery", func(_ *QuerySchema) bool {
		return true
	})
	defer p.Reset()

	go trans.Running(ctx)

	outputRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "field1", Type: influxql.Integer},
	)
	chunkT := NewChunkImpl(outputRowDataType, "test")
	chunkT.SetTime([]int64{0})
	chunkT.ResetIntervalIndex(0)
	c1 := NewColumnImpl(influxql.Integer)
	c1.AppendIntegerValue(1)
	chunkT.AddColumn(c1)

	p2 := gomonkey.ApplyMethod(c1, "IsNilV2", func(_ *ColumnImpl, _ int) bool {
		return false
	})
	defer p2.Reset()

	p3 := gomonkey.ApplyMethod(c1, "StringValue", func(_ *ColumnImpl, _ int) int64 {
		return 10086
	})
	defer p3.Reset()

	//trans.inputPort.State <- &ChunkImpl{time: []int64{0, 0, 0}, rowDataType: rowDataType}
	trans.inputPort.State <- chunkT
	<-trans.output.State
	cancel()
	trans.wg.Wait()
	// cache set value immediately query will get nothing， thus sleep 10ms
	time.Sleep(time.Millisecond * 10)
	_, _, ok := lastrowcache.Get("test", "", []byte("test,t1=1"), []string{"field1"})
	assert.True(t, ok)
}

func TestChunkToLastRowCache(t *testing.T) {
	convey.Convey("test chunk to last row", t, func() {
		rowDataT := hybridqp.NewRowDataTypeImpl(
			influxql.VarRef{Val: "value1", Type: influxql.Integer},
			influxql.VarRef{Val: "value2", Type: influxql.Float},
			influxql.VarRef{Val: "value3", Type: influxql.String},
			influxql.VarRef{Val: "value4", Type: influxql.Boolean},
			influxql.VarRef{Val: "value5", Type: influxql.Boolean},
		)
		chunk := &ChunkImpl{time: []int64{0, 0}, rowDataType: rowDataT}
		c1 := NewColumnImpl(influxql.Integer)
		c1.AppendIntegerValue(1)
		c1.AppendNilsV2(true)
		c2 := NewColumnImpl(influxql.Float)
		c2.AppendFloatValue(2)
		c2.AppendNilsV2(true)
		c3 := NewColumnImpl(influxql.String)
		c3.AppendStringBytes([]byte("test"), []uint32{0})
		c3.AppendNilsV2(true)
		c4 := NewColumnImpl(influxql.Boolean)
		c4.AppendBooleanValue(true)
		c4.AppendNilsV2(true)
		c5 := NewColumnImpl(influxql.Boolean)
		c5.AppendBooleanValue(true)
		c5.AppendNilsV2(false)

		chunk.AddColumn([]Column{c1, c2, c3, c4, c5}...)
		mapping := make(map[influxql.Expr]influxql.VarRef)
		mapping[&influxql.VarRef{}] = influxql.VarRef{Val: "value1"}
		mapping[&influxql.Call{Args: []influxql.Expr{&influxql.VarRef{Val: "value2"}}}] = influxql.VarRef{Val: "value2"}
		schema := &QuerySchema{mapping: make(map[influxql.Expr]influxql.VarRef)}
		rnoutputs := []gomonkey.OutputCell{
			{Values: gomonkey.Params{"testInt"}},
			{Values: gomonkey.Params{"testFloat"}},
			{Values: gomonkey.Params{"testString"}},
			{Values: gomonkey.Params{"testBoolean"}},
			{Values: gomonkey.Params{"testBoolean2"}},
		}
		p := gomonkey.ApplyFuncSeq(LastRowFieldKey, rnoutputs)
		defer p.Reset()
		ans := ChunkToLastRowCache(chunk, schema)
		convey.So(ans, convey.ShouldNotBeNil)
	})
}

func TestLastRowFieldKey(t *testing.T) {
	convey.Convey("test last row field key", t, func() {
		field := influxql.VarRef{Val: "test"}
		mapping := map[influxql.Expr]influxql.VarRef{
			&influxql.Call{}: influxql.VarRef{Val: "222"},
			&influxql.Call{Args: []influxql.Expr{&influxql.VarRef{Val: "test"}}}: influxql.VarRef{Val: "test"},
		}
		ans := LastRowFieldKey(&field, mapping)
		convey.So(ans, convey.ShouldEqual, "test")

		mapping = map[influxql.Expr]influxql.VarRef{
			&influxql.Call{Args: []influxql.Expr{&influxql.Call{Args: []influxql.Expr{&influxql.VarRef{Val: "test"}}}}}: influxql.VarRef{Val: "test"},
		}
		ans = LastRowFieldKey(&field, mapping)
		convey.So(ans, convey.ShouldEqual, "")
	})
}

type MocTsspFile struct {
	path string
	immutable.TSSPFile
}

func (m MocTsspFile) Path() string {
	return m.path
}

func (m MocTsspFile) Unref() {
}

func (m MocTsspFile) UnrefFileReader() {
}

type MockTSIndexInfo struct {
	comm.TSIndexInfo
}

func (m *MockTSIndexInfo) Unref() {
}

func buildShardsFrags() ShardsFragments {
	lockPath := "./tssp"
	fileName := "00000001-0001-00000000.tssp"
	f1 := &MocTsspFile{path: lockPath + "/" + fileName}
	shardsFragments := map[uint64]*FileFragments{
		1: {FragmentCount: 9,
			FileMarks: map[string]FileFragment{
				f1.Path(): NewFileFragmentForTest(f1, fragment.FragmentRanges{{Start: 1, End: 10}}, 9)}},
	}
	return shardsFragments
}

func buildIndexInfo() *CSIndexInfo {
	lockPath := "./tssp"
	fileName := "00000001-0001-00000000.tssp"
	f1 := &MocTsspFile{path: lockPath + "/" + fileName}
	return NewCSIndexInfo("shard1", NewAttachedIndexInfo([]immutable.TSSPFile{f1}, nil), 0)
}

func TestIndexScanTransform_UnrefIndex(t *testing.T) {
	transform := &IndexScanTransform{
		frags:       buildShardsFrags(),
		indexInfo:   buildIndexInfo(),
		tsIndexInfo: &MockTSIndexInfo{},
	}
	transform.Unref()
}
