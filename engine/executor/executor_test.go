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

package executor

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/memory"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

var SimpleMergeDAG func()
var PipelineExecutorGen func() *PipelineExecutor
var executor *PipelineExecutor

func init() {
	PipelineExecutorGen = func() *PipelineExecutor {
		chunk1 := BuildChunk()
		chunk2 := BuildChunk()
		chunk3 := BuildChunk()

		opt := query.ProcessorOptions{
			Interval: hybridqp.Interval{
				Duration: 10 * time.Nanosecond,
			},
			Dimensions: []string{"host"},
			Ascending:  true,
			ChunkSize:  100,
		}
		schema := NewQuerySchema(nil, nil, &opt)

		source1 := NewSourceFromSingleChunk(buildRowDataType(), chunk1)
		source2 := NewSourceFromSingleChunk(buildRowDataType(), chunk2)
		source3 := NewSourceFromSingleChunk(buildRowDataType(), chunk3)
		trans := NewMergeTransform([]hybridqp.RowDataType{buildRowDataType(), buildRowDataType(), buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()}, nil, schema)

		sink := NewSinkFromFunction(buildRowDataType(), func(chunk Chunk) error {
			return nil
		})

		Connect(source1.Output, trans.Inputs[0])
		Connect(source2.Output, trans.Inputs[1])
		Connect(source3.Output, trans.Inputs[2])
		Connect(trans.Outputs[0], sink.Input)

		m := NewLogicalMerge([]hybridqp.QueryNode{NewLogicalMocSource(buildRowDataType()), NewLogicalMocSource(buildRowDataType()), NewLogicalMocSource(buildRowDataType())}, schema)
		var processors Processors

		sinkVertex := NewTransformVertex(NewLogicalSink(buildRowDataType(), schema), sink)
		source1Vertex := NewTransformVertex(NewLogicalMocSource(buildRowDataType()), source1)
		source2Vertex := NewTransformVertex(NewLogicalMocSource(buildRowDataType()), source2)
		source3Vertex := NewTransformVertex(NewLogicalMocSource(buildRowDataType()), source3)
		mergeVertex := NewTransformVertex(m, trans)

		dag := NewTransformDag()
		dag.mapVertexToInfo[sinkVertex] = &TransformVertexInfo{
			directEdges: []*TransformEdge{&TransformEdge{from: mergeVertex}},
		}
		dag.mapVertexToInfo[mergeVertex] = &TransformVertexInfo{
			directEdges: []*TransformEdge{&TransformEdge{from: source1Vertex}, &TransformEdge{from: source2Vertex}, &TransformEdge{from: source3Vertex}},
		}

		processors = append(processors, source1)
		processors = append(processors, source2)
		processors = append(processors, source3)
		processors = append(processors, trans)
		processors = append(processors, sink)

		executor := NewPipelineExecutor(processors)
		executor.dag = dag
		executor.root = sinkVertex
		return executor
	}
	executor = PipelineExecutorGen()
}

func TestAppendRowValue(t *testing.T) {
	colFloat := NewColumnImpl(influxql.Float)
	appendRowValue(colFloat, 7.77)
	colInt := NewColumnImpl(influxql.Integer)
	appendRowValue(colInt, int64(7))
	colBool := NewColumnImpl(influxql.Boolean)
	appendRowValue(colBool, true)
	colStr := NewColumnImpl(influxql.String)
	appendRowValue(colStr, "test")
}

func ParseChunkTagsNew(kv [][]string) *ChunkTags {
	var m influx.PointTags
	var ss []string
	for i := range kv {
		m = append(m, influx.Tag{kv[i][0], kv[i][1]})
		ss = append(ss, kv[i][0])
	}
	sort.Sort(&m)
	return NewChunkTags(m, ss)
}

func TestChunkTag(t *testing.T) {
	s := [][]string{{"name", "martino"}, {"sex", ""}, {"country", "china"}}
	tag := ParseChunkTagsNew(s)
	subset := tag.Subset(nil)
	assert.Equal(t, record.Str2bytes("name martino sex  country china "), subset)
	assert.Equal(t, []uint16{6, 5, 13, 17, 18, 26, 32}, tag.offsets)

	for i := range s {
		if v, ok := tag.GetChunkTagValue(s[i][0]); !ok {
			t.Errorf("%s should exist!", s[i][0])
		} else {
			assert.Equal(t, s[i][1], v)
		}
		if v, ok := tag.KeyValues()[s[i][0]]; !ok {
			t.Errorf("%s should exist!", s[i][0])
		} else {
			assert.Equal(t, s[i][1], v)
		}

	}

	k, v := tag.GetChunkTagAndValues()
	assert.Equal(t, []string{"name", "sex", "country"}, k)
	assert.Equal(t, []string{"martino", "", "china"}, v)

	newTag := tag.KeepKeys([]string{"name", "country"})
	newSubset := newTag.Subset(nil)
	assert.Equal(t, record.Str2bytes("name martino country china "), newSubset)
	assert.Equal(t, []uint16{4, 5, 13, 21, 27}, newTag.offsets)
}

func BuildChunk() Chunk {
	rowDataType := buildRowDataType()

	b := NewChunkBuilder(rowDataType)

	chunk := b.NewChunk("mst")

	chunk.AppendTime([]int64{11, 12, 13, 14, 15}...)
	chunk.AddTagAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AddIntervalIndex(0)

	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 3, 4, 5}...)
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4, 5}...)
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendStringValues([]string{"tomA", "jerryA", "danteA", "martino"}...)
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 4, 5}...)
	chunk.Column(1).AppendNilsV2(true, true, false, true, true)

	chunk.Column(2).AppendFloatValues([]float64{1.1, 1.2, 1.3, 1.4}...)
	chunk.Column(2).AppendColumnTimes([]int64{1, 2, 3, 4}...)
	chunk.Column(2).AppendManyNotNil(4)
	chunk.Column(2).AppendNil()

	return chunk
}

func TestMultiPipelineExecutors(t *testing.T) {
	pipelineExecutorResourceManager.Reset()
	var wg sync.WaitGroup
	SimpleMergeDAG = func() {
		defer wg.Done()
		executor := PipelineExecutorGen()
		if e := executor.ExecuteExecutor(context.Background()); e != nil {
			t.Errorf(e.Error())
		}
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go SimpleMergeDAG()
	}
	wg.Wait()
	if pipelineExecutorResourceManager.memBucket.GetFreeResource() != pipelineExecutorResourceManager.memBucket.GetTotalResource() {
		t.Errorf("still has occupied memories")
	}
}

func TestMultiPipelineExecutors_MemSize(t *testing.T) {
	defer func() {
		mem, _ := memory.SysMem()
		pipelineExecutorResourceManager.SetManagerParas(mem, time.Second)
	}()
	pipelineExecutorResourceManager.Reset()
	pipelineExecutorResourceManager.SetManagerParas(30000, time.Second)
	var wg sync.WaitGroup
	SimpleMergeDAG = func() {
		defer wg.Done()
		executor := PipelineExecutorGen()
		if e := executor.ExecuteExecutor(context.Background()); e != nil {
			t.Errorf(e.Error())
		}
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go SimpleMergeDAG()
	}
	wg.Wait()
	if pipelineExecutorResourceManager.memBucket.GetFreeResource() != pipelineExecutorResourceManager.memBucket.GetTotalResource() {
		t.Errorf("still has occupied memories")
	}
}

func TestMultiPipelineExecutors_ALLTimeout(t *testing.T) {
	defer func() {
		mem, _ := memory.SysMem()
		pipelineExecutorResourceManager.SetManagerParas(mem, time.Second)
	}()
	pipelineExecutorResourceManager.Reset()
	pipelineExecutorResourceManager.SetManagerParas(2000, time.Second)
	var wg sync.WaitGroup
	SimpleMergeDAG = func() {
		defer wg.Done()
		executor := PipelineExecutorGen()
		if e := executor.ExecuteExecutor(context.Background()); e != nil {
			if !assert.Equal(t, e.Error(), errno.NewError(errno.BucketLacks).Error()) {
				t.Errorf(e.Error())
			}
		}
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go SimpleMergeDAG()
	}
	wg.Wait()
	if pipelineExecutorResourceManager.memBucket.GetFreeResource() != pipelineExecutorResourceManager.memBucket.GetTotalResource() {
		t.Errorf("still has occupied memories")
	}

	mem, _ := memory.SysMem()
	pipelineExecutorResourceManager.SetManagerParas(mem, time.Second)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go SimpleMergeDAG()
	}

	wg.Wait()
	if pipelineExecutorResourceManager.memBucket.GetFreeResource() != pipelineExecutorResourceManager.memBucket.GetTotalResource() {
		t.Errorf("still has occupied memories")
	}
}

func TestInitMstName(t *testing.T) {
	heap := &AppendHeapItems{}
	assert.Equal(t, "", initMstName(heap))
	heap.Items = append(heap.Items, &Item{
		ChunkBuf: BuildSQLChunk1(),
	})
	heap.Items = append(heap.Items, &Item{
		ChunkBuf: BuildSQLChunk1(),
	})
	assert.Equal(t, "cpu", initMstName(heap))
	heap.Items = append(heap.Items, &Item{
		ChunkBuf: BuildSQLChunk5(),
	})
	assert.Equal(t, "cpu,mst1", initMstName(heap))
}

func TestSortedAppendInit_Empty(t *testing.T) {
	h := &AppendHeapItems{}
	assert.Equal(t, "", initMstName(h))
}

func TestSetPipelineExecutorResourceManagerParas(t *testing.T) {
	defer func() {
		mem, _ := memory.SysMem()
		pipelineExecutorResourceManager.SetManagerParas(mem, time.Second)
	}()
	pipelineExecutorResourceManager.SetManagerParas(0, 0)
	SetPipelineExecutorResourceManagerParas(1000, time.Second)
	assert.Equal(t, int64(1000), pipelineExecutorResourceManager.memBucket.GetTotalResource())
	assert.Equal(t, time.Second, pipelineExecutorResourceManager.memBucket.GetTimeDuration())
	SetPipelineExecutorResourceManagerParas(2000, 2*time.Second)
	assert.Equal(t, int64(1000), pipelineExecutorResourceManager.memBucket.GetTotalResource())
	assert.Equal(t, time.Second, pipelineExecutorResourceManager.memBucket.GetTimeDuration())
	SetPipelineExecutorResourceManagerParas(500, time.Second/10)
	assert.Equal(t, int64(500), pipelineExecutorResourceManager.memBucket.GetTotalResource())
	assert.Equal(t, time.Second/10, pipelineExecutorResourceManager.memBucket.GetTimeDuration())
}

type LogicalSink struct {
	LogicalPlanBase
}

func NewLogicalSink(rt hybridqp.RowDataType, schema *QuerySchema) *LogicalSink {
	Sink := &LogicalSink{
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     rt,
			ops:    nil,
		},
	}

	return Sink
}

func (p *LogicalSink) Clone() hybridqp.QueryNode {
	clone := &LogicalSink{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalSink) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{}
}

func (p *LogicalSink) ReplaceChildren(children []hybridqp.QueryNode) {

}

func (p *LogicalSink) ReplaceChild(ordinal int, child hybridqp.QueryNode) {

}

func (p *LogicalSink) Explain(writer LogicalPlanWriter) {
	writer.Explain(p)
}

func (p *LogicalSink) String() string {
	return GetTypeName(p)
}

func (p *LogicalSink) Type() string {
	return GetType(p)
}

func (p *LogicalSink) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), 0)
}

func (p *LogicalSink) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalSink) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalSink) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalSink) Dummy() bool {
	return true
}

type LogicalMocSource struct {
	LogicalPlanBase
}

func NewLogicalMocSource(rt hybridqp.RowDataType) *LogicalMocSource {
	MocSource := &LogicalMocSource{
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: nil,
			rt:     rt,
			ops:    nil,
		},
	}

	return MocSource
}

func (p *LogicalMocSource) Clone() hybridqp.QueryNode {
	clone := &LogicalMocSource{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalMocSource) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{}
}

func (p *LogicalMocSource) ReplaceChildren(children []hybridqp.QueryNode) {

}

func (p *LogicalMocSource) ReplaceChild(ordinal int, child hybridqp.QueryNode) {

}

func (p *LogicalMocSource) Explain(writer LogicalPlanWriter) {
	writer.Explain(p)
}

func (p *LogicalMocSource) String() string {
	return GetTypeName(p)
}

func (p *LogicalMocSource) Type() string {
	return GetType(p)
}

func (p *LogicalMocSource) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), 0)
}

func (p *LogicalMocSource) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalMocSource) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalMocSource) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalMocSource) Dummy() bool {
	return true
}

func TestLogicalIndexScan(t *testing.T) {
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	schema := NewQuerySchema(nil, nil, &opt)
	seriesNode := NewLogicalSeries(schema)

	s := NewLogicalIndexScan(seriesNode, schema)
	strings.Contains(s.Digest(), "LogicalIndexScan")
	assert.Equal(t, len(s.Children()), 1)
	children := []hybridqp.QueryNode{nil}
	s.ReplaceChildren(children)
	assert.Equal(t, len(s.Children()), 1)
	s1 := s.Clone()
	s.ReplaceChild(0, seriesNode)
	if reflect.DeepEqual(s, s1) {
		panic("expected clone result wrong")
	}
	if !reflect.DeepEqual(schema, s.Schema()) {
		panic("wrong expected schema")
	}
	assert.Equal(t, len(s.RowDataType().Fields()), 0)
	assert.Equal(t, len(s.RowExprOptions()), 0)
	assert.Equal(t, s.Dummy(), false)
}

func TestNewStoreExecutorBuilder(t *testing.T) {
	m := make(map[uint64][][]interface{})
	m[1] = nil
	m[2] = nil
	traits := NewStoreExchangeTraits(nil, m)
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions:            []string{"host"},
		Ascending:             true,
		ChunkSize:             100,
		EnableBinaryTreeMerge: 1,
	}

	b := NewStoreExecutorBuilder(traits, opt.EnableBinaryTreeMerge)
	if b == nil {
		panic("nil StoreExecutorBuilder")
	}

	schema := NewQuerySchema(nil, nil, &opt)
	logicSeries := NewLogicalSeries(schema)
	if logicSeries == nil {
		panic("unexpected logicSeries vertex")
	}
	p, _ := b.Build(logicSeries)
	pipelineExecutor := p.(*PipelineExecutor)
	_ = pipelineExecutor
}

func TestNewScannerStoreExecutorBuilder(t *testing.T) {
	m := make(map[uint64][][]interface{})
	m[1] = nil
	m[2] = nil
	traits := NewStoreExchangeTraits(nil, m)
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions:            []string{"host"},
		Ascending:             true,
		ChunkSize:             100,
		EnableBinaryTreeMerge: 1,
	}
	req := &RemoteQuery{
		Opt: opt,
	}
	unrefs := []UnRefDbPt{
		{
			Db: "db0",
			Pt: uint32(1),
		},
		{
			Db: "db1",
			Pt: uint32(2),
		},
	}
	info := &IndexScanExtraInfo{
		ShardID: uint64(10),
		UnRefDbPt: UnRefDbPt{
			Db: "db0",
			Pt: uint32(1),
		},
		Req: req,
	}
	info_clone := info.Clone()
	if !reflect.DeepEqual(info, info_clone) {
		panic("unexpected clone result")
	}
	b := NewScannerStoreExecutorBuilder(traits, nil, req, nil, &unrefs)
	if b == nil {
		panic("nil StoreExecutorBuilder")
	}
	b.info = info_clone
	schema := NewQuerySchema(nil, nil, &opt)
	logicSeries1 := NewLogicalSeries(schema)
	node := NewLogicalIndexScan(logicSeries1, schema)
	if node == nil {
		panic("unexpected indexScan vertex")
	}
	p, _ := b.Build(node)
	pipelineExecutor := p.(*PipelineExecutor)
	_ = pipelineExecutor
}

func TestNewIndexScanTransform(t *testing.T) {
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions:            []string{"host"},
		Ascending:             true,
		ChunkSize:             100,
		EnableBinaryTreeMerge: 1,
	}
	req := &RemoteQuery{
		Opt: opt,
	}
	info := &IndexScanExtraInfo{
		ShardID: uint64(10),
		UnRefDbPt: UnRefDbPt{
			Db: "db0",
			Pt: uint32(1),
		},
		Req: req,
	}
	schema := NewQuerySchema(nil, nil, &opt)
	indexScan := NewIndexScanTransform(buildRowDataType(), nil, schema, nil, info)
	assert.Equal(t, "IndexScanTransform", indexScan.Name())
	assert.Equal(t, 1, len(indexScan.GetOutputs()))
	assert.Equal(t, 0, len(indexScan.GetInputs()))
	assert.Equal(t, 0, indexScan.GetInputNumber(nil))
	assert.Equal(t, 0, indexScan.GetOutputNumber(nil))
	indexScan.Release()
	indexScan.Close()
}

func TestGetInnerDimensions(t *testing.T) {
	out := []string{"A", "B", "D"}
	in := []string{"B", "C", "A"}
	result := getInnerDimensions(out, in)
	assert.Equal(t, []string{"A", "B", "D", "C"}, result)
}
