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

package executor_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
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
var PipelineExecutorGen func() *executor.PipelineExecutor
var exe *executor.PipelineExecutor

func init() {
	PipelineExecutorGen = func() *executor.PipelineExecutor {
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
		schema := executor.NewQuerySchema(nil, nil, &opt)

		source1 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk1})
		source2 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk2})
		source3 := NewSourceFromSingleChunk(buildRowDataType(), []executor.Chunk{chunk3})
		trans := executor.NewMergeTransform([]hybridqp.RowDataType{buildRowDataType(), buildRowDataType(), buildRowDataType()}, []hybridqp.RowDataType{buildRowDataType()}, nil, schema)

		sink := NewSinkFromFunction(buildRowDataType(), func(chunk executor.Chunk) error {
			return nil
		})

		executor.Connect(source1.Output, trans.Inputs[0])
		executor.Connect(source2.Output, trans.Inputs[1])
		executor.Connect(source3.Output, trans.Inputs[2])
		executor.Connect(trans.Outputs[0], sink.Input)

		m := executor.NewLogicalMerge([]hybridqp.QueryNode{NewLogicalMocSource(buildRowDataType()), NewLogicalMocSource(buildRowDataType()), NewLogicalMocSource(buildRowDataType())}, schema)
		var processors executor.Processors

		sinkVertex := executor.NewTransformVertex(NewLogicalSink(buildRowDataType(), schema), sink)
		source1Vertex := executor.NewTransformVertex(NewLogicalMocSource(buildRowDataType()), source1)
		source2Vertex := executor.NewTransformVertex(NewLogicalMocSource(buildRowDataType()), source2)
		source3Vertex := executor.NewTransformVertex(NewLogicalMocSource(buildRowDataType()), source3)
		mergeVertex := executor.NewTransformVertex(m, trans)

		dag := executor.NewTransformDag()
		sinkVertexInfo := executor.NewTransformVertexInfo()
		sinkVertexInfo.AddDirectEdge(executor.NewTransformEdge(mergeVertex, nil))
		mergeVertexInfo := executor.NewTransformVertexInfo()
		mergeVertexInfo.AddDirectEdge(executor.NewTransformEdge(source1Vertex, nil))
		mergeVertexInfo.AddDirectEdge(executor.NewTransformEdge(source2Vertex, nil))
		mergeVertexInfo.AddDirectEdge(executor.NewTransformEdge(source3Vertex, nil))
		dag.SetVertexToInfo(sinkVertex, sinkVertexInfo)
		dag.SetVertexToInfo(mergeVertex, mergeVertexInfo)

		processors = append(processors, source1)
		processors = append(processors, source2)
		processors = append(processors, source3)
		processors = append(processors, trans)
		processors = append(processors, sink)

		executor := executor.NewPipelineExecutorFromDag(dag, sinkVertex)
		executor.SetProcessors(processors)
		return executor
	}
	exe = PipelineExecutorGen()
}

func TestAppendRowValue(t *testing.T) {
	colFloat := executor.NewColumnImpl(influxql.Float)
	executor.AppendRowValue(colFloat, 7.77)
	colInt := executor.NewColumnImpl(influxql.Integer)
	executor.AppendRowValue(colInt, int64(7))
	colBool := executor.NewColumnImpl(influxql.Boolean)
	executor.AppendRowValue(colBool, true)
	colStr := executor.NewColumnImpl(influxql.String)
	executor.AppendRowValue(colStr, "test")
}

func ParseChunkTagsNew(kv [][]string) *executor.ChunkTags {
	var m influx.PointTags
	var ss []string
	for i := range kv {
		ss = append(ss, kv[i][0])
		if len(kv[i]) != 2 {
			continue
		}
		m = append(m, influx.Tag{Key: kv[i][0], Value: kv[i][1]})
	}
	sort.Sort(&m)
	return executor.NewChunkTags(m, ss)
}

func TestChunkTag(t *testing.T) {
	split := string(byte(0))
	executor.IgnoreEmptyTag = true
	s := [][]string{{"name", "martino"}, {"sex", ""}, {"country", "china"}, {"region"}}
	tag := ParseChunkTagsNew(s)
	subset := tag.Subset(nil)
	assert.Equal(t, record.Str2bytes("name"+split+"martino"+split+"sex"+split+split+"country"+split+"china"+split+split+split), subset)
	assert.Equal(t, []uint16{8, 5, 13, 17, 18, 26, 32, 33, 34}, tag.GetOffsets())

	for i := range s {
		if len(s[i]) != 2 {
			continue
		}
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
	assert.Equal(t, record.Str2bytes("name"+split+"martino"+split+"country"+split+"china"+split), newSubset)
	assert.Equal(t, []uint16{4, 5, 13, 21, 27}, newTag.GetOffsets())
}

func BuildChunk() executor.Chunk {
	rowDataType := buildRowDataType()

	b := executor.NewChunkBuilder(rowDataType)

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
	executor.GetPipelineExecutorResourceManager().Reset()
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
	if executor.GetPipelineExecutorResourceManager().GetMemBucket().GetFreeResource() != executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTotalResource() {
		t.Errorf("still has occupied memories")
	}
}

func TestMultiPipelineExecutors_MemSize(t *testing.T) {
	defer func() {
		mem, _ := memory.SysMem()
		executor.GetPipelineExecutorResourceManager().SetManagerParas(mem, time.Second)
	}()
	executor.GetPipelineExecutorResourceManager().Reset()
	executor.GetPipelineExecutorResourceManager().SetManagerParas(30000, time.Second)
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
	if executor.GetPipelineExecutorResourceManager().GetMemBucket().GetFreeResource() != executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTotalResource() {
		t.Errorf("still has occupied memories")
	}
}

func TestMultiPipelineExecutors_ALLTimeout(t *testing.T) {
	defer func() {
		mem, _ := memory.SysMem()
		executor.GetPipelineExecutorResourceManager().SetManagerParas(mem, time.Second)
	}()
	executor.GetPipelineExecutorResourceManager().Reset()
	executor.GetPipelineExecutorResourceManager().SetManagerParas(2000, time.Second)
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
	if executor.GetPipelineExecutorResourceManager().GetMemBucket().GetFreeResource() != executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTotalResource() {
		t.Errorf("still has occupied memories")
	}

	mem, _ := memory.SysMem()
	executor.GetPipelineExecutorResourceManager().SetManagerParas(mem, time.Second)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go SimpleMergeDAG()
	}

	wg.Wait()
	if executor.GetPipelineExecutorResourceManager().GetMemBucket().GetFreeResource() != executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTotalResource() {
		t.Errorf("still has occupied memories")
	}
}

func TestInitMstName(t *testing.T) {
	heap := &executor.AppendHeapItems{}
	assert.Equal(t, "", executor.InitMstName(heap))
	heap.Items = append(heap.Items, &executor.Item{
		ChunkBuf: BuildSQLChunk1(),
	})
	heap.Items = append(heap.Items, &executor.Item{
		ChunkBuf: BuildSQLChunk1(),
	})
	assert.Equal(t, "cpu", executor.InitMstName(heap))
	heap.Items = append(heap.Items, &executor.Item{
		ChunkBuf: BuildSQLChunk5(),
	})
	assert.Equal(t, "cpu,mst1", executor.InitMstName(heap))
}

func TestSortedAppendInit_Empty(t *testing.T) {
	h := &executor.AppendHeapItems{}
	assert.Equal(t, "", executor.InitMstName(h))
}

func TestSetPipelineExecutorResourceManagerParas(t *testing.T) {
	defer func() {
		mem, _ := memory.SysMem()
		executor.GetPipelineExecutorResourceManager().SetManagerParas(mem, time.Second)
	}()
	executor.GetPipelineExecutorResourceManager().SetManagerParas(0, 0)
	executor.SetPipelineExecutorResourceManagerParas(1000, time.Second)
	assert.Equal(t, int64(1000), executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTotalResource())
	assert.Equal(t, time.Second, executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTimeDuration())
	executor.SetPipelineExecutorResourceManagerParas(2000, 2*time.Second)
	assert.Equal(t, int64(1000), executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTotalResource())
	assert.Equal(t, time.Second, executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTimeDuration())
	executor.SetPipelineExecutorResourceManagerParas(500, time.Second/10)
	assert.Equal(t, int64(500), executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTotalResource())
	assert.Equal(t, time.Second/10, executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTimeDuration())
}

type LogicalSink struct {
	executor.LogicalPlanBase
}

func NewLogicalSink(rt hybridqp.RowDataType, schema *executor.QuerySchema) *LogicalSink {
	Sink := &LogicalSink{
		LogicalPlanBase: *executor.NewLogicalPlanBase(schema, rt, nil),
	}

	return Sink
}

func (p *LogicalSink) Clone() hybridqp.QueryNode {
	schema, _ := p.Schema().(*executor.QuerySchema)
	clone := NewLogicalSink(p.RowDataType(), schema)
	return clone
}

func (p *LogicalSink) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{}
}

func (p *LogicalSink) ReplaceChildren(children []hybridqp.QueryNode) {

}

func (p *LogicalSink) ReplaceChild(ordinal int, child hybridqp.QueryNode) {

}

func (p *LogicalSink) Explain(writer executor.LogicalPlanWriter) {
	writer.Explain(p)
}

func (p *LogicalSink) String() string {
	return executor.GetTypeName(p)
}

func (p *LogicalSink) Type() string {
	return executor.GetType(p)
}

func (p *LogicalSink) Digest() string {
	return fmt.Sprintf("%s[%d]", executor.GetTypeName(p), 0)
}

func (p *LogicalSink) Dummy() bool {
	return true
}

type LogicalMocSource struct {
	executor.LogicalPlanBase
}

func NewLogicalMocSource(rt hybridqp.RowDataType) *LogicalMocSource {
	MocSource := &LogicalMocSource{
		LogicalPlanBase: *executor.NewLogicalPlanBase(nil, rt, nil),
	}

	return MocSource
}

func (p *LogicalMocSource) Clone() hybridqp.QueryNode {
	clone := NewLogicalMocSource(p.RowDataType())
	return clone
}

func (p *LogicalMocSource) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{}
}

func (p *LogicalMocSource) ReplaceChildren(children []hybridqp.QueryNode) {

}

func (p *LogicalMocSource) ReplaceChild(ordinal int, child hybridqp.QueryNode) {

}

func (p *LogicalMocSource) Explain(writer executor.LogicalPlanWriter) {
	writer.Explain(p)
}

func (p *LogicalMocSource) String() string {
	return executor.GetTypeName(p)
}

func (p *LogicalMocSource) Type() string {
	return executor.GetType(p)
}

func (p *LogicalMocSource) Digest() string {
	return fmt.Sprintf("%s[%d]", executor.GetTypeName(p), 0)
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
	schema := executor.NewQuerySchema(nil, nil, &opt)
	seriesNode := executor.NewLogicalSeries(schema)

	s := executor.NewLogicalIndexScan(seriesNode, schema)
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
	traits := executor.NewStoreExchangeTraits(nil, m)
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions:            []string{"host"},
		Ascending:             true,
		ChunkSize:             100,
		EnableBinaryTreeMerge: 1,
	}

	b := executor.NewStoreExecutorBuilder(traits, opt.EnableBinaryTreeMerge)
	if b == nil {
		panic("nil StoreExecutorBuilder")
	}

	schema := executor.NewQuerySchema(nil, nil, &opt)
	logicSeries := executor.NewLogicalSeries(schema)
	if logicSeries == nil {
		panic("unexpected logicSeries vertex")
	}
	p, _ := b.Build(logicSeries)
	pipelineExecutor := p.(*executor.PipelineExecutor)
	_ = pipelineExecutor
}

func TestNewScannerStoreExecutorBuilder(t *testing.T) {
	t.Skip("occasional failure")
	m := make(map[uint64][][]interface{})
	m[1] = nil
	m[2] = nil
	traits := executor.NewStoreExchangeTraits(nil, m)
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions:            []string{"host"},
		Ascending:             true,
		ChunkSize:             100,
		EnableBinaryTreeMerge: 1,
	}
	req := &executor.RemoteQuery{
		Opt:      opt,
		ShardIDs: []uint64{1, 2},
	}
	unrefs := []executor.UnRefDbPt{
		{
			Db: "db0",
			Pt: uint32(1),
		},
		{
			Db: "db1",
			Pt: uint32(2),
		},
	}
	info := &executor.IndexScanExtraInfo{
		ShardID: uint64(10),
		UnRefDbPt: executor.UnRefDbPt{
			Db: "db0",
			Pt: uint32(1),
		},
		Req: req,
	}
	info_clone := info.Clone()
	if !reflect.DeepEqual(info, info_clone) {
		panic("unexpected clone result")
	}

	b := executor.NewScannerStoreExecutorBuilder(traits, nil, req, nil, &unrefs, 1)
	b = executor.NewScannerStoreExecutorBuilder(traits, nil, req, nil, &unrefs, 2)
	if b == nil {
		panic("nil StoreExecutorBuilder")
	}
	b.SetInfo(info_clone)
	schema := executor.NewQuerySchema(nil, nil, &opt)
	logicSeries1 := executor.NewLogicalSeries(schema)
	node := executor.NewLogicalIndexScan(logicSeries1, schema)
	if node == nil {
		panic("unexpected indexScan vertex")
	}
	p, _ := b.Build(node)
	pipelineExecutor := p.(*executor.PipelineExecutor)
	go func() {
		pipelineExecutor.Abort()
	}()

	pipelineExecutor.Execute(context.Background())
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
	req := &executor.RemoteQuery{
		Opt: opt,
	}
	info := &executor.IndexScanExtraInfo{
		ShardID: uint64(10),
		UnRefDbPt: executor.UnRefDbPt{
			Db: "db0",
			Pt: uint32(1),
		},
		Req: req,
	}
	schema := executor.NewQuerySchema(nil, nil, &opt)
	indexScan := executor.NewIndexScanTransform(buildRowDataType(), nil, schema, nil, info, make(chan struct{}, 2))
	assert.Equal(t, "IndexScanTransform", indexScan.Name())
	assert.Equal(t, 1, len(indexScan.GetOutputs()))
	assert.Equal(t, 1, len(indexScan.GetInputs()))
	assert.Equal(t, 0, indexScan.GetInputNumber(nil))
	assert.Equal(t, 0, indexScan.GetOutputNumber(nil))
	indexScan.Release()
	indexScan.Close()
}

func TestGetInnerDimensions(t *testing.T) {
	out := []string{"A", "B", "D"}
	in := []string{"B", "C", "A"}
	result := executor.GetInnerDimensions(out, in)
	assert.Equal(t, []string{"A", "B", "D", "C"}, result)
}
