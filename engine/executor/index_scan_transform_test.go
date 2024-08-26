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

package executor_test

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func init() {
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0)
}

type cursor struct {
	closeErr bool
}

func (c *cursor) StartSpan(span *tracing.Span) {
}

func (c *cursor) EndSpan() {
}

func (c *cursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

func (c *cursor) SetOps(ops []*comm.CallOption) {
	return
}

func (c *cursor) SinkPlan(plan hybridqp.QueryNode) {}

func (c *cursor) GetSchema() record.Schemas {
	return nil
}

func (c *cursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	return nil, nil, nil
}

func (c *cursor) Name() string {
	return ""
}

func (c *cursor) Close() error {
	if !c.closeErr {
		return nil
	}
	return fmt.Errorf("cursor close err")
}

func buildIRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
	)
	return rowDataType
}

func BuildIChunk(name string) executor.Chunk {
	rowDataType := buildInRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk(name)
	chunk.AppendTimes([]int64{1, 2, 3})
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val"), 0)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func buildISchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		Sources:     []influxql.Source{&influxql.Measurement{Name: "mst", EngineType: config.TSSTORE}},
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func TestIndexScanTransformDemo(t *testing.T) {
	chunk1 := BuildIChunk("m")
	ctx := context.Background()
	outputRowDataType := buildIRowDataType()
	schema := buildISchema()

	trans := executor.NewIndexScanTransform(outputRowDataType, nil, schema, nil, nil, make(chan struct{}, 1), 0, false)
	sink := NewSinkFromFunction(outputRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var process []executor.Processor
	process = append(process, sink)
	process = append(process, trans)
	exec := executor.NewPipelineExecutor(process)
	// build child pipeline executor
	childTrans := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	var childProcess []executor.Processor
	childProcess = append(childProcess, childTrans)
	childExec := executor.NewPipelineExecutor(childProcess)
	childDag := executor.NewTransformDag()
	inputQn := executor.NewLogicalSeries(schema)
	qn := executor.NewLogicalIndexScan(inputQn, schema)
	root := executor.NewTransformVertex(qn, childTrans)
	childDag.AddVertex(root)
	childExec.SetRoot(root)
	childExec.SetDag(childDag)
	trans.GetResFromAllocator()
	trans.FreeResFromAllocator()
	trans.SetPipelineExecutor(childExec)
	// execute
	trans.Work(ctx)
	childExec.Release()
	exec.Release()
}

func TestIndexScanTransform_Abort(t *testing.T) {
	outputRowDataType := buildIRowDataType()
	schema := buildISchema()

	trans := executor.NewIndexScanTransform(outputRowDataType, nil, schema,
		nil, &executor.IndexScanExtraInfo{}, make(chan struct{}, 1), 0, false)

	trans.Abort()
	trans.Abort()
	err := trans.Work(context.Background())
	require.NoError(t, err)
}

func TestIndexScanCursorClose(t *testing.T) {
	cursors := comm.KeyCursors{&cursor{}, &cursor{closeErr: true}}
	keyCursors := make([][]interface{}, 1)
	keyCursors[0] = make([]interface{}, 2)
	keyCursors[0][0] = cursors[0]
	keyCursors[0][1] = cursors[1]
	plan := executor.NewLogicalDummyShard(keyCursors)
	trans1 := &executor.IndexScanTransform{}
	trans1.CursorsClose(plan)
	trans1.SetIndexScanErr(true)
	trans1.CursorsClose(plan)
	trans1.CursorsClose(nil)
}

func buildColStoreSchema(initSKIndex bool) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		Sources:     []influxql.Source{&influxql.Measurement{Name: "m", EngineType: config.COLUMNSTORE}},
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	if initSKIndex {
		opt.Sources[0].(*influxql.Measurement).IndexRelation = &influxql.IndexRelation{Oids: []uint32{uint32(index.BloomFilter)}}
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildTsStoreSchema(initSKIndex bool) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		Sources:     []influxql.Source{&influxql.Measurement{Name: "m", EngineType: config.TSSTORE}},
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	if initSKIndex {
		opt.Sources[0].(*influxql.Measurement).IndexRelation = &influxql.IndexRelation{Oids: []uint32{uint32(index.BloomFilter)}}
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func testIndexScanTransform_SparseIndex(t *testing.T, index *executor.LogicalIndexScan, info *executor.IndexScanExtraInfo) {
	// init the chunk and schema
	ctx := context.Background()
	chunk1 := buildInputChunk("m")
	outputRowDataType := buildInputRowDataType()
	if info == nil {
		info = buildIndexScanExtraInfo()
	}

	// build the pipeline executor
	var process []executor.Processor
	logger.InitLogger(config.Logger{Level: zap.DebugLevel})
	trans := executor.NewIndexScanTransform(outputRowDataType, index.RowExprOptions(), index.Schema(), index.Children()[0], info, make(chan struct{}, 1), 0, false)
	source := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	sink := NewSinkFromFunction(outputRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	if err := executor.Connect(source.GetOutputs()[0], trans.GetInputs()[0]); err != nil {
		t.Fatal(err)
	}
	if err := executor.Connect(trans.GetOutputs()[0], sink.Input); err != nil {
		t.Fatal(err)
	}
	process = append(process, source)
	process = append(process, trans)
	process = append(process, sink)
	exec := executor.NewPipelineExecutor(process)

	// build the sub pipeline executor
	var childProcess []executor.Processor
	childSource := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	childSink := NewSinkFromFunction(outputRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	childProcess = append(childProcess, childSource)
	childProcess = append(childProcess, childSink)
	childExec := executor.NewPipelineExecutor(childProcess)
	childDag := executor.NewTransformDag()
	root := executor.NewTransformVertex(index, childSink)
	childDag.AddVertex(root)
	childExec.SetRoot(root)
	childExec.SetDag(childDag)

	var wg sync.WaitGroup
	// make the pipeline executor work
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := trans.Work(ctx); err != nil {
			t.Error(err)
			return
		}
	}()
	wg.Wait()
	exec.Release()
	childExec.Release()
	trans.Close()
}
func testIndexScanTransformTsIndex(t *testing.T, index *executor.LogicalIndexScan, info *executor.IndexScanExtraInfo) {
	// init the chunk and schema
	ctx := context.WithValue(context.Background(), query.IndexScanDagStartTimeKey, time.Now())
	chunk1 := buildInputChunk("m")
	outputRowDataType := buildInputRowDataType()

	// build the pipeline executor
	var process []executor.Processor
	logger.InitLogger(config.Logger{Level: zap.DebugLevel})
	trans := executor.NewIndexScanTransform(outputRowDataType, index.RowExprOptions(), index.Schema(), index.Children()[0], info, make(chan struct{}, 1), 0, false)
	source := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	sink := NewSinkFromFunction(outputRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	if err := executor.Connect(source.GetOutputs()[0], trans.GetInputs()[0]); err != nil {
		t.Fatal(err)
	}
	if err := executor.Connect(trans.GetOutputs()[0], sink.Input); err != nil {
		t.Fatal(err)
	}
	process = append(process, source)
	process = append(process, trans)
	process = append(process, sink)
	exec := executor.NewPipelineExecutor(process)

	// build the sub pipeline executor
	var childProcess []executor.Processor
	childSource := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	childSink := NewSinkFromFunction(outputRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	childProcess = append(childProcess, childSource)
	childProcess = append(childProcess, childSink)
	childExec := executor.NewPipelineExecutor(childProcess)
	childDag := executor.NewTransformDag()
	root := executor.NewTransformVertex(index, childSink)
	childDag.AddVertex(root)
	childExec.SetRoot(root)
	childExec.SetDag(childDag)

	var wg sync.WaitGroup
	// make the pipeline executor work
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := trans.Work(ctx); err != nil {
			t.Error(err)
			return
		}
	}()
	wg.Wait()
	exec.Release()
	childExec.Release()
	trans.Close()
}

func TestIndexScanTransform_SparseIndex(t *testing.T) {
	// build the logical plan
	schema := buildColStoreSchema(false)
	reader := executor.NewLogicalColumnStoreReader(nil, schema)
	index := executor.NewLogicalIndexScan(reader, schema)
	testIndexScanTransform_SparseIndex(t, index, nil)
}

func TestIndexScanTransform_SparseIndex_HashAgg_Unknown(t *testing.T) {
	// build the logical plan
	schema := buildColStoreSchema(true)
	reader := executor.NewLogicalColumnStoreReader(nil, schema)
	agg := executor.NewLogicalHashAgg(reader, schema, executor.UNKNOWN_EXCHANGE, nil)
	index := executor.NewLogicalIndexScan(agg, schema)
	testIndexScanTransform_SparseIndex(t, index, nil)
}

func TestIndexScanTransform_SparseIndex_HashAgg(t *testing.T) {
	// build the logical plan
	schema := buildColStoreSchema(true)
	reader := executor.NewLogicalColumnStoreReader(nil, schema)
	agg := executor.NewLogicalHashAgg(reader, schema, executor.READER_EXCHANGE, nil)
	index := executor.NewLogicalIndexScan(agg, schema)
	testIndexScanTransform_SparseIndex(t, index, nil)
}

func TestIndexScanTransform_SparseIndex_HashMerge(t *testing.T) {
	// build the logical plan
	schema := buildColStoreSchema(true)
	reader := executor.NewLogicalColumnStoreReader(nil, schema)
	merge := executor.NewLogicalHashMerge(reader, schema, executor.READER_EXCHANGE, nil)
	index := executor.NewLogicalIndexScan(merge, schema)
	testIndexScanTransform_SparseIndex(t, index, nil)
}

func TestIndexScanTransform_PtQuerys_HashMerge(t *testing.T) {
	// build the logical plan
	schema := buildColStoreSchema(true)
	reader := executor.NewLogicalColumnStoreReader(nil, schema)
	merge := executor.NewLogicalHashMerge(reader, schema, executor.READER_EXCHANGE, nil)
	index := executor.NewLogicalIndexScan(merge, schema)
	info := buildIndexScanExtraInfoForPtQuerys()
	testIndexScanTransform_SparseIndex(t, index, info)
}

func TestIndexScanTransform(t *testing.T) {
	// build the logical plan
	schema := buildTsStoreSchema(true)
	reader := executor.NewLogicalReader(nil, schema)
	index := executor.NewLogicalIndexScan(reader, schema)
	info := buildIndexScanExtraInfo1()
	testIndexScanTransformTsIndex(t, index, info)
}
