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
	"math"
	"testing"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/require"
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

	trans := executor.NewIndexScanTransform(outputRowDataType, nil, schema, nil, nil, make(chan struct{}, 1))
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
		nil, &executor.IndexScanExtraInfo{}, make(chan struct{}, 1))

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
