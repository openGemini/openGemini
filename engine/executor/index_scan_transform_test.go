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
	"math"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

func init() {
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0)
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
	chunk.AppendTime([]int64{1, 2, 3}...)
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val"), 0)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3}...)
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3}...)
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

func TestIndexScanTransformLowerDAGResourceLack(t *testing.T) {
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

	beforeTotalResource := executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTotalResource()
	beforeTimer := executor.GetPipelineExecutorResourceManager().GetMemBucket().GetTimeDuration()

	executor.GetPipelineExecutorResourceManager().SetManagerParas(1, 1000000000) // 1s
	// execute
	if err := trans.WorkHelper(ctx); !errno.Equal(err, errno.BucketLacks) {
		t.Error("IndexScanTransformLowerDAGResourceLack test error")
	}
	executor.GetPipelineExecutorResourceManager().SetManagerParas(beforeTotalResource, beforeTimer)
	childExec.Release()
	exec.Release()
}
