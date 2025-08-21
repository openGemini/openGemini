/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func buildOrderByTransformRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value", Type: influxql.Float},
	)
	return rowDataType
}

func buildOrderByTransformInChunk() executor.Chunk {
	rowDataType := buildOrderByTransformRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("tag1")
	chunk.AppendTimes([]int64{1, 3, 4})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=tag1val,tag2=tag2val"), 0)
	chunk.AppendIntervalIndex(0)
	AppendFloatValues(chunk, 0, []float64{1.1, 3.3, 4.4}, []bool{true, true, true})
	return chunk
}

func TestOrderByTransformInitPromDims1(t *testing.T) {
	inRowDataType := buildOrderByTransformRowDataType()
	outRowDataType := inRowDataType
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "val0", Type: influxql.Float},
		},
	}
	chunk := buildOrderByTransformInChunk()
	opt1 := query.ProcessorOptions{
		PromQuery:      true,
		GroupByAllDims: true,
	}
	trans1 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt1, nil)
	trans1.GetTagAndIndexes(chunk)
	assert.Equal(t, trans1.GetCurrTags(0), "\x04\x00\x05\x00\r\x00\x12\x00\x1a\x00tag1\x00tag1val\x00tag2\x00tag2val\x00")
	opt2 := query.ProcessorOptions{
		PromQuery:  true,
		Without:    true,
		Dimensions: []string{"tag1"},
	}
	trans2 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt2, nil)
	trans2.GetTagAndIndexes(chunk)
	assert.Equal(t, trans2.GetCurrTags(0), "\x02\x00\x05\x00\r\x00tag2\x00tag2val\x00")
	opt3 := query.ProcessorOptions{
		PromQuery:  true,
		Dimensions: []string{"tag1"},
	}
	trans3 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt3, opt3.Dimensions)
	trans3.GetTagAndIndexes(chunk)
	assert.Equal(t, trans3.GetCurrTags(0), "\x02\x00\x05\x00\r\x00tag1\x00tag1val\x00")
}

func TestOrderByTransformInitPromDims2(t *testing.T) {
	inRowDataType := buildOrderByTransformRowDataType()
	outRowDataType := inRowDataType
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "val0", Type: influxql.Float},
		},
	}
	chunk := buildOrderByTransformInChunk()
	opt1 := query.ProcessorOptions{
		PromQuery:      true,
		GroupByAllDims: true,
	}
	trans1 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt1, nil)
	trans1.GetTagsResetTagIndexes(chunk)
	assert.Equal(t, trans1.GetCurrTags(0), "\x04\x00\x05\x00\r\x00\x12\x00\x1a\x00tag1\x00tag1val\x00tag2\x00tag2val\x00")
	opt2 := query.ProcessorOptions{
		PromQuery:  true,
		Without:    true,
		Dimensions: []string{"tag1"},
	}
	trans2 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt2, nil)
	trans2.GetTagsResetTagIndexes(chunk)
	assert.Equal(t, trans2.GetCurrTags(0), "\x02\x00\x05\x00\r\x00tag2\x00tag2val\x00")
	opt3 := query.ProcessorOptions{
		PromQuery:  true,
		Dimensions: []string{"tag1"},
	}
	trans3 := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt3, opt3.Dimensions)
	trans3.GetTagsResetTagIndexes(chunk)
	assert.Equal(t, trans3.GetCurrTags(0), "\x02\x00\x05\x00\r\x00tag1\x00tag1val\x00")
}

func TestOrderByTransformWithSameDims(t *testing.T) {
	inRowDataType := buildOrderByTransformRowDataType()
	outRowDataType := inRowDataType
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{Val: "val0", Type: influxql.Float},
			Ref:  influxql.VarRef{Val: "val0", Type: influxql.Float},
		},
	}
	opt1 := query.ProcessorOptions{
		IsSameDims: true,
	}
	chunk := buildOrderByTransformInChunk()

	// 1. build source, orderBy and sink transform.
	source := NewSourceFromMultiChunk(inRowDataType, []executor.Chunk{chunk})
	trans := executor.NewOrderByTransform(inRowDataType, outRowDataType, exprOpt, &opt1, nil)
	var outChunks []executor.Chunk
	sink := NewSinkFromFunction(outRowDataType, func(chunk executor.Chunk) error {
		outChunks = append(outChunks, chunk.Clone())
		return nil
	})

	// 2. connect all transforms and build the pipeline executor
	assert.NoError(t, executor.Connect(source.Output, trans.GetInputs()[0]))
	assert.NoError(t, executor.Connect(trans.GetOutputs()[0], sink.Input))
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)

	// 3. run the pipeline executor
	err := executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("connect error")
	}
	executors.Release()

	// 4. check the result
	dstChunks := []executor.Chunk{chunk}
	if len(dstChunks) != len(outChunks) {
		t.Fatalf("the chunk number is not the same as the target: %d != %d\n", len(dstChunks), len(outChunks))
	}
	for i := range outChunks {
		assert.Equal(t, outChunks[i].Name(), dstChunks[i].Name())
		assert.Equal(t, outChunks[i].Tags(), dstChunks[i].Tags())
		assert.Equal(t, outChunks[i].Time(), dstChunks[i].Time())
		assert.Equal(t, outChunks[i].TagIndex(), dstChunks[i].TagIndex())
		assert.Equal(t, outChunks[i].IntervalIndex(), dstChunks[i].IntervalIndex())
		for j := range outChunks[i].Columns() {
			assert.Equal(t, outChunks[i].Column(j), dstChunks[i].Column(j))
		}
	}
}
