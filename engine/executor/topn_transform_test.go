// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"bytes"
	"context"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func TopNCompareMultiCol(c1 executor.Chunk, c2 executor.Chunk, t *testing.T) {
	if c1 == nil && c2 == nil {
		return
	}
	assert.Equal(t, c1.TagLen(), c2.TagLen())
	for i, tag1s := range c1.Tags() {
		tag2s := c2.Tags()[i]
		assert.Equal(t, tag1s.GetTag(), tag2s.GetTag())
	}
	assert.Equal(t, c1.Time(), c2.Time())
	assert.Equal(t, len(c1.Columns()), len(c2.Columns()))
	for i := range c1.Columns() {
		assert.Equal(t, c1.Columns()[i].IntegerValues(), c2.Columns()[i].IntegerValues())
	}
}

func buildTopNRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Integer},
	)
	return rowDataType
}

func BuildTopNInChunk1() executor.Chunk {
	rowDataType := buildTopNRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3})
	chunk.NewDims(1)
	chunk.AddDims([]string{"tv1"})
	chunk.AddDims([]string{"tv2"})
	chunk.AddDims([]string{"tv1"})
	for _, col := range chunk.Columns() {
		switch col.DataType() {
		case influxql.Integer:
			col.AppendIntegerValues([]int64{1, 2, 3})
		case influxql.Float:
			col.AppendFloatValues([]float64{1.1, 2.2, 3.3})
		case influxql.Boolean:
			col.AppendBooleanValues([]bool{true, false, true})
		case influxql.String:
			col.AppendStringValues([]string{"a", "b", "c"})
		default:
			panic("datatype error")
		}
		col.AppendManyNotNil(3)
		col.AppendManyNil(1)
	}
	return chunk
}

func BuildTopNInChunk2() executor.Chunk {
	rowDataType := buildTopNRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3})
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=tv1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=tv2"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tk1=tv3"), 2)
	for _, col := range chunk.Columns() {
		switch col.DataType() {
		case influxql.Integer:
			col.AppendIntegerValues([]int64{1, 2, 3})
		case influxql.Float:
			col.AppendFloatValues([]float64{1.1, 2.2, 3.3})
		case influxql.Boolean:
			col.AppendBooleanValues([]bool{true, false, true})
		case influxql.String:
			col.AppendStringValues([]string{"a", "b", "c"})
		default:
			panic("datatype error")
		}
		col.AppendManyNotNil(3)
		col.AppendManyNil(1)
	}
	return chunk
}

func BuildTopNResult1() executor.Chunk {
	rowDataType := buildTopNRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("m1")
	chunk1.AppendTimes([]int64{0, 0})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=tv1"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=tv2"), 1)
	AppendIntegerValues(chunk1, 0, []int64{2, 1}, []bool{true, true})
	return chunk1
}

func BuildTopNResult2() executor.Chunk {
	rowDataType := buildTopNRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk1 := b.NewChunk("m1")
	chunk1.AppendTimes([]int64{0, 0, 0})
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=tv3"), 0)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=tv2"), 1)
	chunk1.AddTagAndIndex(*ParseChunkTags("tk1=tv1"), 2)
	AppendIntegerValues(chunk1, 0, []int64{3, 2, 1}, []bool{true, true, true})
	return chunk1
}

func builTopNSchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:  1000,
		RowsChan:   outPutRowsChan,
		Ascending:  true,
		Dimensions: []string{"tk1"},
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func TestTopnTransform1(t *testing.T) {
	chunk1 := BuildTopNInChunk1()
	expResult := BuildTopNResult1()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildTopNRowDataType()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := builTopNSchema()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "topn_ddcm", Args: []influxql.Expr{&influxql.VarRef{Val: "f1", Type: influxql.Integer},
				&influxql.NumberLiteral{Val: 0.000003}, &influxql.IntegerLiteral{Val: 12}, &influxql.StringLiteral{Val: "count"}}},
			Ref: influxql.VarRef{Val: outRowDataType.Field(0).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(0).Expr.(*influxql.VarRef).Type},
		},
	}
	trans, err := executor.NewTopNTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic(err.Error())
	}
	checkResult := func(chunk executor.Chunk) error {
		TopNCompareMultiCol(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestTopnTransform2(t *testing.T) {
	chunk1 := BuildTopNInChunk2()
	expResult := BuildTopNResult2()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildTopNRowDataType()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := builTopNSchema()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "topn_ddcm", Args: []influxql.Expr{&influxql.VarRef{Val: "f1", Type: influxql.Integer},
				&influxql.NumberLiteral{Val: 0.000003}, &influxql.IntegerLiteral{Val: 12}, &influxql.StringLiteral{Val: "sum"}}},
			Ref: influxql.VarRef{Val: outRowDataType.Field(0).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(0).Expr.(*influxql.VarRef).Type},
		},
	}
	trans, err := executor.NewTopNTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic(err.Error())
	}
	checkResult := func(chunk executor.Chunk) error {
		TopNCompareMultiCol(chunk, expResult, t)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func buildBenchmarkTopNSchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:  1000,
		RowsChan:   outPutRowsChan,
		Ascending:  true,
		Fill:       influxql.NumberFill,
		FillValue:  99,
		Dimensions: []string{"host"},
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildBenchmarkTopNDimsSchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:  1000,
		RowsChan:   outPutRowsChan,
		Ascending:  true,
		Dimensions: []string{"host"},
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildBenchmarkTopNDimsSchemaCompareHashAgg() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:  1000,
		RowsChan:   outPutRowsChan,
		Ascending:  true,
		Dimensions: []string{"host"},
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildChunks(startChunkLoc, chunkN, countPerGroup int, chunkList *[]executor.Chunk, b *executor.ChunkBuilder, val int64) {
	for i := startChunkLoc; i < chunkN; i++ {
		chunk := b.NewChunk("mst")
		tags := make([]executor.ChunkTags, 0)
		tagIndex := make([]int, 0)
		tagPerChunk := 1000 / countPerGroup
		for t := 0; t < tagPerChunk; t++ {
			var buffer bytes.Buffer
			buffer.WriteString("host")
			buffer.WriteString("=")
			buffer.WriteString(strconv.Itoa(t + i*tagPerChunk))
			tags = append(tags, *ParseChunkTags(buffer.String()))
			tagIndex = append(tagIndex, t*countPerGroup)
		}
		chunk.AppendTagsAndIndexes(tags, tagIndex)
		times := make([]int64, 0, 1000)
		for j := 0; j < tagPerChunk; j++ {
			for k := 0; k < countPerGroup; k++ {
				times = append(times, int64(k))
				chunk.Column(0).AppendIntegerValue(val)
				chunk.Column(0).AppendNotNil()
			}
		}
		chunk.AppendTimes(times)
		*chunkList = append(*chunkList, chunk)
	}
}

func buildBenchChunksTopnTransform(largeGroupN int, smallGroupN int) []executor.Chunk {
	countPerSmallGroup := 1
	countPerLargeGroup := 10
	rowDataType := buildBenchRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	largeGroupChunkN := (largeGroupN * countPerLargeGroup) / 1000
	smallGroupChunkN := (smallGroupN * countPerSmallGroup) / 1000
	chunkList := make([]executor.Chunk, 0, largeGroupChunkN+smallGroupChunkN)
	buildChunks(0, largeGroupChunkN, countPerLargeGroup, &chunkList, b, 10)
	buildChunks(largeGroupChunkN, smallGroupChunkN+largeGroupChunkN, countPerSmallGroup, &chunkList, b, 1)
	return chunkList
}

func buildDimChunks(startChunkLoc, chunkN, countPerGroup int, chunkList *[]executor.Chunk, b *executor.ChunkBuilder, val int64) {
	for i := startChunkLoc; i < chunkN; i++ {
		tmpCountPerGroup := countPerGroup
		chunk := b.NewChunk("mst")
		chunk.NewDims(1)
		chunk.AppendTagsAndIndexes([]executor.ChunkTags{}, []int{0})
		tagPerChunk := 1000 / countPerGroup
		if tagPerChunk == 0 {
			tagPerChunk = 1
			tmpCountPerGroup = 1000
		}
		times := make([]int64, 0, 1000)
		for t := 0; t < tagPerChunk; t++ {
			for k := 0; k < tmpCountPerGroup; k++ {
				chunk.AddDims([]string{strconv.Itoa(t + (i * 1000 / countPerGroup))})
				times = append(times, int64(k))
				chunk.Column(0).AppendIntegerValue(val)
				chunk.Column(0).AppendNotNil()
			}
		}
		chunk.AppendTimes(times)
		*chunkList = append(*chunkList, chunk)
	}
}

func buildBenchDimChunksTopnTransform(largeGroupN int, smallGroupN int) []executor.Chunk {
	countPerSmallGroup := 1
	countPerLargeGroup := 100
	rowDataType := buildBenchRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	largeGroupChunkN := (largeGroupN * countPerLargeGroup) / 1000
	smallGroupChunkN := (smallGroupN * countPerSmallGroup) / 1000
	chunkList := make([]executor.Chunk, 0, largeGroupChunkN+smallGroupChunkN)
	//buildDimChunks(0, largeGroupChunkN, countPerLargeGroup, &chunkList, b, 10)
	//buildDimChunks(largeGroupChunkN, smallGroupChunkN+largeGroupChunkN, countPerSmallGroup, &chunkList, b, 1)
	buildDimChunks(0, smallGroupChunkN, countPerSmallGroup, &chunkList, b, 1)
	buildDimChunks(smallGroupChunkN, smallGroupChunkN+largeGroupChunkN, countPerLargeGroup, &chunkList, b, 10)
	return chunkList
}

func BenchmarkTopnTransform1(b *testing.B) {
	srcRowDataType := buildTopNRowDataType()
	dstRowDataType := buildTopNRowDataType()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "topn", Args: []influxql.Expr{&influxql.VarRef{Val: "f1", Type: influxql.Integer},
				&influxql.NumberLiteral{Val: 0.000003}, &influxql.IntegerLiteral{Val: 12}, &influxql.StringLiteral{Val: "sum"}}},
			Ref: influxql.VarRef{Val: `value`, Type: influxql.Integer},
		},
	}
	chunks := buildBenchChunksTopnTransform(100, 1000)
	schema := buildBenchmarkTopNSchema()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewTopNTransform([]hybridqp.RowDataType{srcRowDataType}, []hybridqp.RowDataType{dstRowDataType}, exprOpt, schema, executor.Fill)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.GetInputs()[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.GetOutputs()[0], sink.Input)
		if err != nil {
			b.Fatalf("connect error")
		}
		var processors executor.Processors
		processors = append(processors, source)
		processors = append(processors, trans1)
		processors = append(processors, sink)
		executors := executor.NewPipelineExecutor(processors)

		b.StartTimer()
		err = executors.Execute(context.Background())
		if err != nil {
			b.Fatalf("connect error")
		}
		b.StopTimer()
		executors.Release()
	}
}

func BenchmarkTopnTransformDims1(b *testing.B) {
	go func() {
		_ = http.ListenAndServe("127.0.0.1:6060", nil)
	}()
	srcRowDataType := buildTopNRowDataType()
	dstRowDataType := buildTopNRowDataType()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "topn", Args: []influxql.Expr{&influxql.VarRef{Val: "f1", Type: influxql.Integer},
				&influxql.NumberLiteral{Val: 0.0003}, &influxql.IntegerLiteral{Val: 10}, &influxql.StringLiteral{Val: "sum"}}},
			Ref: influxql.VarRef{Val: `value`, Type: influxql.Integer},
		},
	}
	chunks := buildBenchDimChunksTopnTransform(1000, 1000)
	schema := buildBenchmarkTopNDimsSchema()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewTopNTransform([]hybridqp.RowDataType{srcRowDataType}, []hybridqp.RowDataType{dstRowDataType}, exprOpt, schema, executor.Fill)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.GetInputs()[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.GetOutputs()[0], sink.Input)
		if err != nil {
			b.Fatalf("connect error")
		}
		var processors executor.Processors
		processors = append(processors, source)
		processors = append(processors, trans1)
		processors = append(processors, sink)
		executors := executor.NewPipelineExecutor(processors)

		b.StartTimer()
		err = executors.Execute(context.Background())
		if err != nil {
			b.Fatalf("connect error")
		}
		b.StopTimer()
		executors.Release()
	}
}

func BenchmarkTopnTransformDims1CompareHashAgg(b *testing.B) {
	go func() {
		_ = http.ListenAndServe("127.0.0.1:6060", nil)
	}()
	srcRowDataType := buildTopNRowDataType()
	dstRowDataType := buildTopNRowDataType()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "val0", Type: influxql.Integer}}},
			Ref:  influxql.VarRef{Val: "val0", Type: influxql.Integer},
		},
	}
	chunks := buildBenchDimChunksTopnTransform(1000, 1000)
	schema := buildBenchmarkTopNDimsSchemaCompareHashAgg()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform([]hybridqp.RowDataType{srcRowDataType}, []hybridqp.RowDataType{dstRowDataType}, exprOpt, schema, executor.Normal)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.GetInputs()[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.GetOutputs()[0], sink.Input)
		if err != nil {
			b.Fatalf("connect error")
		}
		var processors executor.Processors
		processors = append(processors, source)
		processors = append(processors, trans1)
		processors = append(processors, sink)
		executors := executor.NewPipelineExecutor(processors)

		b.StartTimer()
		err = executors.Execute(context.Background())
		if err != nil {
			b.Fatalf("connect error")
		}
		b.StopTimer()
		executors.Release()
	}
}
