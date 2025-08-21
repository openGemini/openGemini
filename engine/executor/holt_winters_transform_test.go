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

func buildHwRowDataType1() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "mean", Type: influxql.Float},
		influxql.VarRef{Val: "holt_winters", Type: influxql.Float},
		influxql.VarRef{Val: "count", Type: influxql.Integer},
		influxql.VarRef{Val: "holt_winters_with_fit", Type: influxql.Integer},
	)
	return rowDataType
}

func buildHwRowDataType2() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "holt_winters", Type: influxql.Float},
	)
	return rowDataType
}

func buildErrorHwRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "mean", Type: influxql.Float},
		influxql.VarRef{Val: "holt_winters", Type: influxql.Float},
		influxql.VarRef{Val: "count", Type: influxql.Integer},
		influxql.VarRef{Val: "holt_winters_with_fit", Type: influxql.String},
	)
	return rowDataType
}

func BuildHwInChunk1() executor.Chunk {
	rowDataType := buildHwRowDataType1()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mem")
	chunk.AppendTimes([]int64{1671782640000000000, 1671782643000000000, 1671782646000000000})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + "1"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1})
	chunk.Column(0).AppendManyNotNil(1)
	chunk.Column(0).AppendNil()
	chunk.Column(0).AppendFloatValues([]float64{2})
	chunk.Column(0).AppendManyNotNil(1)

	chunk.Column(1).AppendFloatValues([]float64{1})
	chunk.Column(1).AppendManyNotNil(1)
	chunk.Column(1).AppendNil()
	chunk.Column(1).AppendFloatValues([]float64{2})
	chunk.Column(1).AppendManyNotNil(1)

	chunk.Column(2).AppendIntegerValues([]int64{1})
	chunk.Column(2).AppendManyNotNil(1)
	chunk.Column(2).AppendNil()
	chunk.Column(2).AppendIntegerValues([]int64{1})
	chunk.Column(2).AppendManyNotNil(1)

	chunk.Column(3).AppendIntegerValues([]int64{1})
	chunk.Column(3).AppendManyNotNil(1)
	chunk.Column(3).AppendNil()
	chunk.Column(3).AppendIntegerValues([]int64{1})
	chunk.Column(3).AppendManyNotNil(1)

	return chunk
}

func BuildHwInChunk2() executor.Chunk {
	rowDataType := buildHwRowDataType1()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mem")
	chunk.AppendTimes([]int64{1671782649000000000, 1671782658000000000})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + "1"), 0)
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + "2"), 1)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{3})
	chunk.Column(0).AppendManyNotNil(1)
	chunk.Column(0).AppendFloatValues([]float64{4})
	chunk.Column(0).AppendManyNotNil(1)

	chunk.Column(1).AppendFloatValues([]float64{3})
	chunk.Column(1).AppendManyNotNil(1)
	chunk.Column(1).AppendFloatValues([]float64{4})
	chunk.Column(1).AppendManyNotNil(1)

	chunk.Column(2).AppendIntegerValues([]int64{1})
	chunk.Column(2).AppendManyNotNil(1)
	chunk.Column(2).AppendIntegerValues([]int64{1})
	chunk.Column(2).AppendManyNotNil(1)

	chunk.Column(3).AppendIntegerValues([]int64{1})
	chunk.Column(3).AppendManyNotNil(1)
	chunk.Column(3).AppendIntegerValues([]int64{1})
	chunk.Column(3).AppendManyNotNil(1)
	return chunk
}

func BuildHwInChunk3() executor.Chunk {
	rowDataType := buildHwRowDataType1()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mem")
	chunk.AppendTimes([]int64{1671782661000000000, 1671782664000000000})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + "2"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{5})
	chunk.Column(0).AppendManyNotNil(1)
	chunk.Column(0).AppendFloatValues([]float64{6})
	chunk.Column(0).AppendManyNotNil(1)

	chunk.Column(1).AppendFloatValues([]float64{5})
	chunk.Column(1).AppendManyNotNil(1)
	chunk.Column(1).AppendFloatValues([]float64{6})
	chunk.Column(1).AppendManyNotNil(1)

	chunk.Column(2).AppendIntegerValues([]int64{1})
	chunk.Column(2).AppendManyNotNil(1)
	chunk.Column(2).AppendIntegerValues([]int64{1})
	chunk.Column(2).AppendManyNotNil(1)

	chunk.Column(3).AppendIntegerValues([]int64{1})
	chunk.Column(3).AppendManyNotNil(1)
	chunk.Column(3).AppendIntegerValues([]int64{1})
	chunk.Column(3).AppendManyNotNil(1)
	return chunk
}

func buildHoltWintersSchema1() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		StartTime:   1671782630000028374,
		EndTime:     1671782697060077431,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	opt.Interval.Duration = 3000000000 // 3s
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	var meanArgs []influxql.Expr
	var meanArg influxql.Expr = &influxql.VarRef{
		Val:   "f2",
		Type:  influxql.Float,
		Alias: "",
	}
	meanArgs = append(meanArgs, meanArg)
	var hwArgs []influxql.Expr
	var hwArg0 influxql.Expr = &influxql.Call{
		Name: "mean",
		Args: meanArgs,
	}
	var hwArg1 influxql.Expr = &influxql.IntegerLiteral{
		Val: 10,
	}
	var hwArg2 influxql.Expr = &influxql.IntegerLiteral{
		Val: 1,
	}
	hwArgs = append(hwArgs, hwArg0, hwArg1, hwArg2)
	var hw influxql.Call = influxql.Call{
		Name: "holt_winters",
		Args: hwArgs,
	}

	var countArgs []influxql.Expr
	var countArg influxql.Expr = &influxql.VarRef{
		Val:   "f2",
		Type:  influxql.Float,
		Alias: "",
	}
	countArgs = append(countArgs, countArg)
	var hwwfArgs []influxql.Expr
	var hwwfArg0 influxql.Expr = &influxql.Call{
		Name: "count",
		Args: countArgs,
	}
	var hwwfArg1 influxql.Expr = &influxql.IntegerLiteral{
		Val: 12,
	}
	var hwwfArg2 influxql.Expr = &influxql.IntegerLiteral{
		Val: 1,
	}
	hwwfArgs = append(hwwfArgs, hwwfArg0, hwwfArg1, hwwfArg2)
	var hwwf influxql.Call = influxql.Call{
		Name: "holt_winters_with_fit",
		Args: hwwfArgs,
	}
	var hws []*influxql.Call
	hws = append(hws, &hw)
	hws = append(hws, &hwwf)
	schema.SetHoltWinters(hws)
	return schema
}

func buildHoltWintersSchema2() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		StartTime:   1597042800000000000,
		EndTime:     1597049400000000000,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	opt.Interval.Duration = 1200000000000 // 20min
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	var firstArgs []influxql.Expr
	var firstArg influxql.Expr = &influxql.VarRef{
		Val:   "value",
		Type:  influxql.Float,
		Alias: "",
	}
	firstArgs = append(firstArgs, firstArg)
	var hwArgs []influxql.Expr
	var hwArg0 influxql.Expr = &influxql.Call{
		Name: "first",
		Args: firstArgs,
	}
	var hwArg1 influxql.Expr = &influxql.IntegerLiteral{
		Val: 6,
	}
	var hwArg2 influxql.Expr = &influxql.IntegerLiteral{
		Val: 4,
	}
	hwArgs = append(hwArgs, hwArg0, hwArg1, hwArg2)
	var hw influxql.Call = influxql.Call{
		Name: "holt_winters",
		Args: hwArgs,
	}
	var hws []*influxql.Call
	hws = append(hws, &hw)
	schema.SetHoltWinters(hws)
	return schema
}

func TestHoltWintersDemo1(t *testing.T) {
	chunk1 := BuildHwInChunk1()
	chunk2 := BuildHwInChunk2()
	chunk3 := BuildHwInChunk3()
	inRowDataType := buildHwRowDataType1()
	outRowDataType := buildHwRowDataType1()
	source := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1, chunk2, chunk3})
	schema := buildHoltWintersSchema1()
	trans, _ := executor.NewHoltWintersTransform(inRowDataType, outRowDataType, schema.Options().(*query.ProcessorOptions), schema)
	sink := NewSinkFromFunction(outRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func BuildHwInChunk4() executor.Chunk {
	rowDataType := buildHwRowDataType2()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("cpu")
	chunk.AppendTimes([]int64{1597042800000000000, 1597044000000000000})
	chunk.AppendTagsAndIndex(*executor.NewChunkTagsV2(nil), 0)
	chunk.Column(0).AppendFloatValues([]float64{10, 37})
	chunk.Column(0).AppendManyNotNil(2)
	return chunk
}

func BuildHwInChunk5() executor.Chunk {
	rowDataType := buildHwRowDataType2()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("cpu")
	chunk.AppendTimes([]int64{1597045200000000000, 1597046400000000000})
	chunk.AppendTagsAndIndex(*executor.NewChunkTagsV2(nil), 0)
	chunk.Column(0).AppendFloatValues([]float64{48, 80})
	chunk.Column(0).AppendManyNotNil(2)
	return chunk
}

func BuildHwInChunk6() executor.Chunk {
	rowDataType := buildHwRowDataType2()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("cpu")
	chunk.AppendTimes([]int64{1597047600000000000, 1597048800000000000})
	chunk.AppendTagsAndIndex(*executor.NewChunkTagsV2(nil), 0)
	chunk.Column(0).AppendFloatValues([]float64{39, 25})
	chunk.Column(0).AppendManyNotNil(2)
	return chunk
}

func TestHoltWintersDemo2(t *testing.T) {
	chunk1 := BuildHwInChunk4()
	chunk2 := BuildHwInChunk5()
	chunk3 := BuildHwInChunk6()
	inRowDataType := buildHwRowDataType2()
	outRowDataType := buildHwRowDataType2()
	source := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1, chunk2, chunk3})
	schema := buildHoltWintersSchema2()
	trans, _ := executor.NewHoltWintersTransform(inRowDataType, outRowDataType, schema.Options().(*query.ProcessorOptions), schema)
	sink := NewSinkFromFunction(outRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestHoltWintersDemo3(t *testing.T) {
	inRowDataType := buildErrorHwRowDataType()
	outRowDataType := buildHwRowDataType1()
	schema := buildHoltWintersSchema1()
	_, err := executor.NewHoltWintersTransform(inRowDataType, outRowDataType, schema.Options().(*query.ProcessorOptions), schema)
	assert.NotEqual(t, err, nil)
}

func buildHoltWintersSchema4() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		StartTime:   1671782630000028374,
		EndTime:     1671782697060077431,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	opt.Interval.Duration = 3000000000 // 3s
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	var meanArgs []influxql.Expr
	var meanArg influxql.Expr = &influxql.VarRef{
		Val:   "f2",
		Type:  influxql.Float,
		Alias: "",
	}
	meanArgs = append(meanArgs, meanArg)
	var hwArgs []influxql.Expr
	var hwArg0 influxql.Expr = &influxql.Call{
		Name: "mean",
		Args: meanArgs,
	}
	var hwArg1 influxql.Expr = &influxql.IntegerLiteral{
		Val: 2000, // test out of chunksize
	}
	var hwArg2 influxql.Expr = &influxql.IntegerLiteral{
		Val: 1,
	}
	hwArgs = append(hwArgs, hwArg0, hwArg1, hwArg2)
	var hw influxql.Call = influxql.Call{
		Name: "holt_winters",
		Args: hwArgs,
	}

	var countArgs []influxql.Expr
	var countArg influxql.Expr = &influxql.VarRef{
		Val:   "f2",
		Type:  influxql.Float,
		Alias: "",
	}
	countArgs = append(countArgs, countArg)
	var hwwfArgs []influxql.Expr
	var hwwfArg0 influxql.Expr = &influxql.Call{
		Name: "count",
		Args: countArgs,
	}
	var hwwfArg1 influxql.Expr = &influxql.IntegerLiteral{
		Val: 12,
	}
	var hwwfArg2 influxql.Expr = &influxql.IntegerLiteral{
		Val: 1,
	}
	hwwfArgs = append(hwwfArgs, hwwfArg0, hwwfArg1, hwwfArg2)
	var hwwf influxql.Call = influxql.Call{
		Name: "holt_winters_with_fit",
		Args: hwwfArgs,
	}
	var hws []*influxql.Call
	hws = append(hws, &hw)
	hws = append(hws, &hwwf)
	schema.SetHoltWinters(hws)
	return schema
}

func TestHoltWintersDemo4(t *testing.T) {
	chunk1 := BuildHwInChunk1()
	chunk2 := BuildHwInChunk2()
	chunk3 := BuildHwInChunk3()
	inRowDataType := buildHwRowDataType1()
	outRowDataType := buildHwRowDataType1()
	source := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1, chunk2, chunk3})
	schema := buildHoltWintersSchema4()
	trans, _ := executor.NewHoltWintersTransform(inRowDataType, outRowDataType, schema.Options().(*query.ProcessorOptions), schema)
	sink := NewSinkFromFunction(outRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	executor.Connect(source.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func BuildHwInChunk7(tagVal string) executor.Chunk {
	rowDataType := buildHwRowDataType1()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mem")
	chunk.AppendTimes([]int64{1671782640000000000, 1671782643000000000, 1671782646000000000})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + tagVal), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1})
	chunk.Column(0).AppendManyNotNil(1)
	chunk.Column(0).AppendNil()
	chunk.Column(0).AppendFloatValues([]float64{2})
	chunk.Column(0).AppendManyNotNil(1)

	chunk.Column(1).AppendFloatValues([]float64{1})
	chunk.Column(1).AppendManyNotNil(1)
	chunk.Column(1).AppendNil()
	chunk.Column(1).AppendFloatValues([]float64{2})
	chunk.Column(1).AppendManyNotNil(1)

	chunk.Column(2).AppendIntegerValues([]int64{1})
	chunk.Column(2).AppendManyNotNil(1)
	chunk.Column(2).AppendNil()
	chunk.Column(2).AppendIntegerValues([]int64{1})
	chunk.Column(2).AppendManyNotNil(1)

	chunk.Column(3).AppendIntegerValues([]int64{1})
	chunk.Column(3).AppendManyNotNil(1)
	chunk.Column(3).AppendNil()
	chunk.Column(3).AppendIntegerValues([]int64{1})
	chunk.Column(3).AppendManyNotNil(1)

	return chunk
}

func buildHoltWintersSchema5() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		StartTime:   1671782640000000000,
		EndTime:     1671782646000000000,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	opt.Interval.Duration = 3000000000 // 3s
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	var meanArgs []influxql.Expr
	var meanArg influxql.Expr = &influxql.VarRef{
		Val:   "f2",
		Type:  influxql.Float,
		Alias: "",
	}
	meanArgs = append(meanArgs, meanArg)
	var hwArgs []influxql.Expr
	var hwArg0 influxql.Expr = &influxql.Call{
		Name: "mean",
		Args: meanArgs,
	}
	var hwArg1 influxql.Expr = &influxql.IntegerLiteral{
		Val: 2000, // test out of chunksize
	}
	var hwArg2 influxql.Expr = &influxql.IntegerLiteral{
		Val: 1,
	}
	hwArgs = append(hwArgs, hwArg0, hwArg1, hwArg2)
	var hw influxql.Call = influxql.Call{
		Name: "holt_winters",
		Args: hwArgs,
	}

	var countArgs []influxql.Expr
	var countArg influxql.Expr = &influxql.VarRef{
		Val:   "f2",
		Type:  influxql.Float,
		Alias: "",
	}
	countArgs = append(countArgs, countArg)
	var hwwfArgs []influxql.Expr
	var hwwfArg0 influxql.Expr = &influxql.Call{
		Name: "count",
		Args: countArgs,
	}
	var hwwfArg1 influxql.Expr = &influxql.IntegerLiteral{
		Val: 12,
	}
	var hwwfArg2 influxql.Expr = &influxql.IntegerLiteral{
		Val: 1,
	}
	hwwfArgs = append(hwwfArgs, hwwfArg0, hwwfArg1, hwwfArg2)
	var hwwf influxql.Call = influxql.Call{
		Name: "holt_winters_with_fit",
		Args: hwwfArgs,
	}
	var hws []*influxql.Call
	hws = append(hws, &hw)
	hws = append(hws, &hwwf)
	schema.SetHoltWinters(hws)
	return schema
}

func TestHoltWintersDemo5(t *testing.T) {
	chunk1 := BuildHwInChunk7("1")
	chunk2 := BuildHwInChunk7("2")
	chunk3 := BuildHwInChunk7("3")
	chunk4 := BuildHwInChunk7("4")
	inRowDataType := buildHwRowDataType1()
	outRowDataType := buildHwRowDataType1()
	source := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1, chunk2, chunk3, chunk4})
	schema := buildHoltWintersSchema5()
	trans, _ := executor.NewHoltWintersTransform(inRowDataType, outRowDataType, schema.Options().(*query.ProcessorOptions), schema)
	sink := NewSinkFromFunction(outRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	assert.NoError(t, executor.Connect(source.Output, trans.GetInputs()[0]))
	assert.NoError(t, executor.Connect(trans.GetOutputs()[0], sink.Input))
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	assert.NoError(t, executors.Execute(context.Background()))
	executors.Release()
}
