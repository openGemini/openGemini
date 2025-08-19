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

func buildJoinCondition() influxql.Expr {
	var joinCondition influxql.Expr
	f1 := &influxql.VarRef{
		Val:  "m1.tag1",
		Type: influxql.Integer,
	}
	f2 := &influxql.VarRef{
		Val:  "m2.tag1",
		Type: influxql.Integer,
	}
	f3 := &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: f1,
		RHS: f2,
	}
	joinCondition = f3
	return joinCondition
}
func buildOutputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val2", Type: influxql.Boolean},
		influxql.VarRef{Val: "val3", Type: influxql.Integer},
	)
	return rowDataType
}
func buildInRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
		influxql.VarRef{Val: "val1", Type: influxql.String},
		influxql.VarRef{Val: "val2", Type: influxql.Boolean},
		influxql.VarRef{Val: "val3", Type: influxql.Integer},
	)
	return rowDataType
}

func BuildInChunk1(name string) executor.Chunk {
	rowDataType := buildInRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk(name)
	chunk.AppendTimes([]int64{1, 2, 3})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + "tag1val"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(0).AppendManyNotNil(3)
	chunk.Column(1).AppendStringValues([]string{"f1", "f2", "f3"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(1).AppendManyNotNil(3)
	chunk.Column(2).AppendBooleanValues([]bool{true, true, true})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(2).AppendManyNotNil(3)
	chunk.Column(3).AppendIntegerValues([]int64{1, 2, 3})
	chunk.Column(3).AppendColumnTimes([]int64{1, 2, 3})
	chunk.Column(3).AppendManyNotNil(3)
	return chunk
}

func BuildInChunk2(name string) executor.Chunk {
	rowDataType := buildInRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk(name)
	chunk.AppendTimes([]int64{1, 2})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + "tag1val"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 2})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2})
	chunk.Column(0).AppendManyNotNil(2)
	chunk.Column(1).AppendStringValues([]string{"f1", "f2"})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2})
	chunk.Column(1).AppendManyNotNil(2)
	chunk.Column(2).AppendBooleanValues([]bool{true, true})
	chunk.Column(2).AppendColumnTimes([]int64{1, 2})
	chunk.Column(2).AppendManyNotNil(2)
	chunk.Column(3).AppendIntegerValues([]int64{1, 2})
	chunk.Column(3).AppendColumnTimes([]int64{1, 2})
	chunk.Column(3).AppendManyNotNil(2)

	return chunk
}

func BuildInChunk3(name string) executor.Chunk {
	rowDataType := buildInRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk(name)
	chunk.AppendTimes([]int64{6, 7})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + "tag1val"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{6, 7})
	chunk.Column(0).AppendColumnTimes([]int64{6, 7})
	chunk.Column(0).AppendManyNotNil(2)
	chunk.Column(1).AppendStringValues([]string{"f6", "f7"})
	chunk.Column(1).AppendColumnTimes([]int64{6, 7})
	chunk.Column(1).AppendManyNotNil(2)
	chunk.Column(2).AppendBooleanValues([]bool{true, true})
	chunk.Column(2).AppendColumnTimes([]int64{6, 7})
	chunk.Column(2).AppendManyNotNil(2)
	chunk.Column(3).AppendIntegerValues([]int64{6, 7})
	chunk.Column(3).AppendColumnTimes([]int64{6, 7})
	chunk.Column(3).AppendManyNotNil(2)
	return chunk
}

func BuildInChunk4(name string) executor.Chunk {
	rowDataType := buildInRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk(name)
	chunk.AppendTimes([]int64{6, 7})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=" + "tag1val2"), 0)
	chunk.AppendIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{6, 7})
	chunk.Column(0).AppendColumnTimes([]int64{6, 7})
	chunk.Column(0).AppendManyNotNil(2)
	chunk.Column(1).AppendStringValues([]string{"f6", "f7"})
	chunk.Column(1).AppendColumnTimes([]int64{6, 7})
	chunk.Column(1).AppendManyNotNil(2)
	chunk.Column(2).AppendBooleanValues([]bool{true, true})
	chunk.Column(2).AppendColumnTimes([]int64{6, 7})
	chunk.Column(2).AppendManyNotNil(2)
	chunk.Column(3).AppendIntegerValues([]int64{6, 7})
	chunk.Column(3).AppendColumnTimes([]int64{6, 7})
	chunk.Column(3).AppendManyNotNil(2)
	return chunk
}

func buildJoinCase() *influxql.Join {
	joinCondition := buildJoinCondition()
	joinCase := &influxql.Join{}
	joinCase.Condition = joinCondition
	joinCase.LSrc = &influxql.SubQuery{}
	joinCase.RSrc = &influxql.SubQuery{}
	joinCase.LSrc.(*influxql.SubQuery).Alias = "m1"
	joinCase.RSrc.(*influxql.SubQuery).Alias = "m2"
	return joinCase
}

func buildFullJoinSchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	var expr1 influxql.Expr = &influxql.VarRef{
		Val:  "m1.field1",
		Type: influxql.Float,
	}
	var expr2 influxql.Expr = &influxql.VarRef{
		Val:  "m1.field2",
		Type: influxql.String,
	}
	var expr3 influxql.Expr = &influxql.VarRef{
		Val:  "m2.field3",
		Type: influxql.Boolean,
	}
	var expr4 influxql.Expr = &influxql.VarRef{
		Val:  "m2.field4",
		Type: influxql.Integer,
	}
	alias1 := influxql.VarRef{
		Val:  "val0",
		Type: influxql.Float,
	}
	alias2 := influxql.VarRef{
		Val:  "val1",
		Type: influxql.String,
	}
	alias3 := influxql.VarRef{
		Val:  "val2",
		Type: influxql.Boolean,
	}
	alias4 := influxql.VarRef{
		Val:  "val3",
		Type: influxql.Integer,
	}
	schema.Mapping()[expr1] = alias1
	schema.Mapping()[expr2] = alias2
	schema.Mapping()[expr3] = alias3
	schema.Mapping()[expr4] = alias4
	return schema
}

func TestFullJoinTransformDemo1(t *testing.T) {
	chunk1 := BuildInChunk2("m1")
	chunk2 := BuildInChunk1("m2")
	chunk3 := BuildInChunk3("m2")
	ctx := context.Background()
	outputRowDataType := buildOutputRowDataType()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2, chunk3})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	joinCase := buildJoinCase()
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
	}
	schema := buildFullJoinSchema()
	trans, _ := executor.NewFullJoinTransform(inRowDataTypes, outputRowDataType, joinCase, schema)
	sink := NewSinkFromFunction(outputRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	ec := make(chan error, 1)
	go func() {
		ec <- executors.Execute(context.Background())
		close(ec)
		close(opt.RowsChan)
	}()
	var closed bool
	for {
		select {
		case data, ok := <-opt.RowsChan:
			if !ok {
				closed = true
				break
			}
			_ = data
			//fmt.Println(data.Rows[0].Values[0])
		case <-ctx.Done():
			closed = true
			break
		}
		if closed {
			break
		}
	}
	executors.Release()
}

func TestFullJoinTransformDemo2(t *testing.T) {
	chunk1 := BuildInChunk1("m1")
	chunk2 := BuildInChunk2("m2")
	chunk3 := BuildInChunk3("m1")
	ctx := context.Background()
	outputRowDataType := buildOutputRowDataType()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1, chunk3})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	joinCase := buildJoinCase()
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
	}
	schema := buildFullJoinSchema()
	trans, _ := executor.NewFullJoinTransform(inRowDataTypes, outputRowDataType, joinCase, schema)
	sink := NewSinkFromFunction(outputRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	ec := make(chan error, 1)
	go func() {
		ec <- executors.Execute(context.Background())
		close(ec)
		close(opt.RowsChan)
	}()
	var closed bool
	for {
		select {
		case data, ok := <-opt.RowsChan:
			if !ok {
				closed = true
				break
			}
			_ = data
			//fmt.Println(data.Rows[0].Values[0])
		case <-ctx.Done():
			closed = true
			break
		}
		if closed {
			break
		}
	}
	executors.Release()
}

func TestFullJoinTransformDemo3(t *testing.T) {
	chunk1 := BuildInChunk1("m1")
	chunk2 := BuildInChunk4("m2")
	ctx := context.Background()
	outputRowDataType := buildOutputRowDataType()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	joinCase := buildJoinCase()
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
	}
	schema := buildFullJoinSchema()
	trans, _ := executor.NewFullJoinTransform(inRowDataTypes, outputRowDataType, joinCase, schema)
	sink := NewSinkFromFunction(outputRowDataType, func(chunk executor.Chunk) error {
		return nil
	})
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	ec := make(chan error, 1)
	go func() {
		ec <- executors.Execute(context.Background())
		close(ec)
		close(opt.RowsChan)
	}()
	var closed bool
	for {
		select {
		case data, ok := <-opt.RowsChan:
			if !ok {
				closed = true
				break
			}
			_ = data
			//fmt.Println(data.Rows[0].Values[0])
		case <-ctx.Done():
			closed = true
			break
		}
		if closed {
			break
		}
	}
	executors.Release()
}

func TestFullJoinTransformDemo4(t *testing.T) {
	outputRowDataType := buildOutputRowDataType()
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, outputRowDataType)
	inRowDataTypes = append(inRowDataTypes, outputRowDataType)
	joinCase := buildJoinCase()
	joinCase.LSrc.(*influxql.SubQuery).Alias = "m1"
	joinCase.RSrc.(*influxql.SubQuery).Alias = "m1"
	schema := buildFullJoinSchema()
	_, err := executor.NewFullJoinTransform(inRowDataTypes, outputRowDataType, joinCase, schema)
	assert.NotEqual(t, err, nil)
}
