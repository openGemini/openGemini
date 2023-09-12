/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

func buildHashAggInputRowDataType1() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Integer},
		influxql.VarRef{Val: "val1", Type: influxql.Float},
		influxql.VarRef{Val: "val2", Type: influxql.Integer},
		influxql.VarRef{Val: "val3", Type: influxql.Float},
		influxql.VarRef{Val: "val4", Type: influxql.Boolean},
		influxql.VarRef{Val: "val5", Type: influxql.String},
		influxql.VarRef{Val: "val6", Type: influxql.Integer},
		influxql.VarRef{Val: "val7", Type: influxql.Float},
		influxql.VarRef{Val: "val8", Type: influxql.Boolean},
		influxql.VarRef{Val: "val9", Type: influxql.String},
		influxql.VarRef{Val: "val10", Type: influxql.Integer},
		influxql.VarRef{Val: "val11", Type: influxql.Float},
		influxql.VarRef{Val: "val12", Type: influxql.Boolean},
		influxql.VarRef{Val: "val13", Type: influxql.String},
		influxql.VarRef{Val: "val14", Type: influxql.Integer},
		influxql.VarRef{Val: "val15", Type: influxql.Float},
		influxql.VarRef{Val: "val16", Type: influxql.Integer},
		influxql.VarRef{Val: "val17", Type: influxql.Float},
		influxql.VarRef{Val: "val18", Type: influxql.Integer},
		influxql.VarRef{Val: "val19", Type: influxql.Float},
	)
	return rowDataType
}

func buildHashAggInputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "val0", Type: influxql.Float},
		influxql.VarRef{Val: "val1", Type: influxql.Integer},
	)
	return rowDataType
}

func buildHashAggOutputRowDataType1() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sumVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "sumVal1", Type: influxql.Float},
		influxql.VarRef{Val: "countVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "countVal1", Type: influxql.Integer},
		influxql.VarRef{Val: "countVal2", Type: influxql.Integer},
		influxql.VarRef{Val: "countVal3", Type: influxql.Integer},
		influxql.VarRef{Val: "firstVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "firstVal1", Type: influxql.Float},
		influxql.VarRef{Val: "firstVal2", Type: influxql.Boolean},
		influxql.VarRef{Val: "firstVal3", Type: influxql.String},
		influxql.VarRef{Val: "lastVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "lastVal1", Type: influxql.Float},
		influxql.VarRef{Val: "lastVal2", Type: influxql.Boolean},
		influxql.VarRef{Val: "lastVal3", Type: influxql.String},
		influxql.VarRef{Val: "minVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "minVal1", Type: influxql.Float},
		influxql.VarRef{Val: "maxVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "maxVal1", Type: influxql.Float},
		influxql.VarRef{Val: "perVal0", Type: influxql.Integer},
		influxql.VarRef{Val: "perVal1", Type: influxql.Float},
	)
	return rowDataType
}

func buildHashAggOutputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "sumVal0", Type: influxql.Float},
		influxql.VarRef{Val: "firstVal1", Type: influxql.Integer},
	)
	return rowDataType
}

func BuildHashAggInChunkForDimsIn3() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType1()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.NewDims(1)
	chunk.AddDims([]string{"tag1val1"})
	chunk.AddDims([]string{"tag1val2"})
	chunk.AddDims([]string{"tag1val1"})
	chunk.AddDims([]string{"tag1val2"})
	for _, col := range chunk.Columns() {
		switch col.DataType() {
		case influxql.Integer:
			col.AppendIntegerValues([]int64{1, 2, 3})
		case influxql.Float:
			col.AppendFloatValues([]float64{1, 2, 3})
		case influxql.Boolean:
			col.AppendBooleanValues([]bool{true, false, true})
		case influxql.String:
			col.AppendStringValues([]string{"1", "2", "3"})
		default:
			panic("datatype error")
		}
		col.AppendManyNotNil(3)
		col.AppendManyNil(1)
	}
	return chunk
}

func BuildHashAggInChunkForDimsIn4() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType1()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.NewDims(1)
	chunk.AddDims([]string{"tag1val3"})
	chunk.AddDims([]string{"tag1val3"})
	chunk.AddDims([]string{"tag1val4"})
	chunk.AddDims([]string{"tag1val4"})
	for _, col := range chunk.Columns() {
		switch col.DataType() {
		case influxql.Integer:
			col.AppendIntegerValues([]int64{1, 2, 3, 4})
		case influxql.Float:
			col.AppendFloatValues([]float64{1, 2, 3, 4})
		case influxql.Boolean:
			col.AppendBooleanValues([]bool{true, false, true, false})
		case influxql.String:
			col.AppendStringValues([]string{"1", "2", "3", "4"})
		default:
			panic("datatype error")
		}
		col.AppendManyNotNil(4)
	}
	return chunk
}

func buildAggFuncsName() []string {
	return []string{
		"sum", "sum",
		"count", "count", "count", "count",
		"first", "first", "first", "first",
		"last", "last", "last", "last",
		"min", "min",
		"max", "max",
		"percentile", "percentile"}
}

func TestHashAggTransformGroupByNoFixIntervalNullFillForDimsInForAllAggForNormal(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn3()
	chunk2 := BuildHashAggInChunkForDimsIn4()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType1()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByNoFixIntervalNullFill()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Normal)
	if err != nil {
		panic("")
	}
	printChunk := func(chunk executor.Chunk) error {
		str := StringToRows(chunk)
		fmt.Println(str)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func buildHashAggTransformSchemaGroupByNoFixIntervalNullFill() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		Fill:        influxql.NullFill,
		Interval:    hybridqp.Interval{Duration: 1},
		StartTime:   influxql.MinTime,
		EndTime:     influxql.MaxTime,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func TestHashAggTransformGroupByNoFixIntervalNullFillForDimsInForAllAgg(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn3()
	chunk2 := BuildHashAggInChunkForDimsIn4()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType1()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByNoFixIntervalNullFill()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	printChunk := func(chunk executor.Chunk) error {
		str := StringToRows(chunk)
		fmt.Println(str)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestHashAggTransformGroupByFixIntervalNullFillForDimsInForAllAgg(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn3()
	chunk2 := BuildHashAggInChunkForDimsIn4()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType1()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByFixIntervalNullFill1()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	printChunk := func(chunk executor.Chunk) error {
		str := StringToRows(chunk)
		fmt.Println(str)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestHashAggTransformGroupByFixIntervalNoFillForDimsInForAllAgg(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn3()
	chunk2 := BuildHashAggInChunkForDimsIn4()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType1()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByFixIntervalNullFill()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	printChunk := func(chunk executor.Chunk) error {
		str := StringToRows(chunk)
		fmt.Println(str)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestHashAggTransformGroupByFixIntervalNumFillForDimsInForAllAgg(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn3()
	chunk2 := BuildHashAggInChunkForDimsIn4()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType1()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByFixIntervalNumFill()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	printChunk := func(chunk executor.Chunk) error {
		str := StringToRows(chunk)
		fmt.Println(str)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func BuildHashAggInChunkForDimsIn1() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.NewDims(1)
	chunk.AddDims([]string{"tag1val1"})
	chunk.AddDims([]string{"tag1val2"})
	chunk.AddDims([]string{"tag1val1"})
	chunk.AddDims([]string{"tag1val2"})
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3, 4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendManyNotNil(4)
	return chunk
}

func BuildHashAggInChunkForDimsIn2() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.NewDims(1)
	chunk.AddDims([]string{"tag1val3"})
	chunk.AddDims([]string{"tag1val3"})
	chunk.AddDims([]string{"tag1val4"})
	chunk.AddDims([]string{"tag1val4"})
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3, 4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendManyNotNil(4)
	return chunk
}

func TestHashAggTransformGroupByFixIntervalNullFillForDimsIn(t *testing.T) {
	chunk1 := BuildHashAggInChunkForDimsIn1()
	chunk2 := BuildHashAggInChunkForDimsIn2()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByFixIntervalNullFill()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("val0")}},
			Ref:  influxql.VarRef{Val: "sumVal0", Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("val1")}},
			Ref:  influxql.VarRef{Val: "firstVal1", Type: influxql.Integer},
		},
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	printChunk := func(chunk executor.Chunk) error {
		str := StringToRows(chunk)
		fmt.Println(str)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func BuildHashAggInChunk1() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val1"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val2"), 1)
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val1"), 2)
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val2"), 3)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3, 4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendManyNotNil(4)
	return chunk
}

func BuildHashAggInChunk2() executor.Chunk {
	rowDataType := buildHashAggInputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("m1")
	chunk.AppendTimes([]int64{1, 2, 3, 4})
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val3"), 0)
	chunk.AddTagAndIndex(*ParseChunkTags("tag1=" + "tag1val4"), 2)
	chunk.AddIntervalIndex(0)
	chunk.Column(0).AppendFloatValues([]float64{1, 2, 3, 4})
	chunk.Column(0).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(0).AppendManyNotNil(4)
	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendColumnTimes([]int64{1, 2, 3, 4})
	chunk.Column(1).AppendManyNotNil(4)
	return chunk
}

func buildHashAggTransformSchemaGroupByFixIntervalNullFill() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     4,
		Fill:        influxql.NullFill,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildHashAggTransformSchemaGroupByFixIntervalNullFill1() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     5,
		Fill:        influxql.NullFill,
		Interval:    hybridqp.Interval{Duration: 1},
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildHashAggTransformSchemaGroupByFixIntervalNumFill() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     4,
		Fill:        influxql.NumberFill,
		FillValue:   1,
		Interval:    hybridqp.Interval{Duration: 1},
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func TestHashAggTransformGroupByFixIntervalNullFill(t *testing.T) {
	chunk1 := BuildHashAggInChunk1()
	chunk2 := BuildHashAggInChunk2()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupByFixIntervalNullFill()
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("val0")}},
			Ref:  influxql.VarRef{Val: "sumVal0", Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("val1")}},
			Ref:  influxql.VarRef{Val: "firstVal1", Type: influxql.Integer},
		},
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	printChunk := func(chunk executor.Chunk) error {
		str := StringToRows(chunk)
		fmt.Println(str)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func buildHashAggTransformSchemaGroupbyIntervalFixIntervalNoneFill() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		Fill:        influxql.NoFill,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	opt.Interval.Duration = 2
	opt.StartTime = 1
	opt.EndTime = 4
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func TestHashAggTransformGroupbyIntervalFixIntervalNoneFill(t *testing.T) {
	chunk1 := BuildHashAggInChunk1()
	chunk2 := BuildHashAggInChunk2()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaGroupbyIntervalFixIntervalNoneFill()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("val0")}},
			Ref:  influxql.VarRef{Val: "sumVal0", Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("val1")}},
			Ref:  influxql.VarRef{Val: "firstVal1", Type: influxql.Integer},
		},
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	printChunk := func(chunk executor.Chunk) error {
		str := StringToRows(chunk)
		fmt.Println(str)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func buildHashAggTransformSchemaIntervalFixIntervalNumFill() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Ascending:   true,
		StartTime:   1,
		EndTime:     6,
		Fill:        influxql.NumberFill,
		FillValue:   99,
	}
	opt.Interval.Duration = 2
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func TestHashAggTransformIntervalFixIntervalNumFill(t *testing.T) {
	chunk1 := BuildHashAggInChunk1()
	chunk2 := BuildHashAggInChunk2()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	source2 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk2})
	var inRowDataTypes []hybridqp.RowDataType
	inRowDataTypes = append(inRowDataTypes, source1.Output.RowDataType)
	inRowDataTypes = append(inRowDataTypes, source2.Output.RowDataType)
	var outRowDataTypes []hybridqp.RowDataType
	outRowDataType := buildHashAggOutputRowDataType()
	outRowDataTypes = append(outRowDataTypes, outRowDataType)
	schema := buildHashAggTransformSchemaIntervalFixIntervalNumFill()

	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("val0")}},
			Ref:  influxql.VarRef{Val: "sumVal0", Type: influxql.Float},
		},
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("val1")}},
			Ref:  influxql.VarRef{Val: "firstVal1", Type: influxql.Integer},
		},
	}
	trans, err := executor.NewHashAggTransform(inRowDataTypes, outRowDataTypes, exprOpt, schema, executor.Fill)
	if err != nil {
		panic("")
	}
	printChunk := func(chunk executor.Chunk) error {
		str := StringToRows(chunk)
		fmt.Println(str)
		return nil
	}
	sink := NewSinkFromFunction(outRowDataType, printChunk)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(source2.Output, trans.GetInputs()[1])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, source2)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestGroupKeysPoolAlloc(t *testing.T) {
	groupKeysPool := executor.NewGroupKeysPool(10)
	values := groupKeysPool.AllocValues(1024)
	assert.Equal(t, 1024, len(values))

	tags := groupKeysPool.AllocGroupTags(1024)
	assert.Equal(t, 1024, len(tags))

	zvalues := groupKeysPool.AllocZValues(1024)
	assert.Equal(t, 1024, len(zvalues))
}

func TestIntervalKeysPoolAlloc(t *testing.T) {
	groupKeysPool := executor.NewIntervalKeysMpool(10)
	values := groupKeysPool.AllocValues(1024)
	assert.Equal(t, 1024, len(values))

	keys := groupKeysPool.AllocIntervalKeys(1024)
	assert.Equal(t, 1024, len(keys))
}

func buildHashAggTransformSchemaBenchmark(interval int) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		Fill:        influxql.NullFill,
	}
	opt.Dimensions = append(opt.Dimensions, "host")
	if interval != 1 {
		opt.Interval.Duration = time.Duration(interval) * time.Nanosecond
		opt.StartTime = 0
		opt.EndTime = 8
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func benchmarkHashAggTransform(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType, interval int) {
	chunks := buildBenchChunksFixWindow(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	schema := buildHashAggTransformSchemaBenchmark(interval)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
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

func BenchmarkHashAggTransform(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 128, 1, 4
	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		StartTime:  0,
		EndTime:    8,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func BenchmarkAggregateTransformForCompare(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1024, 128, 4

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func BenchmarkHashAggTransformHcd(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 512, 1, 1
	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func BenchmarkAggregateTransformForCompareHcd(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1024, 512, 2

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		&opt, exprOpt, srcRowDataType, dstRowDataType)
}

func buildHashAggTransformSchemaBenchmark1(interval int) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   0,
		EndTime:     1024000,
	}
	opt.Interval.Duration = time.Duration(interval) * time.Nanosecond
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func benchmarkHashAggTransform1(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType, interval int) {
	chunks := buildBenchChunks1(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	schema := buildHashAggTransformSchemaBenchmark1(interval)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
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

func buildBenchChunks1(chunkCount, chunkSize, tagPerChunk, intervalPerChunk int) []executor.Chunk {
	rowDataType := buildBenchRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunkList := make([]executor.Chunk, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunk := b.NewChunk("mst")
		if tagPerChunk == 0 {
			chunk.AppendTagsAndIndexes([]executor.ChunkTags{}, []int{0})
		} else {
			tags := make([]executor.ChunkTags, 0)
			tagIndex := make([]int, 0)
			tagIndexInterval := chunkSize / tagPerChunk
			for t := 0; t < tagPerChunk; t++ {
				var buffer bytes.Buffer
				buffer.WriteString("host")
				buffer.WriteString("=")
				buffer.WriteString(strconv.Itoa(t + i*tagPerChunk))
				tags = append(tags, *ParseChunkTags(buffer.String()))
				tagIndex = append(tagIndex, t*tagIndexInterval)
			}
			chunk.AppendTagsAndIndexes(tags, tagIndex)
		}
		count := 0
		if intervalPerChunk == 0 {
			return nil
		}
		intervalIndex := make([]int, 0, chunkSize/intervalPerChunk)
		times := make([]int64, 0, chunkSize)
		for j := 0; j < chunkSize; j++ {
			if j%intervalPerChunk == 0 {
				intervalIndex = append(intervalIndex, intervalPerChunk*count)
				count++
			}
			times = append(times, int64(i*chunkSize+j))
			chunk.Column(0).AppendFloatValue(float64(i*chunkSize + j))
			chunk.Column(0).AppendNotNil()
		}
		chunk.AppendIntervalIndexes(intervalIndex)
		chunk.AppendTimes(times)
		chunkList = append(chunkList, chunk)
	}
	return chunkList
}

func BenchmarkHashAggTransformInterval(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 0, 1024, 1
	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Ordered:   true,
		Ascending: true,
		ChunkSize: ChunkSize,
		Parallel:  false,
		StartTime: 0,
		EndTime:   1024000,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggTransform1(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func benchmarkStreamAggregateTransform1(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType) {
	chunks := buildBenchChunks1(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewStreamAggregateTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt,
			&opt, false)
		sink := NewNilSink(dstRowDataType)
		err := executor.Connect(source.Output, trans1.Inputs[0])
		if err != nil {
			b.Fatalf("connect error")
		}
		err = executor.Connect(trans1.Outputs[0], sink.Input)
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

func BenchmarkAggregateTransformForCompareInterval(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk := 1000, 1024, 0, 1

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	intervalDuration := time.Duration(int64(intervalPerChunk))
	opt := query.ProcessorOptions{
		Exprs:     []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Interval:  hybridqp.Interval{Duration: intervalDuration * time.Nanosecond},
		Ordered:   true,
		Ascending: true,
		ChunkSize: ChunkSize,
		Parallel:  false,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkStreamAggregateTransform1(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType)
}

func buildHashAggTransformSchemaBenchmarkFixWindow(interval int) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Ascending:   true,
		StartTime:   0,
		EndTime:     8,
		Fill:        influxql.NullFill,
		Dimensions:  []string{"host"},
	}
	if interval != 1 {
		opt.Interval.Duration = time.Duration(interval) * time.Nanosecond
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildHashAggTransformSchemaBenchmarkFixWindow1(interval int) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Ascending:   true,
		StartTime:   0,
		EndTime:     8,
		Fill:        influxql.NoFill,
		Dimensions:  []string{"host"},
	}
	if interval != 1 {
		opt.Interval.Duration = time.Duration(interval) * time.Nanosecond
	}
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func benchmarkHashAggTransformFixWindow(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType, interval int) {
	chunks := buildBenchChunksFixWindow(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	schema := buildHashAggTransformSchemaBenchmarkFixWindow(interval)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
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

func benchmarkHashAggTransformFixWindow1(b *testing.B, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk int,
	opt query.ProcessorOptions, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType, interval int) {
	chunks := buildBenchChunksFixWindow(chunkCount, ChunkSize, tagPerChunk, intervalPerChunk)
	schema := buildHashAggTransformSchemaBenchmarkFixWindow1(interval)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
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

func buildBenchChunksFixWindow(chunkCount, chunkSize, tagPerChunk, intervalPerChunk int) []executor.Chunk {
	rowDataType := buildBenchRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunkList := make([]executor.Chunk, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunk := b.NewChunk("mst")
		if tagPerChunk == 0 {
			chunk.AppendTagsAndIndexes([]executor.ChunkTags{}, []int{0})
		} else {
			tags := make([]executor.ChunkTags, 0)
			tagIndex := make([]int, 0)
			tagIndexInterval := chunkSize / tagPerChunk
			for t := 0; t < tagPerChunk; t++ {
				var buffer bytes.Buffer
				buffer.WriteString("host")
				buffer.WriteString("=")
				buffer.WriteString(strconv.Itoa(t + i*tagPerChunk))
				tags = append(tags, *ParseChunkTags(buffer.String()))
				tagIndex = append(tagIndex, t*tagIndexInterval)
			}
			chunk.AppendTagsAndIndexes(tags, tagIndex)
		}
		if intervalPerChunk == 0 {
			return nil
		}
		intervalIndex := make([]int, 0, chunkSize/intervalPerChunk)
		times := make([]int64, 0, chunkSize)
		for j := 0; j < tagPerChunk; j++ {
			for k := 0; k < chunkSize/tagPerChunk; k++ {
				times = append(times, int64(k))
				chunk.Column(0).AppendFloatValue(float64(k))
				chunk.Column(0).AppendNotNil()
			}
		}
		chunk.AppendIntervalIndexes(intervalIndex)
		chunk.AppendTimes(times)
		chunkList = append(chunkList, chunk)
	}
	return chunkList
}

func BenchmarkHashAggTransformGroupByIntervalFixWindow(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 128, 1, 4 // 8 points pre serise
	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		StartTime:  0,
		EndTime:    8,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
		Fill:       influxql.NullFill,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggTransformFixWindow(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func BenchmarkHashAggTransformGroupByIntervalNonFixWindowForCompare(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 128, 1, 4 // 8 points pre serise

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`first("value1")`)},
		Dimensions: []string{"host"},
		Interval:   hybridqp.Interval{Duration: 4 * time.Nanosecond},
		StartTime:  0,
		EndTime:    8,
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
		Fill:       influxql.NoFill,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `first("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggTransformFixWindow1(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func BenchmarkHashAggTransformGroupByTidbCompare(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk, intervalPerChunk, interval := 1000, 1024, 1024, 1, 1 // 8 points pre serise

	srcRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
	)
	dstRowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: `sum("value1")`, Type: influxql.Float},
	)

	opt := query.ProcessorOptions{
		Exprs:      []influxql.Expr{hybridqp.MustParseExpr(`sum("value1")`)},
		Dimensions: []string{"host"},
		Ordered:    true,
		Ascending:  true,
		ChunkSize:  ChunkSize,
		Parallel:   false,
		Fill:       influxql.NoFill,
	}
	exprOpt := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{hybridqp.MustParseExpr("value1")}},
			Ref:  influxql.VarRef{Val: `sum("value1")`, Type: influxql.Float},
		},
	}
	benchmarkHashAggTransform(b, chunkCount, ChunkSize, tagPerChunk, intervalPerChunk,
		opt, exprOpt, srcRowDataType, dstRowDataType, interval)
}

func buildBenchChunksBatch(chunkCount, chunkSize, tagPerChunk int) []executor.Chunk {
	rowDataType := buildHashAggInputRowDataType1()
	b := executor.NewChunkBuilder(rowDataType)
	chunkList := make([]executor.Chunk, 0, chunkCount)
	seriesPoints := chunkSize / tagPerChunk
	tagsId := 0
	for i := 0; i < chunkCount; i++ {
		chunk := b.NewChunk("mst")
		chunk.NewDims(1)
		for j := 0; j < chunkSize; j++ {
			chunk.AppendTime(int64(j))
			if j%seriesPoints == 0 {
				tagsId++
			}
			chunk.AddDims([]string{strconv.Itoa(tagsId)})
			for _, col := range chunk.Columns() {
				switch col.DataType() {
				case influxql.Integer:
					col.AppendIntegerValue(int64(j % seriesPoints))
					col.AppendManyNotNil(1)
				case influxql.Float:
					col.AppendFloatValue(float64(j % seriesPoints))
					col.AppendManyNotNil(1)
				case influxql.Boolean:
					col.AppendBooleanValue(true)
					col.AppendManyNotNil(1)
				case influxql.String:
					col.AppendStringValue(strconv.Itoa(j % seriesPoints))
					col.AppendManyNotNil(1)
				default:
					panic("datatype error")
				}
			}
		}
		chunkList = append(chunkList, chunk)
	}
	return chunkList
}

func buildHashAggTransformSchemaBenchmarkBatch() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func benchmarkHashAggTransformBatch(b *testing.B, chunkCount, ChunkSize, tagPerChunk int, exprOpt []hybridqp.ExprOptions, srcRowDataType, dstRowDataType hybridqp.RowDataType) {
	chunks := buildBenchChunksBatch(chunkCount, ChunkSize, tagPerChunk)
	schema := buildHashAggTransformSchemaBenchmarkBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source := NewSourceFromMultiChunk(srcRowDataType, chunks)
		trans1, _ := executor.NewHashAggTransform(
			[]hybridqp.RowDataType{srcRowDataType},
			[]hybridqp.RowDataType{dstRowDataType},
			exprOpt, schema, executor.Fill)
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

func BenchmarkHashAggTransformGroupbyBatchCompare(b *testing.B) {
	chunkCount, ChunkSize, tagPerChunk := 1000, 8192, 5 // 1 points pre serise, 8 serise to one batch
	inRowDataTypes := make([]hybridqp.RowDataType, 0)
	inRowDataType := buildHashAggInputRowDataType1()
	inRowDataTypes = append(inRowDataTypes, inRowDataType)
	outRowDataType := buildHashAggOutputRowDataType1()
	aggFuncsName := buildAggFuncsName()
	exprOpt := make([]hybridqp.ExprOptions, len(outRowDataType.Fields()))
	for i := range outRowDataType.Fields() {
		if aggFuncsName[i] == "percentile" {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: "percentile", Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val), hybridqp.MustParseExpr("50")}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		} else {
			exprOpt[i] = hybridqp.ExprOptions{
				Expr: &influxql.Call{Name: aggFuncsName[i], Args: []influxql.Expr{hybridqp.MustParseExpr(inRowDataTypes[0].Field(i).Expr.(*influxql.VarRef).Val)}},
				Ref:  influxql.VarRef{Val: outRowDataType.Field(i).Expr.(*influxql.VarRef).Val, Type: outRowDataType.Field(i).Expr.(*influxql.VarRef).Type},
			}
		}
	}
	benchmarkHashAggTransformBatch(b, chunkCount, ChunkSize, tagPerChunk, exprOpt, inRowDataType, outRowDataType)
}

func TestIntervalKeysMPool(t *testing.T) {
	mpool := executor.NewIntervalKeysMpool(1)
	itervalKeys := mpool.AllocIntervalKeys(100)
	if len(itervalKeys) != 100 {
		t.Fatalf("alloc interval keys failed, expect:100, real:%d", len(itervalKeys))
	}
	values := mpool.AllocValues(110)
	if len(values) != 110 {
		t.Fatalf("alloc interval keys failed, expect:110, real:%d", len(values))
	}
	mpool.FreeValues(values)
	mpool.FreeIntervalKeys(itervalKeys)
}
