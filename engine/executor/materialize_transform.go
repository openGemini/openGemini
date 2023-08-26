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
	"errors"
	"fmt"
	"math"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/op"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

type StdoutChunkWriter struct{}

func NewStdoutChunkWriter() *StdoutChunkWriter {
	return &StdoutChunkWriter{}
}

func (w *StdoutChunkWriter) Write(chunk Chunk) {
	fmt.Printf("%v\n", chunk)
}

func (w *StdoutChunkWriter) Close() {

}

func AdjustNils(dst Column, src Column) {
	if src.NilCount() == 0 {
		dst.AppendManyNotNil(src.Length())
	} else {
		for i := 0; i < src.Length(); i++ {
			if src.IsNilV2(i) {
				dst.AppendNil()
			} else {
				dst.AppendNotNil()
			}
		}
	}
}

func TransparentForwardIntegerColumn(dst Column, src Column) {
	dst.AppendIntegerValues(src.IntegerValues())
	AdjustNils(dst, src)
}

func TransparentForwardFloatColumn(dst Column, src Column) {
	dst.AppendFloatValues(src.FloatValues())
	AdjustNils(dst, src)
}

func TransparentForwardBooleanColumn(dst Column, src Column) {
	dst.AppendBooleanValues(src.BooleanValues())
	AdjustNils(dst, src)
}

func TransparentForwardStringColumn(dst Column, src Column) {
	dst.CloneStringValues(src.GetStringBytes())
	AdjustNils(dst, src)
}

func TransparentForwardInteger(dst Column, src Chunk, index []int) {
	srcCol := src.Column(index[0])
	TransparentForwardIntegerColumn(dst, srcCol)
}

func TransparentForwardFloat(dst Column, src Chunk, index []int) {
	srcCol := src.Column(index[0])
	TransparentForwardFloatColumn(dst, srcCol)
}

func TransparentForwardBoolean(dst Column, src Chunk, index []int) {
	srcCol := src.Column(index[0])
	TransparentForwardBooleanColumn(dst, srcCol)
}

func TransparentForwardString(dst Column, src Chunk, index []int) {
	srcCol := src.Column(index[0])
	TransparentForwardStringColumn(dst, srcCol)
}

func TransMath(c *influxql.Call) (func(dst Column, src Chunk, index []int), error) {
	var f func(dst Column, src Chunk, index []int)
	switch c.Name {
	case "row_max":
		base, ok := c.Args[0].(*influxql.VarRef)
		if !ok {
			return nil, errors.New("expect varf in row_max")
		}
		for i := 0; i < len(c.Args); i++ {
			arg, ok := c.Args[i].(*influxql.VarRef)
			if !ok {
				return nil, errors.New("expect varf in row_max")
			}
			if arg.Type != base.Type {
				return nil, errors.New("args' type in row_max must be same")
			}
		}
		f = getMaxRowFunc(base.Type)
	}
	return f, nil
}

func rowMaxInteger(dst Column, src Chunk, index []int) {
	for i := 0; i < src.Len(); i++ {
		var val int64
		isNil := true
		for j := range index {
			empty := src.Column(index[j]).IsNilV2(i)
			if empty {
				continue
			}
			v := src.Column(index[j]).IntegerValues()[src.Column(index[j]).GetValueIndexV2(i)]
			if isNil || v > val {
				val = v
				isNil = false
			}
		}
		if isNil {
			dst.AppendNil()
		} else {
			dst.AppendIntegerValue(val)
			dst.AppendNotNil()
		}
	}
}

func rowMaxFloat(dst Column, src Chunk, index []int) {
	for i := 0; i < src.Len(); i++ {
		var val float64
		isNil := true
		for j := range index {
			empty := src.Column(index[j]).IsNilV2(i)
			if empty {
				continue
			}
			v := src.Column(index[j]).FloatValues()[src.Column(index[j]).GetValueIndexV2(i)]
			if isNil || v > val {
				val = v
				isNil = false
			}
		}
		if isNil {
			dst.AppendNil()
		} else {
			dst.AppendFloatValue(val)
			dst.AppendNotNil()
		}
	}
}

func rowMaxBoolean(dst Column, src Chunk, index []int) {
	for i := 0; i < src.Len(); i++ {
		var val bool
		isNil := true
		for j := range index {
			empty := src.Column(index[j]).IsNilV2(i)
			if empty {
				continue
			}
			v := src.Column(index[j]).BooleanValues()[src.Column(index[j]).GetValueIndexV2(i)]
			if isNil || v {
				val = v
				isNil = false
			}
		}
		if isNil {
			dst.AppendNil()
		} else {
			dst.AppendBooleanValue(val)
			dst.AppendNotNil()
		}
	}
}

func getMaxRowFunc(dataType influxql.DataType) func(dst Column, src Chunk, index []int) {
	switch dataType {
	case influxql.Integer:
		return rowMaxInteger
	case influxql.Float:
		return rowMaxFloat
	case influxql.Boolean:
		return rowMaxBoolean
	}
	return nil
}

func getRowValue(column Column, index int) interface{} {
	switch column.DataType() {
	case influxql.Integer:
		return column.IntegerValue(index)
	case influxql.Float:
		return column.FloatValue(index)
	case influxql.Boolean:
		return column.BooleanValue(index)
	case influxql.String, influxql.Tag:
		return column.StringValue(index)
	default:
		return nil
	}
}

func AppendRowValue(column Column, value interface{}) {
	switch column.DataType() {
	case influxql.Integer:
		if v, ok := value.(int64); ok {
			column.AppendIntegerValue(v)
		} else {
			panic("expect integer value")
		}
	case influxql.Float:
		if v, ok := value.(float64); ok {
			column.AppendFloatValue(v)
		} else {
			panic("expect float value")
		}
	case influxql.Boolean:
		if v, ok := value.(bool); ok {
			column.AppendBooleanValue(v)
		} else {
			panic("expect bool value")
		}
	case influxql.String, influxql.Tag:
		if v, ok := value.(string); ok {
			column.AppendStringValue(v)
		} else {
			panic("expect string value")
		}
	default:
	}
}

// ChunkValuer is a valuer that substitutes values for the mapped interface.
type ChunkValuer struct {
	ref   Chunk
	index int
}

func NewChunkValuer() *ChunkValuer {
	c := &ChunkValuer{
		ref:   nil,
		index: 0,
	}
	return c
}

func (c *ChunkValuer) AtChunkRow(chunk Chunk, index int) {
	c.ref = chunk
	c.index = index
}

// Value returns the value for a key in the MapValuer.
func (c *ChunkValuer) Value(key string) (interface{}, bool) {
	fieldIndex := c.ref.RowDataType().FieldIndex(key)
	column := c.ref.Columns()[fieldIndex]
	if column.IsNilV2(c.index) {
		return nil, false
	}
	// TODO: opt this code
	return getRowValue(column, column.GetValueIndexV2(c.index)), true
}

func (c *ChunkValuer) SetValuer(_ influxql.Valuer, _ int) {

}

type MaterializeTransform struct {
	BaseProcessor

	input           *ChunkPort
	output          *ChunkPort
	ops             []hybridqp.ExprOptions
	opt             *query.ProcessorOptions
	valuer          influxql.ValuerEval
	m               map[string]interface{}
	chunkValuer     *ChunkValuer
	chunkWriter     ChunkWriter
	resultChunkPool *CircularChunkPool
	resultChunk     Chunk
	forward         bool
	ResetTime       bool
	transparents    []func(dst Column, src Chunk, index []int)

	ColumnMap [][]int

	ppMaterializeCost *tracing.Span
}

func createTransparents(ops []hybridqp.ExprOptions) []func(dst Column, src Chunk, index []int) {
	transparents := make([]func(dst Column, src Chunk, index []int), len(ops))

	for i, opt := range ops {
		if vr, ok := opt.Expr.(*influxql.VarRef); ok {
			switch vr.Type {
			case influxql.Integer:
				transparents[i] = TransparentForwardInteger
			case influxql.Float:
				transparents[i] = TransparentForwardFloat
			case influxql.Boolean:
				transparents[i] = TransparentForwardBoolean
			case influxql.String, influxql.Tag:
				transparents[i] = TransparentForwardString
			}
		}
		if vr, ok := opt.Expr.(*influxql.Call); ok {
			f, err := TransMath(vr)
			if err != nil {
				panic(err)
			}
			transparents[i] = f
		}
	}

	return transparents
}

func NewMaterializeTransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, opt *query.ProcessorOptions, writer ChunkWriter, schema *QuerySchema) *MaterializeTransform {
	trans := &MaterializeTransform{
		input:           NewChunkPort(inRowDataType),
		output:          NewChunkPort(outRowDataType),
		ops:             ops,
		opt:             opt,
		m:               make(map[string]interface{}),
		chunkValuer:     NewChunkValuer(),
		resultChunkPool: NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		chunkWriter:     writer,
		forward:         false,
		transparents:    nil,
		ColumnMap:       make([][]int, len(outRowDataType.Fields())),
		ResetTime:       false,
	}

	trans.valuer = influxql.ValuerEval{
		Valuer: influxql.MultiValuer(
			op.Valuer{},
			query.MathValuer{},
			StringValuer{},
			trans.chunkValuer,
		),
		IntegerFloatDivision: true,
	}

	trans.transparents = createTransparents(trans.ops)

	if SetTimeZero(schema) {
		trans.ResetTime = true
	}

	trans.ColumnMapInit()
	return trans
}

type MaterializeTransformCreator struct {
}

func (c *MaterializeTransformCreator) Create(plan LogicalPlan, opt *query.ProcessorOptions) (Processor, error) {
	p := NewMaterializeTransform(plan.Children()[0].RowDataType(), plan.RowDataType(), plan.RowExprOptions(), opt, NewStdoutChunkWriter(), plan.Schema().(*QuerySchema))
	return p, nil
}

var (
	_ bool = RegistryTransformCreator(&LogicalProject{}, &MaterializeTransformCreator{})
)

func (trans *MaterializeTransform) Name() string {
	return "MaterializeTransform"
}

func (trans *MaterializeTransform) Explain() []ValuePair {
	pairs := make([]ValuePair, 0, len(trans.ops))
	for _, option := range trans.ops {
		pairs = append(pairs, ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

func (trans *MaterializeTransform) Close() {
	trans.output.Close()
}

func (trans *MaterializeTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[Materialize]TotalWorkCost", false)
	trans.ppMaterializeCost = tracing.Start(span, "materialize_cost", false)
	defer func() {
		tracing.Finish(span, trans.ppMaterializeCost)
		trans.Close()
	}()

	for {
		select {
		case chunk, ok := <-trans.input.State:
			if !ok {
				return nil
			}
			tracing.StartPP(span)

			tracing.SpanElapsed(trans.ppMaterializeCost, func() {
				trans.resultChunk = trans.materialize(chunk)
			})

			trans.output.State <- trans.resultChunk
			tracing.EndPP(span)
		case <-ctx.Done():
			return nil
		}
	}
}

func maxTime(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func (trans *MaterializeTransform) materialize(chunk Chunk) Chunk {
	oChunk := trans.resultChunkPool.GetChunk()
	oChunk.SetName(chunk.Name())
	if !trans.ResetTime {
		oChunk.AppendTimes(chunk.Time())
	} else {
		resetTimes := make([]int64, len(chunk.Time()))
		zeroTime := maxTime(ZeroTimeStamp, trans.opt.StartTime)
		for j := 0; j < len(chunk.Time()); j++ {
			resetTimes[j] = zeroTime
		}
		oChunk.AppendTimes(resetTimes)
	}
	oChunk.AppendTagsAndIndexes(chunk.Tags(), chunk.TagIndex())
	oChunk.AppendIntervalIndexes(chunk.IntervalIndex())

	for i, f := range trans.transparents {
		dst := oChunk.Column(i)
		if f != nil {
			f(dst, chunk, trans.ColumnMap[i])
		} else {
			for index := 0; index < chunk.NumberOfRows(); index++ {
				trans.chunkValuer.AtChunkRow(chunk, index)
				value := trans.valuer.Eval(trans.ops[i].Expr)
				if value == nil {
					dst.AppendNil()
					continue
				}
				if val, ok := value.(float64); ok && math.IsNaN(val) {
					dst.AppendNil()
					continue
				}
				AppendRowValue(dst, value)
				dst.AppendNotNil()
			}
		}
	}

	return oChunk
}

func (trans *MaterializeTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *MaterializeTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *MaterializeTransform) GetOutputNumber(_ Port) int {
	return INVALID_NUMBER
}

func (trans *MaterializeTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *MaterializeTransform) ColumnMapInit() {
	for i := range trans.ops {
		if val, ok := trans.ops[i].Expr.(*influxql.VarRef); ok {
			trans.ColumnMap[i] = []int{trans.input.RowDataType.FieldIndex(val.Val)}
			continue
		}
		if val, ok := trans.ops[i].Expr.(*influxql.Call); ok {
			index := make([]int, 0, len(val.Args))
			for j := range val.Args {
				if v, k := val.Args[j].(*influxql.VarRef); k {
					index = append(index, trans.input.RowDataType.FieldIndex(v.Val))
				}
			}
			trans.ColumnMap[i] = index
		}
	}
}
