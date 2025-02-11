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

package executor

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/op"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
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

func AdjustNils(dst Column, src Column, low int, high int) {
	if high > src.Length() || high <= 0 || low < 0 || low >= high {
		return
	}
	if dst.Length() == 0 && low == 0 && high == src.Length() {
		src.BitMap().CopyTo(dst.BitMap())
		return
	}
	if src.NilCount() == 0 {
		dst.AppendManyNotNil(src.Length())
	} else {
		if cap(dst.BitMap().bits) < len(dst.BitMap().bits)+high-low {
			s := make([]byte, len(dst.BitMap().bits), len(dst.BitMap().bits)+high-low)
			copy(s, dst.BitMap().bits)
			dst.BitMap().bits = s
		}
		if cap(dst.BitMap().array) < len(dst.BitMap().array)+high-low {
			s := make([]uint16, len(dst.BitMap().array), len(dst.BitMap().array)+high-low)
			copy(s, dst.BitMap().array)
			dst.BitMap().array = s
		}
		l, h := src.GetRangeValueIndexV2(low, high)
		a := src.BitMap().array
		num := 0
		iRange := l
		for i := low; i < high; {
			if iRange == h {
				dst.AppendManyNil(high - i)
				return
			}
			//Calculate the distance from the next non-null value to i
			num = int(a[iRange]) - i
			dst.AppendManyNil(num)
			i = int(a[iRange])
			if h-iRange == high-i {
				dst.AppendManyNotNil(high - i)
				return
			}
			//Calculate the number of non-null values. If i=a[l], i is a non-null value.
			num = 0
			for i < high && iRange < h && i == int(a[iRange]) {
				num += 1
				i += 1
				iRange += 1
			}
			dst.AppendManyNotNil(num)
		}
	}
}

func ResetTime(srcCk, dstCk Chunk, ts int64) {
	resetTimes := make([]int64, len(srcCk.Time()))
	zeroTime := maxTime(ZeroTimeStamp, ts)
	for j := 0; j < len(srcCk.Time()); j++ {
		resetTimes[j] = zeroTime
	}
	dstCk.AppendTimes(resetTimes)
}

func ResetTimeForProm(srcCk, dstCk Chunk, ts int64) {
	dstTimeCol := dstCk.Time()
	timeColLen := len(dstTimeCol)
	dstTimeCol = record.ReserveInt64Slice(dstTimeCol, len(srcCk.Time()))
	for j := timeColLen; j < len(dstTimeCol); j++ {
		dstTimeCol[j] = ts
	}
	dstCk.SetTime(dstTimeCol)
}

func TransparentForwardIntegerColumn(dst Column, src Column) {
	dst.AppendIntegerValues(src.IntegerValues())
	AdjustNils(dst, src, 0, src.Length())
}

func TransparentForwardFloatColumn(dst Column, src Column) {
	dst.AppendFloatValues(src.FloatValues())
	AdjustNils(dst, src, 0, src.Length())
}

func TransparentForwardBooleanColumn(dst Column, src Column) {
	dst.AppendBooleanValues(src.BooleanValues())
	AdjustNils(dst, src, 0, src.Length())
}

func TransparentForwardStringColumn(dst Column, src Column) {
	dst.CloneStringValues(src.GetStringBytes())
	AdjustNils(dst, src, 0, src.Length())
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
	value func(key string) (interface{}, bool)
}

func NewChunkValuer(isPromQuery bool) *ChunkValuer {
	c := &ChunkValuer{
		ref:   nil,
		index: 0,
	}
	if !isPromQuery {
		c.value = c.ValueNormal
	} else {
		c.value = c.ValueProm
	}
	return c
}

func (c *ChunkValuer) AtChunkRow(chunk Chunk, index int) {
	c.ref = chunk
	c.index = index
}

// Value returns the value for a key in the MapValuer.
func (c *ChunkValuer) Value(key string) (interface{}, bool) {
	return c.value(key)
}

func (c *ChunkValuer) ValueNormal(key string) (interface{}, bool) {
	fieldIndex := c.ref.RowDataType().FieldIndex(key)
	column := c.ref.Columns()[fieldIndex]
	if column.IsNilV2(c.index) {
		return nil, false
	}
	// TODO: opt this code
	return getRowValue(column, column.GetValueIndexV2(c.index)), true
}

func (c *ChunkValuer) ValueProm(key string) (interface{}, bool) {
	if key == promql2influxql.ArgNameOfTimeFunc {
		t := float64(c.ref.Time()[c.index]) / 1000000000
		return t, true
	} else {
		return c.ValueNormal(key)
	}
}

func (c *ChunkValuer) SetValueFnOnlyPromTime() {
	c.value = c.ValuePromTime
}

func (c *ChunkValuer) ValuePromTime(key string) (interface{}, bool) {
	if key == promql2influxql.ArgNameOfTimeFunc {
		t := float64(c.ref.Time()[c.index]) / 1000000000
		return t, true
	}
	return nil, false
}

func (c *ChunkValuer) SetValuer(_ influxql.Valuer, _ int) {

}

type MaterializeTransform struct {
	BaseProcessor

	input           *ChunkPort
	output          *ChunkPort
	ops             []hybridqp.ExprOptions
	opt             *query.ProcessorOptions
	schema          *QuerySchema
	valuer          influxql.ValuerEval
	m               map[string]interface{}
	chunkValuer     *ChunkValuer
	chunkWriter     ChunkWriter
	resultChunkPool *CircularChunkPool
	resultChunk     Chunk
	forward         bool
	ResetTime       bool
	isPromQuery     bool
	HasBinaryExpr   bool
	removeMetric    bool
	transparents    []func(dst Column, src Chunk, index []int)

	ColumnMap [][]int

	promQueryTime     int64
	ppMaterializeCost *tracing.Span
	rp                *ResultEvalPool
	labelCall         *influxql.Call
}

func (trans *MaterializeTransform) createTransparents(ops []hybridqp.ExprOptions) []func(dst Column, src Chunk, index []int) {
	transparents := make([]func(dst Column, src Chunk, index []int), len(ops))

	for i, opt := range ops {
		switch vr := opt.Expr.(type) {
		case *influxql.VarRef:
			trans.processVarRef(vr, transparents, i)
		case *influxql.Call:
			trans.processCall(vr, transparents, i)
		case *influxql.BinaryExpr:
			trans.HasBinaryExpr = true
		default:
			return transparents
		}
	}

	return transparents
}

func ChangeCallExprForTimestamp(call *influxql.Call) {
	if call.Name != "timestamp_prom" {
		return
	}

	call.Args = []influxql.Expr{
		&influxql.VarRef{
			Val:  "prom_time",
			Type: influxql.Float,
		},
	}
}

func TranverseBinTreeForTimestamp(expr influxql.Expr) {
	switch ex := expr.(type) {
	case *influxql.Call:
		ChangeCallExprForTimestamp(ex)
	case *influxql.BinaryExpr:
		TranverseBinTreeForTimestamp(ex.LHS)
		TranverseBinTreeForTimestamp(ex.RHS)
	case *influxql.ParenExpr:
		TranverseBinTreeForTimestamp(ex.Expr)
	default:
		return
	}
}

func ChangeOpsForTimestamp(ops []hybridqp.ExprOptions) []hybridqp.ExprOptions {
	for _, expr := range ops {
		TranverseBinTreeForTimestamp(expr.Expr)
	}
	return ops
}

func NewMaterializeTransform(inRowDataType hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, opt *query.ProcessorOptions, writer ChunkWriter, schema *QuerySchema) *MaterializeTransform {
	newOps := ChangeOpsForTimestamp(ops)
	trans := &MaterializeTransform{
		input:           NewChunkPort(inRowDataType),
		output:          NewChunkPort(outRowDataType),
		ops:             newOps,
		opt:             opt,
		schema:          schema,
		m:               make(map[string]interface{}),
		chunkValuer:     NewChunkValuer(opt.IsPromQuery()),
		resultChunkPool: NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		chunkWriter:     writer,
		forward:         false,
		transparents:    nil,
		ColumnMap:       make([][]int, len(outRowDataType.Fields())),
		ResetTime:       false,
		isPromQuery:     schema.Options().IsPromQuery(),
		rp:              NewResultEvalPool(1, schema.Options().IsPromQuery()),
	}

	trans.valuer = influxql.ValuerEval{
		Valuer: influxql.MultiValuer(
			op.Valuer{},
			query.MathValuer{},
			query.StringValuer{},
			LabelValuer{},
			PromTimeValuer{},
			trans.chunkValuer,
		),
		IntegerFloatDivision: true,
	}

	trans.transparents = trans.createTransparents(trans.ops)

	if SetTimeZero(schema) {
		trans.ResetTime = true
	}
	if trans.schema.PromResetTime() {
		trans.promQueryTime = trans.opt.EndTime + trans.opt.QueryOffset.Nanoseconds()
	}
	trans.ColumnMapInit()
	trans.removeMetric = trans.opt.IsPromQuery() && trans.HasBinaryExpr
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

			tracing.StartPP(trans.ppMaterializeCost)
			trans.resultChunk = trans.materialize(chunk)
			if trans.labelCall != nil && ContainsSameTagset(trans.resultChunk) {
				return errno.NewError(errno.ErrSameTagSet)
			}
			tracing.EndPP(trans.ppMaterializeCost)

			trans.output.State <- trans.resultChunk
			tracing.EndPP(span)
		case <-ctx.Done():
			return nil
		}
	}
}

func ContainsSameTagset(chunk Chunk) bool {
	tagMap := make(map[string]struct{})
	for _, tag := range chunk.Tags() {
		s := util.Bytes2str(tag.Subset(nil))
		if _, ok := tagMap[s]; ok {
			return true
		}
		tagMap[s] = struct{}{}
	}
	return false
}

func (trans *MaterializeTransform) processVarRef(vr *influxql.VarRef, transparents []func(dst Column, src Chunk, index []int), i int) {
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

func (trans *MaterializeTransform) processCall(vr *influxql.Call, transparents []func(dst Column, src Chunk, index []int), i int) {
	if labelFunc := query.GetLabelFunction(vr.Name); labelFunc != nil {
		trans.labelCall = vr
	}

	f, err := TransMath(vr)
	if err != nil {
		panic(err)
	}
	transparents[i] = f
}

func maxTime(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func (trans *MaterializeTransform) ResetTransparents() {
	for i := range trans.ops {
		trans.transparents[i] = nil
	}
}

type ResultEval struct {
	dataType     influxql.DataType
	floatValue   []float64
	integerValue []int64
	booleanValue []bool
	uintValue    []uint64
	isNil        []bool
	isLiteral    bool
	intLiteral   int64
	floatLiteral float64
	boolLiteral  bool
	uintLiteral  uint64
	isPromQuery  bool
}

type ResultEvalPool struct {
	resultEvals []*ResultEval
	index       int
	isPromQuery bool
}

func NewResultEvalPool(len int, isPromQuery bool) *ResultEvalPool {
	return &ResultEvalPool{isPromQuery: isPromQuery}
}

func (rp *ResultEvalPool) getResultEval(dataType influxql.DataType) *ResultEval {
	if rp.index >= len(rp.resultEvals) {
		rp.resultEvals = append(rp.resultEvals, &ResultEval{isPromQuery: rp.isPromQuery})
	}
	res := rp.resultEvals[rp.index]
	rp.index += 1
	res.dataType = dataType
	res.isLiteral = false
	return res
}

func (re *ResultEval) appendIntLen(l int) {
	if cap(re.integerValue) < l {
		re.integerValue = make([]int64, l)
	} else {
		for i := len(re.integerValue); i < l; i++ {
			re.integerValue = append(re.integerValue, 0)
		}
	}
	re.appendNilLen(l)
}

func (re *ResultEval) appendFloatLen(l int) {
	if cap(re.floatValue) < l {
		re.floatValue = make([]float64, l)
	} else {
		for i := len(re.floatValue); i < l; i++ {
			re.floatValue = append(re.floatValue, 0)
		}
	}
	re.appendNilLen(l)
}

func (re *ResultEval) appendBoolLen(l int) {
	if cap(re.booleanValue) < l {
		re.booleanValue = make([]bool, l)
	} else {
		for i := len(re.booleanValue); i < l; i++ {
			re.booleanValue = append(re.booleanValue, false)
		}
	}
	re.appendNilLen(l)
}

func (re *ResultEval) appendUintLen(l int) {
	if cap(re.uintValue) < l {
		re.uintValue = make([]uint64, l)
	} else {
		for i := len(re.uintValue); i < l; i++ {
			re.uintValue = append(re.uintValue, 0)
		}
	}
	re.appendNilLen(l)
}

func (re *ResultEval) appendNilLen(l int) {
	if cap(re.isNil) < l {
		re.isNil = make([]bool, l)
	} else {
		for i := len(re.isNil); i < l; i++ {
			re.isNil = append(re.isNil, false)
		}
	}
}

func (re *ResultEval) IsNil(i int) bool {
	if re.isLiteral {
		return false
	}
	return re.isNil[i]
}

func (re *ResultEval) appendLen(l int, dataType influxql.DataType) {
	switch dataType {
	case influxql.Float:
		re.appendFloatLen(l)
	case influxql.Integer:
		re.appendIntLen(l)
	case influxql.Boolean:
		re.appendBoolLen(l)
	}
}

func (re *ResultEval) copy(column *ColumnImpl) {
	if column.NilCount() == 0 {
		for i := 0; i < column.Length(); i++ {
			re.isNil[i] = false
		}
		switch column.DataType() {
		case influxql.Float:
			copy(re.floatValue, column.floatValues)
		case influxql.Integer:
			copy(re.integerValue, column.integerValues)
		case influxql.Boolean:
			copy(re.booleanValue, column.booleanValues)
		}
		return
	}
	k := 0
	switch column.DataType() {
	case influxql.Float:
		for i := 0; i < column.Length(); i++ {
			if column.IsNilV2(i) {
				re.isNil[i] = true
				continue
			}
			re.isNil[i] = false
			re.floatValue[i] = column.FloatValue(k)
			k = k + 1
		}
	case influxql.Integer:
		for i := 0; i < column.Length(); i++ {
			if column.IsNilV2(i) {
				re.isNil[i] = true
				continue
			}
			re.isNil[i] = false
			re.integerValue[i] = column.IntegerValue(k)
			k = k + 1
		}
	case influxql.Boolean:
		for i := 0; i < column.Length(); i++ {
			if column.IsNilV2(i) {
				re.isNil[i] = true
				continue
			}
			re.isNil[i] = false
			re.booleanValue[i] = column.BooleanValue(k)
			k = k + 1
		}
	}
}

func (res *ResultEval) copyTo(l int, dst *ColumnImpl) {
	switch res.dataType {
	case influxql.Float:
		res.copyToForFloat(l, dst)
	case influxql.Integer:
		res.copyToForInteger(l, dst)
	case influxql.Boolean:
		res.copyToForBoolean(l, dst)
	}
}

func (res *ResultEval) copyToForFloat(l int, dst *ColumnImpl) {
	for index := 0; index < l; {
		num := 0
		for index < l && res.IsNil(index) {
			index += 1
			num += 1
		}
		dst.AppendManyNil(num)
		num = 0
		for index < l && !res.IsNil(index) {
			value := res.getFloat64(index)
			dst.AppendFloatValue(value)
			num += 1
			index += 1
		}
		dst.AppendManyNotNil(num)
	}
}

func (res *ResultEval) copyToForInteger(l int, dst *ColumnImpl) {
	for index := 0; index < l; {
		num := 0
		for index < l && res.IsNil(index) {
			index += 1
			num += 1
		}
		dst.AppendManyNil(num)
		num = 0
		for index < l && !res.IsNil(index) {
			dst.AppendIntegerValue(res.getInteger(index))
			index += 1
			num += 1
		}
		dst.AppendManyNotNil(num)
	}
}

func (res *ResultEval) copyToForBoolean(l int, dst *ColumnImpl) {
	for index := 0; index < l; {
		num := 0
		for index < l && res.IsNil(index) {
			index += 1
			num += 1
		}
		dst.AppendManyNil(num)
		num = 0
		for index < l && !res.IsNil(index) {
			dst.AppendBooleanValue(res.getBoolean(index))
			index += 1
			num += 1
		}
		dst.AppendManyNotNil(num)
	}
}

// Eval evaluates an expression and returns a value.
func eval(expr influxql.Expr, v *influxql.ValuerEval, columnMap map[string]*ColumnImpl, l int, rp *ResultEvalPool) *ResultEval {
	if expr == nil {
		columnResult := rp.getResultEval(influxql.Float)
		columnResult.appendNilLen(l)
		for i := 0; i < l; i++ {
			columnResult.isNil[i] = true
		}
		columnResult.isLiteral = false
		return columnResult
	}
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		return evalBinaryExpr(expr, v, columnMap, l, rp)
	case *influxql.BooleanLiteral:
		columnResult := rp.getResultEval(influxql.Boolean)
		columnResult.boolLiteral = expr.Val
		columnResult.isLiteral = true
		return columnResult
	case *influxql.IntegerLiteral:
		columnResult := rp.getResultEval(influxql.Integer)
		columnResult.intLiteral = expr.Val
		columnResult.isLiteral = true
		return columnResult
	case *influxql.NumberLiteral:
		columnResult := rp.getResultEval(influxql.Float)
		columnResult.floatLiteral = expr.Val
		columnResult.isLiteral = true
		return columnResult
	case *influxql.UnsignedLiteral:
		columnResult := rp.getResultEval(influxql.Unsigned)
		columnResult.uintLiteral = expr.Val
		columnResult.isLiteral = true
		return columnResult
	case *influxql.VarRef:
		column := columnMap[expr.Val]
		columnResult := rp.getResultEval(column.DataType())
		columnResult.appendLen(l, column.DataType())
		columnResult.copy(column)
		return columnResult
	default:
		return nil
	}
}

func isSwitchToFloat64(lhs *ResultEval, l int) bool {
	if lhs.dataType == influxql.Float || lhs.dataType == influxql.Integer || lhs.dataType == influxql.Unsigned {
		return true
	}
	return false
}

func isSwitchToUint64(lhs *ResultEval, l int) bool {
	if lhs.dataType == influxql.Unsigned || lhs.dataType == influxql.Integer {
		return true
	}
	return false
}

func (re *ResultEval) getFloat64(i int) float64 {
	if re.isLiteral {
		switch re.dataType {
		case influxql.Float:
			return re.floatLiteral
		case influxql.Integer:
			return float64(re.intLiteral)
		case influxql.Unsigned:
			return float64(re.uintLiteral)
		}
	} else {
		switch re.dataType {
		case influxql.Float:
			return re.floatValue[i]
		case influxql.Integer:
			return float64(re.integerValue[i])
		case influxql.Unsigned:
			return float64(re.uintValue[i])
		}
	}
	return 0
}

func (re *ResultEval) getUint64(i int) uint64 {
	if re.isLiteral {
		switch re.dataType {
		case influxql.Unsigned:
			return re.uintLiteral
		case influxql.Integer:
			return uint64(re.intLiteral)
		}
	} else {
		switch re.dataType {
		case influxql.Unsigned:
			return re.uintValue[i]
		case influxql.Integer:
			return uint64(re.integerValue[i])
		}
	}
	return 0
}

func (re *ResultEval) getInteger(i int) int64 {
	if re.isLiteral {
		if re.dataType == influxql.Integer {
			return re.intLiteral
		}
	} else {
		if re.dataType == influxql.Integer {
			return re.integerValue[i]
		}
	}
	return 0
}

func (re *ResultEval) getBoolean(i int) bool {
	if re.isLiteral {
		if re.dataType == influxql.Boolean {
			return re.boolLiteral
		}
	} else {
		if re.dataType == influxql.Boolean {
			return re.booleanValue[i]
		}
	}
	return false
}

// Calculate && and || for bool types. If both of them are nil, the result is nil.
// If one of them is bool and the other is nil, nil is equivalent to false.
func resultEvalANDAndOR(lhs *ResultEval, rhs *ResultEval, l int, op influxql.Token) {
	if lhs.dataType == influxql.Boolean && rhs.dataType == influxql.Boolean {
		lhs.appendBoolLen(l)
		rhs.appendBoolLen(l)
		for i := 0; i < l; i++ {
			if lhs.IsNil(i) && rhs.IsNil(i) {
				continue
			}
			if lhs.IsNil(i) && !rhs.IsNil(i) {
				lhs.isNil[i] = false
				lhs.booleanValue[i] = false
			}
			if !lhs.IsNil(i) && rhs.IsNil(i) {
				rhs.isNil[i] = false
				rhs.booleanValue[i] = false
			}
			switch op {
			case influxql.AND:
				lhs.booleanValue[i] = lhs.getBoolean(i) && rhs.getBoolean(i)
			case influxql.OR:
				lhs.booleanValue[i] = lhs.getBoolean(i) || rhs.getBoolean(i)
			}
		}
		return
	}
	lhs.appendNilLen(l)
	for i := 0; i < l; i++ {
		lhs.isNil[i] = true
	}
}

func resultEvalBITWISE(lhs *ResultEval, rhs *ResultEval, l int, op influxql.Token) {
	if lhs.dataType == influxql.Boolean && rhs.dataType == influxql.Boolean {
		lhs.appendBoolLen(l)
		rhs.appendBoolLen(l)
		for i := 0; i < l; i++ {
			if lhs.IsNil(i) && rhs.IsNil(i) {
				continue
			}
			if lhs.IsNil(i) && !rhs.IsNil(i) {
				lhs.isNil[i] = false
				lhs.booleanValue[i] = false
			}
			if !lhs.IsNil(i) && rhs.IsNil(i) {
				rhs.isNil[i] = false
				rhs.booleanValue[i] = false
			}
			switch op {
			case influxql.BITWISE_AND:
				lhs.booleanValue[i] = lhs.getBoolean(i) && rhs.getBoolean(i)
			case influxql.BITWISE_OR:
				lhs.booleanValue[i] = lhs.getBoolean(i) || rhs.getBoolean(i)
			case influxql.BITWISE_XOR:
				lhs.booleanValue[i] = lhs.getBoolean(i) != rhs.getBoolean(i)
			}
		}
		return
	} else if lhs.dataType == influxql.Float || rhs.dataType == influxql.Float {
		lhs.appendNilLen(l)
		for i := 0; i < l; i++ {
			lhs.isNil[i] = true
		}
		return
	} else if lhs.dataType == influxql.Unsigned || rhs.dataType == influxql.Unsigned {
		if isSwitchToUint64(lhs, l) && isSwitchToUint64(rhs, l) {
			lhs.appendUintLen(l)
			for i := 0; i < l; i++ {
				if lhs.IsNil(i) || rhs.IsNil(i) {
					lhs.isNil[i] = true
					continue
				}
				switch op {
				case influxql.BITWISE_AND:
					lhs.uintValue[i] = lhs.getUint64(i) & rhs.getUint64(i)
				case influxql.BITWISE_OR:
					lhs.uintValue[i] = lhs.getUint64(i) | rhs.getUint64(i)
				case influxql.BITWISE_XOR:
					lhs.uintValue[i] = lhs.getUint64(i) ^ rhs.getUint64(i)
				}
			}
			lhs.dataType = influxql.Unsigned
			return
		}
	} else if lhs.dataType == influxql.Integer && rhs.dataType == influxql.Integer {
		lhs.appendIntLen(l)
		for i := 0; i < l; i++ {
			if lhs.IsNil(i) || rhs.IsNil(i) {
				lhs.isNil[i] = true
				continue
			}
			switch op {
			case influxql.BITWISE_AND:
				lhs.integerValue[i] = lhs.getInteger(i) & rhs.getInteger(i)
			case influxql.BITWISE_OR:
				lhs.integerValue[i] = lhs.getInteger(i) | rhs.getInteger(i)
			case influxql.BITWISE_XOR:
				lhs.integerValue[i] = lhs.getInteger(i) ^ rhs.getInteger(i)
			}
		}
		return
	}
	lhs.appendNilLen(l)
	for i := 0; i < l; i++ {
		lhs.isNil[i] = true
	}
}

func resultEvalCompare(lhs *ResultEval, rhs *ResultEval, l int, op influxql.Token) {
	if lhs.dataType == influxql.Boolean && rhs.dataType == influxql.Boolean {
		lhs.appendBoolLen(l)
		rhs.appendBoolLen(l)
		for i := 0; i < l; i++ {
			if lhs.IsNil(i) && rhs.IsNil(i) {
				continue
			}
			if lhs.IsNil(i) && !rhs.IsNil(i) {
				lhs.isNil[i] = false
				lhs.booleanValue[i] = false
			}
			if !lhs.IsNil(i) && rhs.IsNil(i) {
				rhs.isNil[i] = false
				rhs.booleanValue[i] = false
			}
			switch op {
			case influxql.EQ:
				lhs.booleanValue[i] = lhs.getBoolean(i) == rhs.getBoolean(i)
			case influxql.NEQ:
				lhs.booleanValue[i] = lhs.getBoolean(i) != rhs.getBoolean(i)
			default:
				lhs.booleanValue[i] = false
			}
		}
		return
	} else if isSwitchToFloat64(lhs, l) && isSwitchToFloat64(rhs, l) {
		lhs.appendBoolLen(l)
		for i := 0; i < l; i++ {
			if lhs.IsNil(i) || rhs.IsNil(i) {
				lhs.booleanValue[i] = false
				continue
			}
			switch op {
			case influxql.EQ:
				lhs.booleanValue[i] = lhs.getFloat64(i) == rhs.getFloat64(i)
			case influxql.NEQ:
				lhs.booleanValue[i] = lhs.getFloat64(i) != rhs.getFloat64(i)
			case influxql.LT:
				lhs.booleanValue[i] = lhs.getFloat64(i) < rhs.getFloat64(i)
			case influxql.LTE:
				lhs.booleanValue[i] = lhs.getFloat64(i) <= rhs.getFloat64(i)
			case influxql.GT:
				lhs.booleanValue[i] = lhs.getFloat64(i) > rhs.getFloat64(i)
			case influxql.GTE:
				lhs.booleanValue[i] = lhs.getFloat64(i) >= rhs.getFloat64(i)
			}
		}
		lhs.dataType = influxql.Boolean
		return
	}
	lhs.dataType = influxql.Boolean
	lhs.appendNilLen(l)
	for i := 0; i < l; i++ {
		lhs.booleanValue[i] = false
	}
}

func resultEvalASM(lhs *ResultEval, rhs *ResultEval, l int, op influxql.Token) {
	if lhs.dataType == influxql.Float || rhs.dataType == influxql.Float {
		if isSwitchToFloat64(lhs, l) && isSwitchToFloat64(rhs, l) {
			lhs.appendFloatLen(l)
			for i := 0; i < l; i++ {
				if lhs.IsNil(i) || rhs.IsNil(i) {
					lhs.isNil[i] = true
					continue
				}
				switch op {
				case influxql.ADD:
					lhs.floatValue[i] = lhs.getFloat64(i) + rhs.getFloat64(i)
				case influxql.SUB:
					lhs.floatValue[i] = lhs.getFloat64(i) - rhs.getFloat64(i)
				case influxql.MUL:
					lhs.floatValue[i] = lhs.getFloat64(i) * rhs.getFloat64(i)
				}
			}
			lhs.dataType = influxql.Float
			return
		}
	} else if lhs.dataType == influxql.Unsigned || rhs.dataType == influxql.Unsigned {
		if isSwitchToUint64(lhs, l) && isSwitchToUint64(rhs, l) {
			lhs.appendUintLen(l)
			for i := 0; i < l; i++ {
				if lhs.IsNil(i) || rhs.IsNil(i) {
					lhs.isNil[i] = true
					continue
				}
				switch op {
				case influxql.ADD:
					lhs.uintValue[i] = lhs.getUint64(i) + rhs.getUint64(i)
				case influxql.SUB:
					lhs.uintValue[i] = lhs.getUint64(i) - rhs.getUint64(i)
				case influxql.MUL:
					lhs.uintValue[i] = lhs.getUint64(i) * rhs.getUint64(i)
				}
			}
			lhs.dataType = influxql.Unsigned
			return
		}
	} else if lhs.dataType == influxql.Integer && rhs.dataType == influxql.Integer {
		lhs.appendIntLen(l)
		for i := 0; i < l; i++ {
			if lhs.IsNil(i) || rhs.IsNil(i) {
				lhs.isNil[i] = true
				continue
			}
			switch op {
			case influxql.ADD:
				lhs.integerValue[i] = lhs.getInteger(i) + rhs.getInteger(i)
			case influxql.SUB:
				lhs.integerValue[i] = lhs.getInteger(i) - rhs.getInteger(i)
			case influxql.MUL:
				lhs.integerValue[i] = lhs.getInteger(i) * rhs.getInteger(i)
			}
		}
		return
	}
	lhs.appendNilLen(l)
	for i := 0; i < l; i++ {
		lhs.isNil[i] = true
	}
}

func resultEvalDIV(lhs *ResultEval, rhs *ResultEval, l int, IntegerFloatDivision bool) {
	if lhs.dataType == influxql.Float || rhs.dataType == influxql.Float || (IntegerFloatDivision && lhs.dataType == influxql.Integer && rhs.dataType == influxql.Integer) {
		if IntegerFloatDivision && lhs.dataType == influxql.Integer && rhs.dataType == influxql.Integer {
			lhs.appendFloatLen(l)
			for i := 0; i < l; i++ {
				if lhs.IsNil(i) || rhs.IsNil(i) {
					lhs.isNil[i] = true
					continue
				}
				lhs.floatValue[i] = float64(lhs.getInteger(i)) / float64(rhs.getInteger(i))
			}

			lhs.dataType = influxql.Float
			return
		}
		if isSwitchToFloat64(lhs, l) && isSwitchToFloat64(rhs, l) {
			lhs.appendFloatLen(l)
			for i := 0; i < l; i++ {
				if lhs.IsNil(i) || rhs.IsNil(i) {
					lhs.isNil[i] = true
					continue
				}
				lhs.floatValue[i] = lhs.getFloat64(i) / rhs.getFloat64(i)
			}
			lhs.dataType = influxql.Float
			return
		}
	} else if lhs.dataType == influxql.Unsigned || rhs.dataType == influxql.Unsigned {
		if isSwitchToUint64(lhs, l) && isSwitchToUint64(rhs, l) {
			lhs.appendUintLen(l)
			for i := 0; i < l; i++ {
				if lhs.IsNil(i) || rhs.IsNil(i) {
					lhs.isNil[i] = true
					continue
				}
				if rhs.getUint64(i) == 0 {
					lhs.uintValue[i] = uint64(0)
				} else {
					lhs.uintValue[i] = lhs.getUint64(i) / rhs.getUint64(i)
				}
			}
			lhs.dataType = influxql.Unsigned
			return
		}
	} else if lhs.dataType == influxql.Integer && rhs.dataType == influxql.Integer {
		lhs.appendIntLen(l)
		for i := 0; i < l; i++ {
			if lhs.IsNil(i) || rhs.IsNil(i) {
				lhs.isNil[i] = true
				continue
			}
			if rhs.getInteger(i) == 0 {
				lhs.integerValue[i] = int64(0)
			} else {
				lhs.integerValue[i] = lhs.getInteger(i) / rhs.getInteger(i)
			}
		}
		return
	}
	lhs.appendNilLen(l)
	for i := 0; i < l; i++ {
		lhs.isNil[i] = true
	}
}

func resultEvalMOD(lhs *ResultEval, rhs *ResultEval, l int) {
	if lhs.dataType == influxql.Float || rhs.dataType == influxql.Float {
		if isSwitchToFloat64(lhs, l) && isSwitchToFloat64(rhs, l) {
			lhs.appendFloatLen(l)
			for i := 0; i < l; i++ {
				if lhs.IsNil(i) || rhs.IsNil(i) {
					lhs.isNil[i] = true
					continue
				}
				lhs.floatValue[i] = math.Mod(lhs.getFloat64(i), rhs.getFloat64(i))
			}
			lhs.dataType = influxql.Float
			return
		}
	} else if lhs.dataType == influxql.Unsigned || rhs.dataType == influxql.Unsigned {
		if isSwitchToUint64(lhs, l) && isSwitchToUint64(rhs, l) {
			lhs.appendUintLen(l)
			for i := 0; i < l; i++ {
				if lhs.IsNil(i) || rhs.IsNil(i) {
					lhs.isNil[i] = true
					continue
				}
				if rhs.getUint64(i) == 0 {
					lhs.uintValue[i] = uint64(0)
				} else {
					lhs.uintValue[i] = lhs.getUint64(i) % rhs.getUint64(i)
				}
			}
			lhs.dataType = influxql.Unsigned
			return
		}
	} else if lhs.dataType == influxql.Integer && rhs.dataType == influxql.Integer {
		lhs.appendIntLen(l)
		for i := 0; i < l; i++ {
			if lhs.IsNil(i) || rhs.IsNil(i) {
				lhs.isNil[i] = true
				continue
			}
			if rhs.getInteger(i) == 0 {
				lhs.integerValue[i] = int64(0)
			} else {
				lhs.integerValue[i] = lhs.getInteger(i) % rhs.getInteger(i)
			}
		}
		return
	}

	lhs.appendNilLen(l)
	for i := 0; i < l; i++ {
		lhs.isNil[i] = true
	}
}

func f(lhs *ResultEval, rhs *ResultEval, l int, op influxql.Token, v *influxql.ValuerEval) {
	switch op {
	case influxql.AND, influxql.OR:
		resultEvalANDAndOR(lhs, rhs, l, op)
	case influxql.EQ, influxql.NEQ, influxql.LT, influxql.LTE, influxql.GT, influxql.GTE:
		resultEvalCompare(lhs, rhs, l, op)
	case influxql.ADD, influxql.SUB, influxql.MUL:
		resultEvalASM(lhs, rhs, l, op)
	case influxql.DIV:
		resultEvalDIV(lhs, rhs, l, v.IntegerFloatDivision)
	case influxql.MOD:
		resultEvalMOD(lhs, rhs, l)
	case influxql.BITWISE_AND, influxql.BITWISE_OR, influxql.BITWISE_XOR:
		resultEvalBITWISE(lhs, rhs, l, op)
	default:
		lhs.appendNilLen(l)
		for i := 0; i < l; i++ {
			lhs.isNil[i] = true
		}
	}
}

func evalBinaryExpr(expr *influxql.BinaryExpr, v *influxql.ValuerEval, columnMap map[string]*ColumnImpl, l int, rp *ResultEvalPool) *ResultEval {
	lhs := eval(expr.LHS, v, columnMap, l, rp)
	rhs := eval(expr.RHS, v, columnMap, l, rp)
	f(lhs, rhs, l, expr.Op, v)
	// promql bool modifier rewriting results, true:1, false:0.
	if expr.ReturnBool {
		lhs.dataType = influxql.Float
		lhs.floatValue = lhs.floatValue[:0]
		for i := range lhs.booleanValue {
			if lhs.booleanValue[i] {
				lhs.floatValue = append(lhs.floatValue, float64(1))
			} else {
				lhs.floatValue = append(lhs.floatValue, float64(0))
			}
		}
	}
	lhs.isLiteral = false
	return lhs

}
func initByType(expr influxql.Expr, chunkValuer *ChunkValuer, columnMap map[string]*ColumnImpl) bool {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		lhs := initByType(expr.LHS, chunkValuer, columnMap)
		rhs := initByType(expr.RHS, chunkValuer, columnMap)
		return lhs && rhs
	case *influxql.BooleanLiteral:
		return true
	case *influxql.IntegerLiteral:
		return true
	case *influxql.NumberLiteral:
		return true
	case *influxql.UnsignedLiteral:
		return true
	case *influxql.VarRef:
		fieldIndex := chunkValuer.ref.RowDataType().FieldIndex(expr.Val)
		if (chunkValuer.ref.Columns()[fieldIndex]).DataType() != influxql.Float && (chunkValuer.ref.Columns()[fieldIndex]).DataType() != influxql.Integer && (chunkValuer.ref.Columns()[fieldIndex]).DataType() != influxql.Boolean {
			return false
		}
		if _, ok := columnMap[expr.Val]; !ok {
			columnMap[expr.Val] = chunkValuer.ref.Columns()[fieldIndex].(*ColumnImpl)
		}
		return true
	default:
		return false
	}
}

func (trans *MaterializeTransform) materialize(chunk Chunk) Chunk {
	oChunk := trans.resultChunkPool.GetChunk()
	oChunk.SetName(chunk.Name())
	if !trans.ResetTime {
		oChunk.AppendTimes(chunk.Time())
	} else {
		if trans.schema.PromResetTime() {
			// change the time in instant query to the query end time.
			ResetTimeForProm(chunk, oChunk, trans.promQueryTime)
		} else {
			ResetTime(chunk, oChunk, trans.opt.StartTime)
		}
	}
	if trans.labelCall != nil {
		trans.processLabelCall(chunk, oChunk)
	} else {
		oChunk.AppendTagsAndIndexes(chunk.Tags(), chunk.TagIndex())
		oChunk.AppendIntervalIndexes(chunk.IntervalIndex())
	}
	if trans.opt.IsPromQuery() && trans.HasBinaryExpr {
		removeTableName(chunk, oChunk)
	}
	columnMap := make(map[string]*ColumnImpl)
	for i, f := range trans.transparents {
		dst := oChunk.Column(i)
		if f != nil {
			f(dst, chunk, trans.ColumnMap[i])
		} else {
			trans.chunkValuer.AtChunkRow(chunk, 0)
			isFast := initByType(trans.ops[i].Expr, trans.chunkValuer, columnMap)
			if isFast {
				trans.rp.index = 0
				res := eval(trans.ops[i].Expr, &trans.valuer, columnMap, chunk.NumberOfRows(), trans.rp)
				res.copyTo(chunk.NumberOfRows(), dst.(*ColumnImpl))
			} else {
				for index := 0; index < chunk.NumberOfRows(); index++ {
					trans.chunkValuer.index = index
					value := trans.valuer.Eval(trans.ops[i].Expr)
					if value == nil {
						dst.AppendNil()
						continue
					}
					if val, ok := value.(float64); ok && math.IsNaN(val) && !trans.isPromQuery {
						dst.AppendNil()
						continue
					}
					AppendRowValue(dst, value)
					dst.AppendNotNil()

				}
			}
		}
	}

	return oChunk
}

func (trans *MaterializeTransform) processLabelCall(chunk, oChunk Chunk) {
	if valuer, ok := trans.valuer.Valuer.(influxql.CallValuer); ok {
		var args []interface{}
		if len(trans.labelCall.Args) > 0 {
			args = make([]interface{}, len(trans.labelCall.Args))
			args[0] = chunk.Tags()
			for i := 1; i < len(trans.labelCall.Args); i++ {
				if arg, ok := trans.labelCall.Args[i].(*influxql.StringLiteral); ok {
					args[i] = arg.Val
				} else {
					args[i] = nil
				}
			}
		}
		values, _ := valuer.Call(trans.labelCall.Name, args)
		if tags, ok := values.([]ChunkTags); ok {
			tagIndexes := chunk.TagIndex()
			oChunk.ResetTagsAndIndexes(tags, tagIndexes)
		}
		oChunk.AppendIntervalIndexes(chunk.IntervalIndex())
	}
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

func removeTableName(chunk, oChunk Chunk) {
	chunkTags := oChunk.Tags()
	dstTags := make([]ChunkTags, 0, len(chunkTags))
	for _, tag := range chunkTags {
		index := -1
		keys, values := tag.GetChunkTagAndValues()
		for i := range keys {
			if keys[i] == promql2influxql.DefaultMetricKeyLabel {
				index = i
				break
			}
		}
		if index != -1 {
			keys = append(keys[:index], keys[index+1:]...)
			values = append(values[:index], values[index+1:]...)
		}
		dstTag := NewChunkTagsByTagKVs(keys, values)
		dstTags = append(dstTags, *dstTag)
	}
	oChunk.ResetTagsAndIndexes(dstTags, chunk.TagIndex())
}
