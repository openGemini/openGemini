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

package executor

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func init() {
	RegistryAggOp("min", &MinOp{})
	RegistryAggOp("max", &MaxOp{})
	RegistryAggOp("percentile_approx", &PercentileApproxOp{})
	RegistryAggOp("min_prom", &MinPromOp{})
	RegistryAggOp("max_prom", &MaxPromOp{})
	RegistryAggOp("count_prom", &FloatCountPromOp{})
	RegistryAggOp("histogram_quantile", &HistogramQuantileOp{})
	RegistryAggOp("count_values_prom", &CountValuesOp{})
}

type MinOp struct{}

func (c *MinOp) CreateRoutine(params *AggCallFuncParams) (Routine, error) {
	inRowDataType, outRowDataType, opt, auxProcessor, isSingleCall := params.InRowDataType, params.OutRowDataType, params.ExprOpt, params.AuxProcessor, params.IsSingleCall
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, errno.NewError(errno.SchemaNotAligned, "min", "input and output schemas are not aligned")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerIterator(MinReduce[int64], MinMerge[int64],
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Float:
		return NewRoutineImpl(NewFloatIterator(MinReduce[float64], MinMerge[float64],
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Boolean:
		return NewRoutineImpl(NewBooleanIterator(BooleanMinReduce, BooleanMinMerge,
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "min", dataType.String())
	}
}

type MaxOp struct{}

func (c *MaxOp) CreateRoutine(params *AggCallFuncParams) (Routine, error) {
	inRowDataType, outRowDataType, opt, auxProcessor, isSingleCall := params.InRowDataType, params.OutRowDataType, params.ExprOpt, params.AuxProcessor, params.IsSingleCall
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, errno.NewError(errno.SchemaNotAligned, "max", "input and output schemas are not aligned")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerIterator(MaxReduce[int64], MaxMerge[int64],
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Float:
		return NewRoutineImpl(NewFloatIterator(MaxReduce[float64], MaxMerge[float64],
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Boolean:
		return NewRoutineImpl(NewBooleanIterator(BooleanMaxReduce, BooleanMaxMerge,
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "max", dataType.String())
	}
}

type PercentileApproxOp struct{}

func (c *PercentileApproxOp) CreateRoutine(params *AggCallFuncParams) (Routine, error) {
	inRowDataType, outRowDataType, exprOpt, isSingleCall, isSubQuery, name, opt := params.InRowDataType, params.OutRowDataType, params.ExprOpt, params.IsSingleCall, params.IsSubQuery, params.Name, params.Opt
	var percentile float64
	var clusterNum int
	var err error
	if isSubQuery {
		isSingleCall = false
	}
	clusterNum, err = getClusterNum(exprOpt.Expr.(*influxql.Call), name)
	if err != nil {
		return nil, err
	}
	percentile, err = getPercentile(exprOpt.Expr.(*influxql.Call), name)
	if err != nil {
		return nil, err
	}
	percentile /= 100
	return NewPercentileApproxRoutineImpl(inRowDataType, outRowDataType, exprOpt, isSingleCall, opt, name, clusterNum, percentile)
}

type BasePromOp struct {
	op string
	fn ColReduceFunc[float64]
	fv ColMergeFunc[float64]
}

func NewBasePromOp(op string, fn ColReduceFunc[float64], fv ColMergeFunc[float64]) BasePromOp {
	return BasePromOp{
		op: op,
		fn: fn,
		fv: fv,
	}
}

func (c *BasePromOp) CreateRoutine(params *AggCallFuncParams) (Routine, error) {
	inRowDataType, outRowDataType, opt, auxProcessor, isSingleCall := params.InRowDataType, params.OutRowDataType, params.ExprOpt, params.AuxProcessor, params.IsSingleCall
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, errno.NewError(errno.SchemaNotAligned, c.op, "input and output schemas are not aligned")
	}
	return NewRoutineImpl(NewFloatIterator(c.fn, c.fv, isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType), inOrdinal, outOrdinal), nil
}

type MinPromOp struct {
	BasePromOp
}

func (c *MinPromOp) CreateRoutine(params *AggCallFuncParams) (Routine, error) {
	c.BasePromOp = NewBasePromOp("min_prom", MinPromReduce, MinPromMerge)
	return c.BasePromOp.CreateRoutine(params)
}

type MaxPromOp struct {
	BasePromOp
}

func (c *MaxPromOp) CreateRoutine(params *AggCallFuncParams) (Routine, error) {
	c.BasePromOp = NewBasePromOp("max_prom", MaxPromReduce, MaxPromMerge)
	return c.BasePromOp.CreateRoutine(params)
}

type FloatCountPromOp struct {
	BasePromOp
}

func (c *FloatCountPromOp) CreateRoutine(params *AggCallFuncParams) (Routine, error) {
	c.BasePromOp = NewBasePromOp("count_prom", FloatCountPromReduce, FloatCountPromMerge)
	return c.BasePromOp.CreateRoutine(params)
}

type FloatColReduceHistogramReduce func(floatItem []bucket) (value float64)

type HistogramQuantileOp struct{}

func (c *HistogramQuantileOp) CreateRoutine(params *AggCallFuncParams) (Routine, error) {
	inRowDataType, outRowDataType, opt := params.InRowDataType, params.OutRowDataType, params.ExprOpt
	params.ProRes.isSingleCall = true
	params.ProRes.isUDAFCall = true
	var percentile float64
	switch arg := opt.Expr.(*influxql.Call).Args[1].(type) {
	case *influxql.NumberLiteral:
		percentile = arg.Val
	case *influxql.IntegerLiteral:
		percentile = float64(arg.Val)
	default:
		return nil, fmt.Errorf("the type of input args of histogram_quantile is unsupported")
	}
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, errno.NewError(errno.SchemaNotAligned, "histogram_quantile", "input and output schemas are not aligned")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatHistogramIterator(FloatHistogramQuantilePromReduce(percentile), inOrdinal, outOrdinal, outRowDataType),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "histogram_quantile", dataType.String())
	}
}

type CountValuesReduce func()

type CountValuesOp struct{}

func (c *CountValuesOp) CreateRoutine(params *AggCallFuncParams) (Routine, error) {
	inRowDataType, outRowDataType, opt := params.InRowDataType, params.OutRowDataType, params.ExprOpt
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	params.ProRes.isSingleCall = true
	params.ProRes.isUDAFCall = true
	arg, ok := opt.Expr.(*influxql.Call).Args[1].(*influxql.StringLiteral)
	if !ok {
		return nil, fmt.Errorf("the type of input args of count_values is unsupported")
	}
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, errno.NewError(errno.SchemaNotAligned, "count_values", "input and output schemas are not aligned")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	if dataType == influxql.Float {
		return NewRoutineImpl(NewCountValuesIterator(inOrdinal, outOrdinal, outRowDataType, arg.Val),
			inOrdinal, outOrdinal), nil
	}
	return nil, errno.NewError(errno.UnsupportedDataType, "count_values", dataType.String())
}
