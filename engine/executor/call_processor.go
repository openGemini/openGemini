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
	"errors"
	"fmt"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/op"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

const (
	DefaultClusterNum = 100
	MaxClusterNum     = 100000
)

const (
	OGSketchInsert     = "ogsketch_insert"
	OGSketchMerge      = "ogsketch_merge"
	OGSketchPercentile = "ogsketch_percentile"
	PercentileApprox   = "percentile_approx"
	PercentileOGSketch = "percentile_ogsketch"
)

func NewProcessors(inRowDataType, outRowDataType hybridqp.RowDataType, exprOpt []hybridqp.ExprOptions, opt *query.ProcessorOptions, isSubQuery bool) (*processorResults, error) {
	var err error
	proRes := &processorResults{}
	coProcessor := NewCoProcessorImpl()
	auxProcessor, isSingleCall := statCallAndAux(inRowDataType, outRowDataType, exprOpt)
	for i := range exprOpt {
		var routine Routine
		switch expr := exprOpt[i].Expr.(type) {
		case *influxql.Call:
			if op.IsAggregateOp(expr) {
				name := exprOpt[i].Expr.(*influxql.Call).Name
				if strings.Contains(name, "castor") {
					processor, err := NewWideProcessorImpl(inRowDataType, outRowDataType, exprOpt)
					proRes.coProcessor = processor.(*WideCoProcessorImpl)
					if err != nil {
						return nil, errors.New("unsupported aggregation operator of call processor")
					}
					proRes.isUDAFCall = true
					return proRes, nil
				}
				routine, err = createRoutineFromUDF(inRowDataType, outRowDataType, exprOpt[i], isSingleCall, nil)
				if err != nil {
					return proRes, err
				}
				coProcessor.AppendRoutine(routine)
				continue
			}
			name := exprOpt[i].Expr.(*influxql.Call).Name
			// Operators implemented through registration
			// TODO migrate all operators
			if aggOp := GetAggOperator(name); aggOp != nil {
				params := &AggCallFuncParams{
					InRowDataType:  inRowDataType,
					OutRowDataType: outRowDataType,
					ExprOpt:        exprOpt[i],
					IsSingleCall:   isSingleCall,
					AuxProcessor:   auxProcessor,
					Opt:            opt,
					ProRes:         proRes,
					IsSubQuery:     isSubQuery,
					Name:           name,
				}
				routine, err = aggOp.CreateRoutine(params)
				coProcessor.AppendRoutine(routine)
				if err != nil {
					return nil, err
				}
				continue
			}
			switch name {
			case "count":
				routine, err = NewCountRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall)
				coProcessor.AppendRoutine(routine)
			case "sum":
				routine, err = NewSumRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall)
				coProcessor.AppendRoutine(routine)
			case "first":
				routine, err = NewFirstRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall, auxProcessor)
				coProcessor.AppendRoutine(routine)
			case "last":
				routine, err = NewLastRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall, auxProcessor)
				coProcessor.AppendRoutine(routine)
			case "percentile":
				if isSubQuery {
					isSingleCall = false
				}
				routine, err = NewPercentileRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall, auxProcessor)
				coProcessor.AppendRoutine(routine)
			case "ogsketch_percentile", "ogsketch_merge", "ogsketch_insert":
				var percentile float64
				var clusterNum int
				var err error
				if isSubQuery {
					isSingleCall = false
				}
				clusterNum, err = getClusterNum(exprOpt[i].Expr.(*influxql.Call), name)
				if err != nil {
					return nil, err
				}
				percentile, err = getPercentile(exprOpt[i].Expr.(*influxql.Call), name)
				if err != nil {
					return nil, err
				}
				percentile /= 100
				routine, err = NewPercentileApproxRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall, opt, name, clusterNum, percentile)
				if err != nil {
					return nil, err
				}
				coProcessor.AppendRoutine(routine)
				if name == "ogsketch_insert" || name == "ogsketch_merge" {
					proRes.isCompositeCall = true
					proRes.clusterNum = clusterNum
				}
			case "median":
				routine, err = NewMedianRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall)
				coProcessor.AppendRoutine(routine)
			case "mode":
				routine, err = NewModeRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall)
				coProcessor.AppendRoutine(routine)
			case "top":
				routine, err = NewTopRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], auxProcessor)
				coProcessor.AppendRoutine(routine)
			case "bottom":
				routine, err = NewBottomRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], auxProcessor)
				coProcessor.AppendRoutine(routine)
			case "distinct":
				routine, err = NewDistinctRoutineImpl(inRowDataType, outRowDataType, exprOpt[i])
				coProcessor.AppendRoutine(routine)
			case "difference", "non_negative_difference":
				isNonNegative := name == "non_negative_difference"
				routine, err = NewDifferenceRoutineImpl(inRowDataType, outRowDataType, exprOpt[i],
					isSingleCall, isNonNegative)
				coProcessor.AppendRoutine(routine)
				proRes.isTimeUniqueCall = true
				proRes.offset = 1
			case "derivative", "non_negative_derivative":
				isNonNegative := name == "non_negative_derivative"
				interval := exprOpt[i].DerivativeInterval(opt.Interval)
				routine, err = NewDerivativeRoutineImpl(inRowDataType, outRowDataType, exprOpt[i],
					isSingleCall, isNonNegative, opt.Ascending, interval)
				coProcessor.AppendRoutine(routine)
				proRes.isTimeUniqueCall = true
				proRes.offset = 1
			case "elapsed":
				routine, err = NewElapsedRoutineImpl(inRowDataType, outRowDataType, exprOpt[i],
					isSingleCall)
				coProcessor.AppendRoutine(routine)
				proRes.isTransformationCall = true
				proRes.offset = 1
			case "moving_average":
				routine, err = NewMovingAverageRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall)
				coProcessor.AppendRoutine(routine)
				proRes.isTransformationCall = true
				expr, _ := exprOpt[i].Expr.(*influxql.Call)
				n, _ := expr.Args[len(expr.Args)-1].(*influxql.IntegerLiteral)
				proRes.offset = int(n.Val) - 1
			case "cumulative_sum":
				routine, err = NewCumulativeSumRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall)
				coProcessor.AppendRoutine(routine)
				proRes.isTransformationCall = true
				proRes.offset = 0
			case "integral":
				routine, err = NewIntegralRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], opt,
					isSingleCall)
				coProcessor.AppendRoutine(routine)
				proRes.isIntegralCall = true
			case "rate", "irate":
				isRate := name == "rate"
				interval := exprOpt[i].DerivativeInterval(opt.Interval)
				routine, err = NewRateRoutineImpl(inRowDataType, outRowDataType, exprOpt[i],
					isSingleCall, isRate, interval)
				coProcessor.AppendRoutine(routine)
			case "absent":
				routine, err = NewAbsentRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall)
				coProcessor.AppendRoutine(routine)
			case "stddev":
				routine, err = NewStddevRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall)
				coProcessor.AppendRoutine(routine)
			case "sample":
				routine, err = NewSampleRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], isSingleCall, auxProcessor)
				coProcessor.AppendRoutine(routine)
			default:
				return nil, errors.New("unsupported aggregation operator of call processor")
			}
		default:
			continue
		}
		if err != nil {
			return nil, err
		}
	}
	proRes.isSingleCall = isSingleCall
	proRes.coProcessor = coProcessor
	return proRes, nil
}

func castorRoutineFactory(_ ...interface{}) (interface{}, error) {
	return nil, nil
}

func NewWideProcessorImpl(inRowDataType, outRowDataType hybridqp.RowDataType, exprOpts []hybridqp.ExprOptions) (CoProcessor, error) {
	for _, exprOpt := range exprOpts {
		inOrdinal := inRowDataType.FieldIndex(exprOpt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
		outOrdinal := outRowDataType.FieldIndex(exprOpt.Ref.Val)
		if inOrdinal < 0 || outOrdinal < 0 || inOrdinal != outOrdinal {
			panic("input and output schemas are not aligned for iterator")
		}
	}
	var wideRoutine *WideRoutineImpl
	expr := exprOpts[0].Expr.(*influxql.Call)
	args := expr.Args[1:]
	switch expr.Name {
	case "castor":
		wideRoutine = NewWideRoutineImpl(NewWideIterator(CastorReduce, args))
	}
	wideProcessor := NewWideCoProcessorImpl(wideRoutine)
	return wideProcessor, nil
}

func getClusterNum(call *influxql.Call, name string) (int, error) {
	var clusterNum int
	if len(call.Args) == 2 {
		clusterNum = DefaultClusterNum
		return clusterNum, nil
	}

	switch arg := call.Args[2].(type) {
	case *influxql.IntegerLiteral:
		clusterNum = int(arg.Val)
	default:
		return 0, errno.NewError(errno.UnsupportedDataType, fmt.Sprintf("second argument of %s", name), arg.String())
	}
	if clusterNum < 1 {
		return 0, errors.New("the value of clusterNum must be a positive integer")
	}
	if clusterNum > MaxClusterNum {
		return 0, fmt.Errorf("clusterNum is too large. It should not be greater than %d", MaxClusterNum)
	}
	return clusterNum, nil
}

func getPercentile(call *influxql.Call, name string) (float64, error) {
	var percentile float64
	switch arg := call.Args[1].(type) {
	case *influxql.NumberLiteral:
		percentile = arg.Val
	case *influxql.IntegerLiteral:
		percentile = float64(arg.Val)
	default:
		return 0, errno.NewError(errno.UnsupportedDataType, fmt.Sprintf("first argument of %s", name), arg.String())
	}
	if percentile < 0 || percentile > 100 {
		return 0, fmt.Errorf("invalid percentile, the value range must be 0 to 100")
	}
	return percentile, nil
}

func countRoutineFactory(args ...interface{}) (interface{}, error) {
	inRowDataType := args[0].(hybridqp.RowDataType)
	outRowDataType := args[1].(hybridqp.RowDataType)
	opt := args[2].(hybridqp.ExprOptions)
	isSingleCall := args[3].(bool)

	return NewCountRoutineImpl(inRowDataType, outRowDataType, opt, isSingleCall)
}

func sumRoutineFactory(args ...interface{}) (interface{}, error) {
	inRowDataType := args[0].(hybridqp.RowDataType)
	outRowDataType := args[1].(hybridqp.RowDataType)
	opt := args[2].(hybridqp.ExprOptions)
	isSingleCall := args[3].(bool)

	return NewSumRoutineImpl(inRowDataType, outRowDataType, opt, isSingleCall)
}

func createRoutineFromUDF(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool, auxProcessor []*AuxProcessor) (Routine, error) {
	if op, ok := op.GetOpFactory().FindAggregateOp(opt.Expr.(*influxql.Call).Name); ok {
		routine, err := op.Factory().Create(inRowDataType, outRowDataType, opt, isSingleCall, auxProcessor)
		return routine.(Routine), err
	}
	return nil, fmt.Errorf("aggregate operator %s found in UDF before, but disappeared", opt.Expr.(*influxql.Call).Name)
}

type processorResults struct {
	isSingleCall, isTransformationCall, isUDAFCall    bool
	isIntegralCall, isTimeUniqueCall, isCompositeCall bool
	offset, clusterNum                                int //Time offset in transform operators, for difference(), derivative(), elapsed(), moving_average(), cumulative_sum()
	coProcessor                                       CoProcessor
}

func statCallAndAux(inRowDataType, outRowDataType hybridqp.RowDataType, exprOpt []hybridqp.ExprOptions) ([]*AuxProcessor, bool) {
	var (
		isSingleCall bool
		callCount    int
		auxProcessor []*AuxProcessor
	)
	for i := range exprOpt {
		switch exprOpt[i].Expr.(type) {
		case *influxql.Call:
			callCount++
			continue
		case *influxql.VarRef:
			auxProcessor = append(auxProcessor, NewAuxCoProcessor(inRowDataType, outRowDataType, exprOpt[i]))
		default:
			panic("unsupported expr type of call processor")
		}
	}
	isSingleCall = callCount == 1
	return auxProcessor, isSingleCall
}

func NewMedianRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for median iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatSliceIterator(NewMedianReduce[float64],
			isSingleCall, inOrdinal, outOrdinal, nil, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColIntegerSliceIterator(NewMedianReduce[int64],
			isSingleCall, inOrdinal, outOrdinal, nil, outRowDataType),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "median", dataType.String())
	}
}

func NewModeRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for mode iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatSliceIterator(NewModeReduce[float64],
			isSingleCall, inOrdinal, outOrdinal, nil, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColIntegerSliceIterator(NewModeReduce[int64],
			isSingleCall, inOrdinal, outOrdinal, nil, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.String:
		return NewRoutineImpl(NewStringColStringSliceIterator(NewModeReduce[string],
			isSingleCall, inOrdinal, outOrdinal, nil, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Boolean:
		return NewRoutineImpl(NewBooleanColBooleanSliceIterator(NewBooleanModeReduce,
			isSingleCall, inOrdinal, outOrdinal, nil, outRowDataType),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "mode", dataType.String())
	}
}

func NewCountRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for count iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(
			NewIntegerColIntegerIterator(IntegerCountReduce, IntegerCountMerge, isSingleCall, inOrdinal, outOrdinal,
				nil, nil),
			inOrdinal, outOrdinal), nil
	case influxql.Float:
		return NewRoutineImpl(
			NewFloatColIntegerIterator(FloatCountReduce, IntegerCountMerge, isSingleCall, inOrdinal, outOrdinal,
				nil, nil),
			inOrdinal, outOrdinal), nil
	case influxql.String:
		return NewRoutineImpl(
			NewStringColIntegerIterator(StringCountReduce, IntegerCountMerge, isSingleCall, inOrdinal, outOrdinal,
				nil, nil),
			inOrdinal, outOrdinal), nil
	case influxql.Boolean:
		return NewRoutineImpl(
			NewBooleanColIntegerIterator(BooleanCountReduce, IntegerCountMerge, isSingleCall, inOrdinal, outOrdinal,
				nil, nil),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "count/mean", dataType.String())
	}
}

func NewSumRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for sum iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(
			NewIntegerColIntegerIterator(IntegerSumReduce, IntegerSumMerge, isSingleCall, inOrdinal, outOrdinal,
				nil, nil),
			inOrdinal, outOrdinal), nil
	case influxql.Float:
		return NewRoutineImpl(
			NewFloatColFloatIterator(FloatSumReduce, FloatSumMerge, isSingleCall, inOrdinal, outOrdinal,
				nil, nil), inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "sum/mean", dataType.String())
	}
}

func NewFirstRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool, auxProcessor []*AuxProcessor) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for first iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		if isSingleCall {
			return NewRoutineImpl(NewIntegerColIntegerIterator(IntegerFirstReduce, IntegerFirstMerge,
				isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
				inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewIntegerTimeColIntegerIterator(IntegerFirstTimeColReduce, IntegerFirstTimeColMerge,
			inOrdinal, outOrdinal),
			inOrdinal, outOrdinal), nil
	case influxql.Float:
		if isSingleCall {
			return NewRoutineImpl(NewFloatColFloatIterator(FloatFirstReduce, FloatFirstMerge,
				isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
				inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewFloatTimeColFloatIterator(FloatFirstTimeColReduce, FloatFirstTimeColMerge,
			inOrdinal, outOrdinal),
			inOrdinal, outOrdinal), nil
	case influxql.String:
		if isSingleCall {
			return NewRoutineImpl(NewStringColStringIterator(StringFirstReduce, StringFirstMerge,
				isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
				inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewStringTimeColStringIterator(StringFirstTimeColReduce, StringFirstTimeColMerge,
			inOrdinal, outOrdinal),
			inOrdinal, outOrdinal), nil
	case influxql.Boolean:
		if isSingleCall {
			return NewRoutineImpl(NewBooleanColBooleanIterator(BooleanFirstReduce, BooleanFirstMerge,
				isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
				inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewBooleanTimeColBooleanIterator(BooleanFirstTimeColReduce, BooleanFirstTimeColMerge,
			inOrdinal, outOrdinal),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "first", dataType.String())
	}
}

func NewLastRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool, auxProcessor []*AuxProcessor) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for last iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		if isSingleCall {
			return NewRoutineImpl(NewIntegerColIntegerIterator(IntegerLastReduce, IntegerLastMerge,
				isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
				inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewIntegerTimeColIntegerIterator(IntegerLastTimeColReduce, IntegerLastTimeColMerge,
			inOrdinal, outOrdinal),
			inOrdinal, outOrdinal), nil
	case influxql.Float:
		if isSingleCall {
			return NewRoutineImpl(NewFloatColFloatIterator(FloatLastReduce, FloatLastMerge,
				isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
				inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewFloatTimeColFloatIterator(FloatLastTimeColReduce, FloatLastTimeColMerge,
			inOrdinal, outOrdinal),
			inOrdinal, outOrdinal), nil
	case influxql.String:
		if isSingleCall {
			return NewRoutineImpl(NewStringColStringIterator(StringLastReduce, StringLastMerge,
				isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
				inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewStringTimeColStringIterator(StringLastTimeColReduce, StringLastTimeColMerge,
			inOrdinal, outOrdinal),
			inOrdinal, outOrdinal), nil
	case influxql.Boolean:
		if isSingleCall {
			return NewRoutineImpl(NewBooleanColBooleanIterator(BooleanLastReduce, BooleanLastMerge,
				isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
				inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewBooleanTimeColBooleanIterator(BooleanLastTimeColReduce, BooleanLastTimeColMerge,
			inOrdinal, outOrdinal),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "last", dataType.String())
	}
}

func NewPercentileRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool, auxProcessor []*AuxProcessor) (Routine, error) {
	var percentile float64
	switch arg := opt.Expr.(*influxql.Call).Args[1].(type) {
	case *influxql.NumberLiteral:
		percentile = arg.Val
	case *influxql.IntegerLiteral:
		percentile = float64(arg.Val)
	default:
		panic("the type of input args of percentile iterator is unsupported")
	}
	if percentile < 0 || percentile > 100 {
		return nil, fmt.Errorf("invalid percentile, the value range must be 0 to 100")
	}
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for percentile iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatSliceIterator(NewFloatPercentileReduce(percentile),
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColIntegerSliceIterator(NewIntegerPercentileReduce(percentile),
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "percentile", dataType.String())
	}
}

func NewPercentileApproxRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, exprOpt hybridqp.ExprOptions, isSingleCall bool, opt *query.ProcessorOptions, name string, clusterNum int, percentile float64) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(exprOpt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(exprOpt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, errno.NewError(errno.SchemaNotAligned, name, "input and output schemas are not aligned")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	outDataType := outRowDataType.Field(outOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		switch name {
		case OGSketchInsert:
			return NewRoutineImpl(NewOGSketchIterator(isSingleCall, inOrdinal, outOrdinal, clusterNum, opt,
				NewFloatOGSketchInsertIem(isSingleCall, inOrdinal, outOrdinal, clusterNum, percentile)),
				inOrdinal, outOrdinal), nil
		case PercentileApprox:
			return NewRoutineImpl(NewOGSketchIterator(isSingleCall, inOrdinal, outOrdinal, clusterNum, opt,
				NewFloatPercentileApproxItem(isSingleCall, inOrdinal, outOrdinal, clusterNum, percentile)),
				inOrdinal, outOrdinal), nil
		}
	case influxql.Integer:
		switch name {
		case OGSketchInsert:
			return NewRoutineImpl(NewOGSketchIterator(isSingleCall, inOrdinal, outOrdinal, clusterNum, opt,
				NewIntegerOGSketchInsertIem(isSingleCall, inOrdinal, outOrdinal, clusterNum, percentile)),
				inOrdinal, outOrdinal), nil
		case PercentileApprox:
			return NewRoutineImpl(NewOGSketchIterator(isSingleCall, inOrdinal, outOrdinal, clusterNum, opt,
				NewIntegerPercentileApproxItem(isSingleCall, inOrdinal, outOrdinal, clusterNum, percentile)),
				inOrdinal, outOrdinal), nil
		}
	case influxql.FloatTuple:
		switch name {
		case OGSketchMerge:
			return NewRoutineImpl(NewOGSketchIterator(isSingleCall, inOrdinal, outOrdinal, clusterNum, opt,
				NewOGSketchMergeItem(isSingleCall, inOrdinal, outOrdinal, clusterNum, percentile)),
				inOrdinal, outOrdinal), nil
		case OGSketchPercentile:
			switch outDataType {
			case influxql.Float:
				return NewRoutineImpl(NewOGSketchIterator(isSingleCall, inOrdinal, outOrdinal, clusterNum, opt,
					NewFloatOGSketchPercentileItem(isSingleCall, inOrdinal, outOrdinal, clusterNum, percentile)),
					inOrdinal, outOrdinal), nil
			case influxql.Integer:
				return NewRoutineImpl(NewOGSketchIterator(isSingleCall, inOrdinal, outOrdinal, clusterNum, opt,
					NewIntegerOGSketchPercentileItem(isSingleCall, inOrdinal, outOrdinal, clusterNum, percentile)),
					inOrdinal, outOrdinal), nil
			}
		}
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, name, dataType.String())
	}
	return nil, errno.NewError(errno.UnsupportedDataType, name, dataType.String())
}

func NewTopRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, auxProcessor []*AuxProcessor) (Routine, error) {
	expr, ok := opt.Expr.(*influxql.Call)
	if !ok {
		panic(fmt.Errorf("NewTopRoutineImpl input illegal, opt.Expr is not influxql.Call"))
	}
	if len(expr.Args) < 2 {
		panic(fmt.Errorf("top() requires 2 or more arguments, got %d", len(expr.Args)))
	}

	// TODO: do for logical plan
	if len(expr.Args) > 2 {
		// Create a max iterator using the groupings in the arguments.
		logger.GetLogger().Info("The top call has more than two parameters")
	} else {
		// There are no arguments so do not organize the points by tags.
		logger.GetLogger().Info("The top call only has two parameters")
	}

	n, ok := expr.Args[len(expr.Args)-1].(*influxql.IntegerLiteral)
	if !ok {
		panic(fmt.Errorf("NewTopRoutineImpl input illegal, opt.Args element is not influxql.IntegerLiteral"))
	}

	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for top iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatHeapIterator(inOrdinal, outOrdinal, auxProcessor, outRowDataType, NewHeapItem[float64](int(n.Val), TopCmpByValueReduce[float64], TopCmpByTimeReduce[float64])),
			inOrdinal, outOrdinal), nil

	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColIntegerHeapIterator(inOrdinal, outOrdinal, auxProcessor, outRowDataType, NewHeapItem[int64](int(n.Val), TopCmpByValueReduce[int64], TopCmpByTimeReduce[int64])),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "top", dataType.String())
	}
}

func NewBottomRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, auxProcessor []*AuxProcessor) (Routine, error) {
	expr, ok := opt.Expr.(*influxql.Call)
	if !ok {
		panic(fmt.Errorf("NewBottomRoutineImpl input illegal, opt.Expr is not influxql.Call"))
	}
	if len(expr.Args) < 2 {
		panic(fmt.Errorf("bottom() requires 2 or more arguments, got %d", len(expr.Args)))
	}

	if len(expr.Args) > 2 {
		// Create a min iterator using the groupings in the arguments.
		logger.GetLogger().Info("The bottom call has more than two parameters")
	} else {
		// There are no arguments so do not organize the points by tags.
		logger.GetLogger().Info("The bottom call only has two parameters")
	}

	n, ok := expr.Args[len(expr.Args)-1].(*influxql.IntegerLiteral)
	if !ok {
		panic(fmt.Errorf("NewBottomRoutineImpl input illegal, opt.Args element is not influxql.IntegerLiteral"))
	}

	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for bottom iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatHeapIterator(inOrdinal, outOrdinal, auxProcessor, outRowDataType, NewHeapItem(int(n.Val), BottomCmpByValueReduce[float64], BottomCmpByTimeReduce[float64])),
			inOrdinal, outOrdinal), nil
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColIntegerHeapIterator(inOrdinal, outOrdinal, auxProcessor, outRowDataType, NewHeapItem(int(n.Val), BottomCmpByValueReduce[int64], BottomCmpByTimeReduce[int64])),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "bottom", dataType.String())
	}
}

func NewDistinctRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for distinct iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColIntegerDistinctIterator(inOrdinal, outOrdinal), inOrdinal, outOrdinal), nil
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatDistinctIterator(inOrdinal, outOrdinal), inOrdinal, outOrdinal), nil
	case influxql.String, influxql.Tag:
		return NewRoutineImpl(NewStringColStringDistinctIterator(inOrdinal, outOrdinal), inOrdinal, outOrdinal), nil
	case influxql.Boolean:
		return NewRoutineImpl(NewBooleanColBooleanDistinctIterator(inOrdinal, outOrdinal), inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "distinct", dataType.String())
	}
}

func NewDifferenceRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions,
	isSingleCall, isNonNegative bool,
) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for difference iterator")
	}
	var calDirection string
	args := opt.Expr.(*influxql.Call).Args
	if len(args) == 2 {
		calDirection = args[1].(*influxql.StringLiteral).Val
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		if calDirection == "front" {
			return NewRoutineImpl(NewFloatColFloatTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
				NewFloatDifferenceItem(isNonNegative, FrontDiffFunc[float64])), inOrdinal, outOrdinal), nil
		}
		if calDirection == "absolute" {
			return NewRoutineImpl(NewFloatColFloatTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
				NewFloatDifferenceItem(isNonNegative, AbsoluteDiffFunc[float64])), inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewFloatColFloatTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
			NewFloatDifferenceItem(isNonNegative, BehindDiffFunc[float64])), inOrdinal, outOrdinal), nil
	case influxql.Integer:
		if calDirection == "front" {
			return NewRoutineImpl(NewIntegerColIntegerTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
				NewIntegerDifferenceItem(isNonNegative, FrontDiffFunc[int64])), inOrdinal, outOrdinal), nil
		}
		if calDirection == "absolute" {
			return NewRoutineImpl(NewIntegerColIntegerTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
				NewIntegerDifferenceItem(isNonNegative, AbsoluteDiffFunc[int64])), inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewIntegerColIntegerTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
			NewIntegerDifferenceItem(isNonNegative, BehindDiffFunc[int64])), inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "difference", dataType.String())
	}
}

func NewDerivativeRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions,
	isSingleCall, isNonNegative, ascending bool, interval hybridqp.Interval,
) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for derivative iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
			NewFloatDerivativeItem(isNonNegative, ascending, interval)), inOrdinal, outOrdinal), nil

	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColFloatTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
			NewIntegerDerivativeItem(isNonNegative, ascending, interval)), inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "derivative", dataType.String())
	}
}

func NewIntegralRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions,
	opts *query.ProcessorOptions, isSingleCall bool,
) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	interval := opt.IntegralInterval()
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for integral iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatIntegralIterator(
			isSingleCall, inOrdinal, outOrdinal, outRowDataType, interval, opts),
			inOrdinal, outOrdinal), nil
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColFloatIntegralIterator(
			isSingleCall, inOrdinal, outOrdinal, outRowDataType, interval, opts),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "integral", dataType.String())
	}
}

func NewElapsedRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions,
	isSingleCall bool,
) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	interval := opt.ElapsedInterval()
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for elapsed iterator")
	}
	return NewRoutineImpl(NewIntegerColIntegerTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
		NewElapsedItem(interval)), inOrdinal, outOrdinal), nil
}

func NewMovingAverageRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions,
	isSingleCall bool) (Routine, error) {
	expr, ok := opt.Expr.(*influxql.Call)
	if !ok {
		panic(fmt.Errorf("NewMovingAverageRoutineImpl input illegal, opt.Expr is not influxql.Call"))
	}

	if len(expr.Args) != 2 {
		panic(fmt.Errorf("moving_average() requires 2 arguments, got %d", len(expr.Args)))
	} else {
		logger.GetLogger().Info("The moving_average call only has two parameters")
	}

	n, ok := expr.Args[len(expr.Args)-1].(*influxql.IntegerLiteral)
	if !ok {
		panic(fmt.Errorf("NewMovingAverageRoutineImpl input illegal, opt.Args element is not influxql.IntegerLiteral"))
	}

	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for moving_average iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColFloatTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
			NewIntegerMovingAverageItem(int(n.Val))), inOrdinal, outOrdinal), nil
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
			NewFloatMovingAverageItem(int(n.Val))), inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "moving_average", dataType.String())
	}
}

func NewCumulativeSumRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions,
	isSingleCall bool,
) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for cumulative_sum iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
			NewFloatCumulativeSumItem()), inOrdinal, outOrdinal), nil
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColIntegerTransIterator(isSingleCall, inOrdinal, outOrdinal, outRowDataType,
			NewIntegerCumulativeSumItem()), inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "cumulative_sum", dataType.String())
	}
}

func NewRateRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions,
	isSingleCall, isRate bool, interval hybridqp.Interval,
) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for rate iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		if isRate {
			return NewRoutineImpl(NewFloatColFloatRateIterator(FloatRateMiddleReduce, FloatRateFinalReduce,
				FloatRateUpdate, FloatRateMerge, isSingleCall, inOrdinal, outOrdinal, outRowDataType, &interval),
				inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewFloatColFloatRateIterator(FloatIrateMiddleReduce, FloatIrateFinalReduce,
			FloatIrateUpdate, FloatIrateMerge, isSingleCall, inOrdinal, outOrdinal, outRowDataType, &interval),
			inOrdinal, outOrdinal), nil
	case influxql.Integer:
		if isRate {
			return NewRoutineImpl(NewIntegerColFloatRateIterator(IntegerRateMiddleReduce, IntegerRateFinalReduce,
				IntegerRateUpdate, IntegerRateMerge, isSingleCall, inOrdinal, outOrdinal, outRowDataType, &interval),
				inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(NewIntegerColFloatRateIterator(IntegerIrateMiddleReduce, IntegerIrateFinalReduce,
			IntegerIrateUpdate, IntegerIrateMerge, isSingleCall, inOrdinal, outOrdinal, outRowDataType, &interval),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "irate/rate", dataType.String())
	}
}

func NewAbsentRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for absent iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(
			NewIntegerColIntegerIterator(IntegerAbsentReduce, IntegerAbsentMerge, isSingleCall, inOrdinal, outOrdinal,
				nil, nil),
			inOrdinal, outOrdinal), nil
	case influxql.Float:
		return NewRoutineImpl(
			NewFloatColIntegerIterator(FloatAbsentReduce, IntegerAbsentMerge, isSingleCall, inOrdinal, outOrdinal,
				nil, nil),
			inOrdinal, outOrdinal), nil
	case influxql.String:
		return NewRoutineImpl(
			NewStringColIntegerIterator(StringAbsentReduce, IntegerAbsentMerge, isSingleCall, inOrdinal, outOrdinal,
				nil, nil),
			inOrdinal, outOrdinal), nil
	case influxql.Boolean:
		return NewRoutineImpl(
			NewBooleanColIntegerIterator(BooleanAbsentReduce, IntegerAbsentMerge, isSingleCall, inOrdinal, outOrdinal,
				nil, nil),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "absent", dataType.String())
	}
}

func NewStddevRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool,
) (Routine, error) {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for stddev iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatSliceIterator(NewFloatStddevReduce(),
			isSingleCall, inOrdinal, outOrdinal, nil, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColIntegerSliceIterator(NewIntegerStddevReduce(),
			isSingleCall, inOrdinal, outOrdinal, nil, outRowDataType),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "stddev", dataType.String())
	}
}

func NewSampleRoutineImpl(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, isSingleCall bool, auxProcessor []*AuxProcessor) (Routine, error) {
	var sampleNum int64
	switch arg := opt.Expr.(*influxql.Call).Args[1].(type) {
	case *influxql.IntegerLiteral:
		sampleNum = arg.Val
	default:
		panic("the type of input args of sample iterator is unsupported")
	}
	if len(opt.Expr.(*influxql.Call).Args) != 2 {
		panic(fmt.Errorf("sample() requires 2 arguments, got %d", len(opt.Expr.(*influxql.Call).Args)))
	}
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for sample iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Float:
		return NewRoutineImpl(NewFloatColFloatSampleIterator(int(sampleNum),
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Integer:
		return NewRoutineImpl(NewIntegerColIntegerSampleIterator(int(sampleNum),
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.String:
		return NewRoutineImpl(NewStringColStringSampleIterator(int(sampleNum),
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	case influxql.Boolean:
		return NewRoutineImpl(NewBooleanColBooleanSampleIterator(int(sampleNum),
			isSingleCall, inOrdinal, outOrdinal, auxProcessor, outRowDataType),
			inOrdinal, outOrdinal), nil
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "sample", dataType.String())
	}
}

func NewAuxCoProcessor(inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions) *AuxProcessor {
	inOrdinal, outOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.VarRef).Val), outRowDataType.FieldIndex(opt.Ref.Val)
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return &AuxProcessor{
			inOrdinal:     inOrdinal,
			outOrdinal:    outOrdinal,
			auxHelperFunc: IntegerAuxHelpFunc,
		}
	case influxql.Float:
		return &AuxProcessor{
			inOrdinal:     inOrdinal,
			outOrdinal:    outOrdinal,
			auxHelperFunc: FloatAuxHelpFunc,
		}
	case influxql.String, influxql.Tag:
		return &AuxProcessor{
			inOrdinal:     inOrdinal,
			outOrdinal:    outOrdinal,
			auxHelperFunc: StringAuxHelpFunc,
		}
	case influxql.Boolean:
		return &AuxProcessor{
			inOrdinal:     inOrdinal,
			outOrdinal:    outOrdinal,
			auxHelperFunc: BooleanAuxHelpFunc,
		}
	default:
		return nil
	}
}

type AuxProcessor struct {
	inOrdinal     int
	outOrdinal    int
	auxHelperFunc func(input, output Column, rowIdx ...int)
}

func IntegerAuxHelpFunc(input, output Column, rowIdx ...int) {
	for _, idx := range rowIdx {
		if !input.IsNilV2(idx) {
			output.AppendIntegerValue(input.IntegerValue(input.GetValueIndexV2(idx)))
			output.AppendNotNil()
		} else {
			output.AppendNil()
		}
	}
}

func FloatAuxHelpFunc(input, output Column, rowIdx ...int) {
	for _, idx := range rowIdx {
		if !input.IsNilV2(idx) {
			output.AppendFloatValue(input.FloatValue(input.GetValueIndexV2(idx)))
			output.AppendNotNil()
		} else {
			output.AppendNil()
		}
	}
}

func StringAuxHelpFunc(input, output Column, rowIdx ...int) {
	for _, idx := range rowIdx {
		if !input.IsNilV2(idx) {
			oriStr := input.StringValue(input.GetValueIndexV2(idx))
			newStr := make([]byte, len(oriStr))
			copy(newStr, oriStr)
			output.AppendStringValue(util.Bytes2str(newStr))
			output.AppendNotNil()
		} else {
			output.AppendNil()
		}
	}
}

func BooleanAuxHelpFunc(input, output Column, rowIdx ...int) {
	for _, idx := range rowIdx {
		if !input.IsNilV2(idx) {
			output.AppendBooleanValue(input.BooleanValue(input.GetValueIndexV2(idx)))
			output.AppendNotNil()
		} else {
			output.AppendNil()
		}
	}
}
