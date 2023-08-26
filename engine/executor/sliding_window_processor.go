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
	"fmt"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

func NewSlidingWindowProcessors(
	inRowDataType, outRowDataType hybridqp.RowDataType, exprOpt []hybridqp.ExprOptions, opt *query.ProcessorOptions, schema hybridqp.Catalog,
) (CoProcessor, int, int) {
	windowSize, slidingNum := getSlidingWindowSizeAndSlidingNum(exprOpt, opt, schema)
	coProcessor := NewCoProcessorImpl()
	for i := range exprOpt {
		switch exprOpt[i].Expr.(type) {
		case *influxql.Call:
			name := exprOpt[i].Expr.(*influxql.Call).Name
			switch name {
			case "count":
				coProcessor.AppendRoutine(NewSlidingWindowCountRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], slidingNum))
			case "sum":
				coProcessor.AppendRoutine(NewSlidingWindowSumRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], slidingNum))
			case "min":
				coProcessor.AppendRoutine(NewSlidingWindowMinRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], slidingNum))
			case "max":
				coProcessor.AppendRoutine(NewSlidingWindowMaxRoutineImpl(inRowDataType, outRowDataType, exprOpt[i], slidingNum))
			default:
				panic(fmt.Sprintf("unsupported [%s] aggregation operator of sliding window processor", name))
			}
		default:
			panic("sliding Window must have nested aggregate operator")
		}
	}
	return coProcessor, windowSize, slidingNum
}

func getSlidingWindowSizeAndSlidingNum(exprOpt []hybridqp.ExprOptions, opt *query.ProcessorOptions, schema hybridqp.Catalog) (int, int) {
	var (
		windowSize      int
		slidingNum      int
		startWindowTime int64
		endWindowTime   int64
	)
	if opt.Interval.IsZero() {
		startWindowTime = opt.StartTime
		endWindowTime = opt.EndTime
	} else {
		startWindowTime, _ = opt.Window(opt.StartTime)
		endWindowTime, _ = opt.Window(opt.EndTime)
	}
	interval := exprOpt[0].SlidingWindowInterval(opt.Interval)
	intervalNum := int((endWindowTime-startWindowTime)/int64(interval.Duration)) + 1
	// get the window size
	for _, call := range schema.SlidingWindow() {
		n, ok := call.Args[len(call.Args)-1].(*influxql.IntegerLiteral)
		if !ok {
			panic("NewSlideWindowRoutineImpl input illegal, window size is not influxql.IntegerLiteral")
		}
		windowSize = int(n.Val)
		slidingNum = intervalNum - windowSize + 1
		break
	}
	if slidingNum <= 0 {
		panic("The number of sliding must be positive, please check the start and end time and window size")
	}
	return windowSize, slidingNum
}

func NewSlidingWindowCountRoutineImpl(
	inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, slidingNum int,
) Routine {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for sliding_window(count) iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(
			NewIntegerSlidingWindowIntegerIterator(
				IntegerCountReduce, IntegerCountMerge, IntegerSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	case influxql.Float:
		return NewRoutineImpl(
			NewIntegerSlidingWindowIntegerIterator(
				FloatCountReduce, IntegerCountMerge, IntegerSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	case influxql.String:
		return NewRoutineImpl(
			NewIntegerSlidingWindowIntegerIterator(
				StringCountReduce, IntegerCountMerge, IntegerSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	case influxql.Boolean:
		return NewRoutineImpl(
			NewBooleanSlidingWindowIntegerIterator(
				BooleanCountReduce, IntegerCountMerge, IntegerSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	default:
		panic(fmt.Sprintf("unsupported sliding_window(count) iterator type: %s", dataType.String()))
	}
}

func NewSlidingWindowSumRoutineImpl(
	inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, slidingNum int,
) Routine {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for sliding_window(sum) iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(
			NewIntegerSlidingWindowIntegerIterator(
				IntegerSumReduce, IntegerSumMerge, IntegerSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	case influxql.Float:
		return NewRoutineImpl(
			NewFloatSlidingWindowFloatIterator(
				FloatSumReduce, FloatSumMerge, FloatSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	default:
		panic(fmt.Sprintf("unsupported sliding_window(sum) iterator type: %s", dataType.String()))
	}
}

func NewSlidingWindowMinRoutineImpl(
	inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, slidingNum int,
) Routine {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for sliding_window(min) iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(
			NewIntegerSlidingWindowIntegerIterator(
				IntegerMinReduce, IntegerMinMerge, IntegerSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	case influxql.Float:
		return NewRoutineImpl(
			NewFloatSlidingWindowFloatIterator(
				FloatMinReduce, FloatMinMerge, FloatSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	case influxql.Boolean:
		return NewRoutineImpl(
			NewBooleanSlidingWindowBooleanIterator(
				BooleanMinReduce, BooleanMinMerge, BooleanSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	default:
		panic(fmt.Sprintf("unsupported sliding_window(min) iterator type: %s", dataType.String()))
	}
}

func NewSlidingWindowMaxRoutineImpl(
	inRowDataType, outRowDataType hybridqp.RowDataType, opt hybridqp.ExprOptions, slidingNum int,
) Routine {
	inOrdinal := inRowDataType.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outRowDataType.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for sliding_window(max) iterator")
	}
	dataType := inRowDataType.Field(inOrdinal).Expr.(*influxql.VarRef).Type
	switch dataType {
	case influxql.Integer:
		return NewRoutineImpl(
			NewIntegerSlidingWindowIntegerIterator(
				IntegerMaxReduce, IntegerMaxMerge, IntegerSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	case influxql.Float:
		return NewRoutineImpl(
			NewFloatSlidingWindowFloatIterator(
				FloatMaxReduce, FloatMaxMerge, FloatSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	case influxql.Boolean:
		return NewRoutineImpl(
			NewBooleanSlidingWindowBooleanIterator(
				BooleanMaxReduce, BooleanMaxMerge, BooleanSlidingWindowMergeFunc,
				inOrdinal, outOrdinal, slidingNum),
			inOrdinal, outOrdinal)
	default:
		panic(fmt.Sprintf("unsupported sliding_window(max) iterator type: %s", dataType.String()))
	}
}
