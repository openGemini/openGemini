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

package engine

import (
	"fmt"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func newProcessor(inSchema, outSchema record.Schemas, exprOpt []hybridqp.ExprOptions) (CoProcessor, bool, bool) {
	var (
		initColMata bool
		callCount   int
	)
	auxProcessors := make([]*auxProcessor, 0)
	for i := range exprOpt {
		switch exprOpt[i].Expr.(type) {
		case *influxql.Call:
			callCount++
			continue
		case *influxql.VarRef:
			auxProcessors = append(auxProcessors, newAuxProcessor(inSchema, outSchema, exprOpt[i]))
		default:
			panic("unsupported expr type of call processor")
		}
	}
	coProcessor := NewCoProcessorImpl()
	for i := range exprOpt {
		switch exprOpt[i].Expr.(type) {
		case *influxql.Call:
			name := exprOpt[i].Expr.(*influxql.Call).Name
			switch name {
			case "count":
				coProcessor.AppendRoutine(newCountRoutineImpl(inSchema, outSchema, exprOpt[i], auxProcessors))
			case "sum":
				coProcessor.AppendRoutine(newSumRoutineImpl(inSchema, outSchema, exprOpt[i], auxProcessors))
			case "first":
				coProcessor.AppendRoutine(newFirstRoutineImpl(inSchema, outSchema, exprOpt[i], auxProcessors))
				initColMata = true
			case "last":
				coProcessor.AppendRoutine(newLastRoutineImpl(inSchema, outSchema, exprOpt[i], auxProcessors))
				initColMata = true
			case "min":
				coProcessor.AppendRoutine(newMinRoutineImpl(inSchema, outSchema, exprOpt[i], auxProcessors))
			case "max":
				coProcessor.AppendRoutine(newMaxRoutineImpl(inSchema, outSchema, exprOpt[i], auxProcessors))
			case "distinct":
				coProcessor.AppendRoutine(newDistinctRoutineImpl(inSchema, outSchema, exprOpt[i]))
			case "top":
				coProcessor.AppendRoutine(newTopRoutineImpl(inSchema, outSchema, exprOpt[i], auxProcessors))
				initColMata = true
			case "bottom":
				coProcessor.AppendRoutine(newBottomRoutineImpl(inSchema, outSchema, exprOpt[i], auxProcessors))
				initColMata = true
			default:
				panic(fmt.Sprintf("unsupported [%s] aggregation operator of call processor", name))
			}
		}
	}
	return coProcessor, initColMata, callCount > 1
}

func newCountRoutineImpl(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions, auxProcessors []*auxProcessor) Routine {
	inOrdinal := inSchema.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outSchema.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for count iterator")
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Int:
		return NewRoutineImpl(
			newIntegerColIntegerReducer(integerCountReduce, integerCountMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Float:
		return NewRoutineImpl(
			newFloatColIntegerReducer(floatCountReduce, integerCountMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Boolean:
		return NewRoutineImpl(
			newBooleanColIntegerReducer(booleanCountReduce, integerCountMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_String, influx.Field_Type_Tag:
		return NewRoutineImpl(
			newStringColIntegerReducer(stringCountReduce, integerCountMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	default:
		panic(fmt.Sprintf("unsupported count iterator type: %d", dataType))
	}
}

func newSumRoutineImpl(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions, auxProcessors []*auxProcessor) Routine {
	inOrdinal := inSchema.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outSchema.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for sum iterator")
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Int:
		return NewRoutineImpl(
			newIntegerColIntegerReducer(integerSumReduce, integerSumMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Float:
		return NewRoutineImpl(
			newFloatColFloatReducer(floatSumReduce, floatSumMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	default:
		panic(fmt.Sprintf("unsupported sum iterator type: %d", dataType))
	}
}

func newMinRoutineImpl(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions, auxProcessors []*auxProcessor) Routine {
	inOrdinal := inSchema.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outSchema.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for min iterator")
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Int:
		return NewRoutineImpl(
			newIntegerColIntegerReducer(integerMinReduce, integerMinMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Float:
		return NewRoutineImpl(
			newFloatColFloatReducer(floatMinReduce, floatMinMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Boolean:
		return NewRoutineImpl(
			newBooleanColBooleanReducer(booleanMinReduce, booleanMinMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	default:
		panic(fmt.Sprintf("unsupported min iterator type: %d", dataType))
	}
}

func newMaxRoutineImpl(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions, auxProcessors []*auxProcessor) Routine {
	inOrdinal := inSchema.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outSchema.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for max iterator")
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Int:
		return NewRoutineImpl(
			newIntegerColIntegerReducer(integerMaxReduce, integerMaxMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Float:
		return NewRoutineImpl(
			newFloatColFloatReducer(floatMaxReduce, floatMaxMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Boolean:
		return NewRoutineImpl(
			newBooleanColBooleanReducer(booleanMaxReduce, booleanMaxMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	default:
		panic(fmt.Sprintf("unsupported max iterator type: %d", dataType))
	}
}

func newFirstRoutineImpl(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions, auxProcessors []*auxProcessor) Routine {
	inOrdinal := inSchema.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outSchema.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for first iterator")
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Int:
		return NewRoutineImpl(
			newIntegerTimeColIntegerReducer(integerFirstReduce, integerFirstMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Float:
		return NewRoutineImpl(
			newFloatTimeColFloatReducer(floatFirstReduce, floatFirstMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Boolean:
		return NewRoutineImpl(
			newBooleanTimeColBooleanReducer(booleanFirstReduce, booleanFirstMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_String, influx.Field_Type_Tag:
		return NewRoutineImpl(
			newStringTimeColStringReducer(stringFirstReduce, stringFirstMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	default:
		panic(fmt.Sprintf("unsupported first iterator type: %d", dataType))
	}
}

func newLastRoutineImpl(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions, auxProcessors []*auxProcessor) Routine {
	inOrdinal := inSchema.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outSchema.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for last iterator")
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Int:
		return NewRoutineImpl(
			newIntegerTimeColIntegerReducer(integerLastReduce, integerLastMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Float:
		return NewRoutineImpl(
			newFloatTimeColFloatReducer(floatLastReduce, floatLastMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Boolean:
		return NewRoutineImpl(
			newBooleanTimeColBooleanReducer(booleanLastReduce, booleanLastMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_String, influx.Field_Type_Tag:
		return NewRoutineImpl(
			newStringTimeColStringReducer(stringLastReduce, stringLastMerge, auxProcessors),
			inOrdinal,
			outOrdinal,
		)
	default:
		panic(fmt.Sprintf("unsupported last iterator type: %d", dataType))
	}
}

func newDistinctRoutineImpl(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions) Routine {
	inOrdinal := inSchema.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outSchema.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for distinct iterator")
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Int:
		return NewRoutineImpl(
			newIntegerColIntegerDistinctReducer(),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Float:
		return NewRoutineImpl(
			newFloatColFloatDistinctReducer(),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Boolean:
		return NewRoutineImpl(
			newBooleanColBooleanDistinctReducer(),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_String, influx.Field_Type_Tag:
		return NewRoutineImpl(
			newStringColStringDistinctReducer(),
			inOrdinal,
			outOrdinal,
		)
	default:
		panic(fmt.Sprintf("unsupported Distinct iterator type: %d", dataType))
	}
}

func newTopRoutineImpl(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions, auxProcessors []*auxProcessor) Routine {
	expr, ok := opt.Expr.(*influxql.Call)
	if !ok {
		panic(fmt.Errorf("NewTopRoutineImpl input illegal, opt.Expr is not influxql.Call"))
	}
	if len(expr.Args) < 2 {
		panic(fmt.Errorf("top() requires 2 or more arguments, got %d", len(expr.Args)))
	}
	inOrdinal := inSchema.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outSchema.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for last iterator")
	}
	n, ok := expr.Args[len(expr.Args)-1].(*influxql.IntegerLiteral)
	if !ok {
		panic(fmt.Errorf("NewTopRoutineImpl input illegal, opt.Args element is not influxql.IntegerLiteral"))
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Int:
		return NewRoutineImpl(
			NewIntegerColIntegerHeapReducer(inOrdinal, outOrdinal, auxProcessors, NewIntegerHeapItem(int(n.Val), integerCmpByValueTop, integerCmpByTimeTop)),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Float:
		return NewRoutineImpl(
			NewFloatColFloatHeapReducer(inOrdinal, outOrdinal, auxProcessors, NewFloatHeapItem(int(n.Val), floatCmpByValueTop, floatCmpByTimeTop)),
			inOrdinal,
			outOrdinal,
		)
	default:
		panic(fmt.Sprintf("unsupported top iterator type: %d", dataType))
	}
}

func newBottomRoutineImpl(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions, auxProcessors []*auxProcessor) Routine {
	expr, ok := opt.Expr.(*influxql.Call)
	if !ok {
		panic(fmt.Errorf("NewBottomRoutineImpl input illegal, opt.Expr is not influxql.Call"))
	}
	if len(expr.Args) < 2 {
		panic(fmt.Errorf("bottom() requires 2 or more arguments, got %d", len(expr.Args)))
	}
	inOrdinal := inSchema.FieldIndex(opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := outSchema.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for last iterator")
	}
	n, ok := expr.Args[len(expr.Args)-1].(*influxql.IntegerLiteral)
	if !ok {
		panic(fmt.Errorf("NewTopRoutineImpl input illegal, opt.Args element is not influxql.IntegerLiteral"))
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Int:
		return NewRoutineImpl(
			NewIntegerColIntegerHeapReducer(inOrdinal, outOrdinal, auxProcessors, NewIntegerHeapItem(int(n.Val), integerCmpByValueBottom, integerCmpByTimeBottom)),
			inOrdinal,
			outOrdinal,
		)
	case influx.Field_Type_Float:
		return NewRoutineImpl(
			NewFloatColFloatHeapReducer(inOrdinal, outOrdinal, auxProcessors, NewFloatHeapItem(int(n.Val), floatCmpByValueBottom, floatCmpByTimeBottom)),
			inOrdinal,
			outOrdinal,
		)
	default:
		panic(fmt.Sprintf("unsupported bottom iterator type: %d", dataType))
	}
}

func newAuxProcessor(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions) *auxProcessor {
	inOrdinal, outOrdinal := inSchema.FieldIndex(opt.Expr.(*influxql.VarRef).Val), outSchema.FieldIndex(opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		panic("input and output schemas are not aligned for aux iterator")
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Int:
		return &auxProcessor{
			inOrdinal:     inOrdinal,
			outOrdinal:    outOrdinal,
			auxHelperFunc: integerAuxHelpFunc,
		}
	case influx.Field_Type_Float:
		return &auxProcessor{
			inOrdinal:     inOrdinal,
			outOrdinal:    outOrdinal,
			auxHelperFunc: floatAuxHelpFunc,
		}
	case influx.Field_Type_String, influx.Field_Type_Tag:
		return &auxProcessor{
			inOrdinal:     inOrdinal,
			outOrdinal:    outOrdinal,
			auxHelperFunc: stringAuxHelpFunc,
		}
	case influx.Field_Type_Boolean:
		return &auxProcessor{
			inOrdinal:     inOrdinal,
			outOrdinal:    outOrdinal,
			auxHelperFunc: booleanAuxHelpFunc,
		}
	default:
		panic(fmt.Sprintf("unsupported aux iterator type: %d", dataType))
	}
}

type auxProcessor struct {
	inOrdinal     int
	outOrdinal    int
	auxHelperFunc func(input, output *record.ColVal, index ...int)
}
