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

package sparseindex

import (
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type checkInRangeFunc func(rgs []*Range) (*Mark, error)

type KeyCondition interface {
	HavePrimaryKey() bool
	GetMaxKeyIndex() int
	IsFirstPrimaryKey() bool
	CanDoBinarySearch() bool
	MayBeInRange(usedKeySize int, indexLeft []*FieldRef, indexRight []*FieldRef, dataTypes []int) (bool, error)
}

type KeyConditionImpl struct {
	condition influxql.Expr
	pkSchema  record.Schemas
	lhs       record.Schemas
	rhs       []influxql.Expr
	ops       []influxql.Token
	rpn       []*RPNElement
}

func NewKeyCondition(condition influxql.Expr, pkSchema record.Schemas) *KeyConditionImpl {
	var stack []influxql.Token
	kc := &KeyConditionImpl{condition: condition, pkSchema: pkSchema}
	cols := genIndexColumnsBySchema(pkSchema)
	kc.initRPN(condition, cols, &stack)
	for len(stack) > 0 {
		kc.rpn = append(kc.rpn, &RPNElement{op: InfluxOpToFunction(stack[len(stack)-1])})
		stack = stack[:len(stack)-1]
	}
	return kc
}

func (kc *KeyConditionImpl) initRPN(
	condition influxql.Expr,
	cols []*ColumnRef,
	stack *[]influxql.Token,
) {
	switch condition.(type) {
	case *influxql.BinaryExpr:
		b := condition.(*influxql.BinaryExpr)
		if IsLogicalOperator(b.Op) && !IsFieldKey(b, kc.pkSchema) {
			*stack = append(*stack, b.Op)
		}
		if nb, ok := b.LHS.(*influxql.BinaryExpr); ok {
			kc.initRPN(nb, cols, stack)
		}
		if nb, ok := b.RHS.(*influxql.BinaryExpr); ok {
			kc.initRPN(nb, cols, stack)
		}
		if lv, ok := b.LHS.(*influxql.VarRef); ok {
			kc.lhs = append(kc.lhs, record.Field{Name: lv.Val, Type: record.ToModelTypes(lv.Type)})
			kc.rhs = append(kc.rhs, b.RHS)
			kc.ops = append(kc.ops, b.Op)
			kc.genRPNElement(b, cols, stack, lv.Val)
		}
	default:
		return
	}
}

func (kc *KeyConditionImpl) genRPNElement(
	b *influxql.BinaryExpr,
	cols []*ColumnRef,
	stack *[]influxql.Token,
	lvVal string,
) {
	var ok bool
	if idx := kc.pkSchema.FieldIndex(lvVal); idx >= 0 {
		rpnElem := &RPNElement{keyColumn: idx}
		switch b.RHS.(type) {
		case *influxql.StringLiteral:
			value := NewFieldRef(cols, idx, 0)
			val := b.RHS.(*influxql.StringLiteral).Val
			value.cols[idx].column.AppendString(val)
			if value.cols[idx].column.Len > 1 {
				value.row = value.cols[idx].column.Len - 1
			}
			ok = genRPNElementByOp(b.Op, value, rpnElem)
		case *influxql.NumberLiteral:
			value := NewFieldRef(cols, idx, 0)
			val := b.RHS.(*influxql.NumberLiteral).Val
			value.cols[idx].column.AppendFloat(val)
			if value.cols[idx].column.Len > 1 {
				value.row = value.cols[idx].column.Len - 1
			}
			ok = genRPNElementByOp(b.Op, value, rpnElem)
		case *influxql.IntegerLiteral:
			value := NewFieldRef(cols, idx, 0)
			val := b.RHS.(*influxql.IntegerLiteral).Val
			value.cols[idx].column.AppendInteger(val)
			if value.cols[idx].column.Len > 1 {
				value.row = value.cols[idx].column.Len - 1
			}
			ok = genRPNElementByOp(b.Op, value, rpnElem)
		default:
			ok = false
		}
		if ok {
			kc.rpn = append(kc.rpn, rpnElem)
		}
		if IsLogicalOperator(b.Op) {
			for len(*stack) > 0 &&
				IsLogicalOperator((*stack)[len(kc.rpn)-1]) &&
				InfluxOpToFunction(b.Op) < InfluxOpToFunction((*stack)[len(kc.rpn)-1]) {
				kc.rpn = append(kc.rpn, &RPNElement{op: InfluxOpToFunction((*stack)[len(*stack)-1])})
				*stack = (*stack)[:len(*stack)-1]
			}
			*stack = append(*stack, b.Op)
		}
	}
}

// applyChainToRange apply the monotonicity of each function on a specific range.
func (kc *KeyConditionImpl) applyChainToRange(
	_ *Range,
	_ []*FunctionBase,
	_ int,
	_ bool,
) *Range {
	return &Range{}
}

// checkInAnyRange check Whether the condition and its negation are feasible
// in the direct product of single column ranges specified by hyper-rectangle.
func (kc *KeyConditionImpl) checkInRange(
	rgs []*Range,
	dataTypes []int,
) (*Mark, error) {
	var err error
	var singlePoint bool
	var beContinue bool
	var rpnStack []*Mark
	for _, elem := range kc.rpn {
		if elem.op == InRange || elem.op == NotInRange {
			rpnStack, beContinue = kc.checkInRangeForRange(rgs, rpnStack, elem, dataTypes, singlePoint)
			if beContinue {
				continue
			}
		} else if elem.op == InSet || elem.op == NotInSet {
			rpnStack, err = kc.checkInRangeForSet(rgs, rpnStack, elem, dataTypes, singlePoint)
			if err != nil {
				return nil, err
			}
		} else if elem.op == AND {
			rpnStack, err = kc.checkInRangeForAnd(rpnStack)
			if err != nil {
				return nil, err
			}
		} else if elem.op == OR {
			rpnStack, err = kc.checkInRangeForOr(rpnStack)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errno.NewError(errno.ErrUnknownOpInCondition)
		}
	}
	if len(rpnStack) != 1 {
		return nil, errno.NewError(errno.ErrInvalidStackInCondition)
	}
	return rpnStack[0], nil
}

func (kc *KeyConditionImpl) checkInRangeForRange(
	rgs []*Range,
	rpnStack []*Mark,
	elem *RPNElement,
	dataTypes []int,
	singlePoint bool,
) ([]*Mark, bool) {
	keyRange := rgs[elem.keyColumn]
	transformedRange := createWholeRangeIncludeBound()
	if len(elem.monotonicChains) > 0 {
		newRange := kc.applyChainToRange(keyRange, elem.monotonicChains, dataTypes[elem.keyColumn], singlePoint)
		if newRange != nil {
			rpnStack = append(rpnStack, NewMark(true, true))
			return rpnStack, true
		}
		transformedRange = newRange
		keyRange = transformedRange
	}
	intersects := elem.rg.intersectsRange(keyRange)
	contains := elem.rg.containsRange(keyRange)
	rpnStack = append(rpnStack, NewMark(intersects, !contains))
	if elem.op == NotInRange {
		rpnStack[len(rpnStack)-1] = rpnStack[len(rpnStack)-1].Not()
	}
	return rpnStack, false
}

func (kc *KeyConditionImpl) checkInRangeForSet(
	rgs []*Range,
	rpnStack []*Mark,
	elem *RPNElement,
	dataTypes []int,
	singlePoint bool,
) ([]*Mark, error) {
	if elem.setIndex.Not() {
		return nil, errno.NewError(errno.ErrRPNSetInNotCreated)
	}
	rpnStack = append(rpnStack, elem.setIndex.checkInRange(rgs, dataTypes, singlePoint))
	if elem.op == NotInSet {
		rpnStack[len(rpnStack)-1] = rpnStack[len(rpnStack)-1].Not()
	}
	return rpnStack, nil
}

func (kc *KeyConditionImpl) checkInRangeForAnd(
	rpnStack []*Mark,
) ([]*Mark, error) {
	if len(rpnStack) == 0 {
		return nil, errno.NewError(errno.ErrRPNIsNullForAnd)
	}
	rg1 := (rpnStack)[len(rpnStack)-1]
	rpnStack = rpnStack[:len(rpnStack)-1]
	rg2 := rpnStack[len(rpnStack)-1]
	rpnStack[len(rpnStack)-1] = rg1.And(rg2)
	return rpnStack, nil
}

func (kc *KeyConditionImpl) checkInRangeForOr(
	rpnStack []*Mark,
) ([]*Mark, error) {
	if len(rpnStack) == 0 {
		return nil, errno.NewError(errno.ErrRPNIsNullForOR)
	}
	rg1 := rpnStack[len(rpnStack)-1]
	rpnStack = rpnStack[:len(rpnStack)-1]
	rg2 := rpnStack[len(rpnStack)-1]
	rpnStack[len(rpnStack)-1] = rg1.Or(rg2)
	return rpnStack, nil
}

// checkInAnyRange check Whether the condition and its negation are feasible
// in the direct product of single column ranges specified by any hyper-rectangle.
func (kc *KeyConditionImpl) checkInAnyRange(
	keySize int,
	leftKeys []*FieldRef,
	rightKeys []*FieldRef,
	leftBounded bool,
	rightBounded bool,
	rgs []*Range,
	dataTypes []int,
	prefixSize int,
	initMask *Mark,
	callBack checkInRangeFunc,
) (*Mark, error) {
	if !leftBounded && !rightBounded {
		return callBack(rgs)
	}
	if leftBounded && rightBounded {
		for prefixSize < keySize {
			if leftKeys[prefixSize].Equals(rightKeys[prefixSize]) {
				rgs[prefixSize] = NewRange(leftKeys[prefixSize], leftKeys[prefixSize], true, true)
				prefixSize++
			} else {
				break
			}
		}
	}
	if prefixSize == keySize {
		return callBack(rgs)
	}

	// (x1 .. x2) x (-inf .. +inf)
	res, completed, err := kc.checkRangeLeftRightBound(leftKeys, rightKeys, leftBounded, rightBounded, keySize, initMask, rgs, dataTypes, prefixSize, callBack)
	if err != nil || completed {
		return res, err
	}

	// [x1] x [y1 .. +inf)
	if leftBounded {
		completed, err = kc.checkRangeLeftBound(keySize, leftKeys, rightKeys, rgs, dataTypes, prefixSize, initMask, res, callBack)
		if err != nil || completed {
			return res, err
		}
	}

	// [x2] x (-inf .. y2]
	if rightBounded {
		completed, err = kc.checkRangeRightBound(keySize, leftKeys, rightKeys, rgs, dataTypes, prefixSize, initMask, res, callBack)
		if err != nil || completed {
			return res, err
		}
	}
	return res, nil
}

func (kc *KeyConditionImpl) checkRangeLeftRightBound(
	leftKeys []*FieldRef,
	rightKeys []*FieldRef,
	leftBounded bool,
	rightBounded bool,
	keySize int,
	initMask *Mark,
	rgs []*Range,
	dataTypes []int,
	prefixSize int,
	callBack checkInRangeFunc,
) (*Mark, bool, error) {
	if prefixSize+1 == keySize {
		if leftBounded && rightBounded {
			rgs[prefixSize] = NewRange(leftKeys[prefixSize], rightKeys[prefixSize], true, true)
		} else if leftBounded {
			rgs[prefixSize] = createLeftBounded(leftKeys[prefixSize], true, false)
		} else if rightBounded {
			rgs[prefixSize] = createRightBounded(rightKeys[prefixSize], true, false)
		}
		m, err := callBack(rgs)
		return m, true, err
	}
	if leftBounded && rightBounded {
		rgs[prefixSize] = NewRange(leftKeys[prefixSize], rightKeys[prefixSize], false, false)
	} else if leftBounded {
		rgs[prefixSize] = createLeftBounded(leftKeys[prefixSize], false, dataTypes[prefixSize] == influx.Field_Type_Unknown)
	} else if rightBounded {
		rgs[prefixSize] = createRightBounded(rightKeys[prefixSize], false, dataTypes[prefixSize] == influx.Field_Type_Unknown)
	}
	for i := prefixSize + 1; i < keySize; i++ {
		if dataTypes[i] == influx.Field_Type_Unknown {
			rgs[i] = createWholeRangeIncludeBound()
		} else {
			rgs[i] = createWholeRangeWithoutBound()
		}
	}

	res := initMask
	m, err := callBack(rgs)
	if err != nil {
		return nil, false, err
	}
	res = res.Or(m)

	/// There are several early-exit conditions (like the one below) hereinafter.
	if res.isComplete() {
		return res, true, nil
	}
	return res, false, nil
}

func (kc *KeyConditionImpl) checkRangeLeftBound(
	keySize int,
	leftKeys []*FieldRef,
	rightKeys []*FieldRef,
	rgs []*Range,
	dataTypes []int,
	prefixSize int,
	initMask *Mark,
	res *Mark,
	callBack checkInRangeFunc,
) (bool, error) {
	rgs[prefixSize] = NewRange(leftKeys[prefixSize], leftKeys[prefixSize], true, true)
	mark, err := kc.checkInAnyRange(keySize, leftKeys, rightKeys, true, false, rgs, dataTypes, prefixSize+1, initMask, callBack)
	if err != nil {
		return false, err
	}
	res = res.Or(mark)
	if res.isComplete() {
		return true, nil
	}
	return false, nil
}

func (kc *KeyConditionImpl) checkRangeRightBound(
	keySize int,
	leftKeys []*FieldRef,
	rightKeys []*FieldRef,
	rgs []*Range,
	dataTypes []int,
	prefixSize int,
	initMask *Mark,
	res *Mark,
	callBack checkInRangeFunc,
) (bool, error) {
	rgs[prefixSize] = NewRange(rightKeys[prefixSize], rightKeys[prefixSize], true, true)
	mark, err := kc.checkInAnyRange(keySize, leftKeys, rightKeys, false, true, rgs, dataTypes, prefixSize+1, initMask, callBack)
	if err != nil {
		return false, err
	}
	res = res.Or(mark)
	if res.isComplete() {
		return true, nil
	}
	return false, nil
}

// MayBeInRange is used to check whether the condition is likely to be in the target range.
func (kc *KeyConditionImpl) MayBeInRange(
	usedKeySize int,
	leftKeys []*FieldRef,
	rightKeys []*FieldRef,
	dataTypes []int,
) (bool, error) {
	initMask := ConsiderOnlyBeTrue
	keyRgs := make([]*Range, 0, usedKeySize)
	for i := 0; i < usedKeySize; i++ {
		if dataTypes[i] == influx.Field_Type_Unknown {
			keyRgs = append(keyRgs, createWholeRangeIncludeBound())
		} else {
			keyRgs = append(keyRgs, createWholeRangeWithoutBound())
		}
	}
	mark, err := kc.checkInAnyRange(
		usedKeySize, leftKeys, rightKeys, true, true, keyRgs, dataTypes, 0, initMask,
		func(rgs []*Range) (*Mark, error) {
			res, err := kc.checkInRange(rgs, dataTypes)
			return res, err
		})
	if err != nil {
		return false, err
	}
	return mark.canBeTrue, nil
}

func (kc *KeyConditionImpl) HavePrimaryKey() bool {
	for i := range kc.rhs {
		if kc.pkSchema.FieldIndex(kc.lhs[i].Name) >= 0 {
			return true
		}
	}
	return false
}
func (kc *KeyConditionImpl) GetMaxKeyIndex() int {
	var res int
	for i := range kc.rhs {
		res = hybridqp.MaxInt(res, kc.pkSchema.FieldIndex(kc.lhs[i].Name))
	}
	return res
}

func (kc *KeyConditionImpl) IsFirstPrimaryKey() bool {
	return kc.GetMaxKeyIndex() == 0
}

func (kc *KeyConditionImpl) CanDoBinarySearch() bool {
	return kc.IsFirstPrimaryKey()
}
