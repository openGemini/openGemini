// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package sparseindex

import (
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/logparser"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type checkInRangeFunc func(rgs []*Range) (Mark, error)

type KeyCondition interface {
	HavePrimaryKey() bool
	GetMaxKeyIndex() int
	IsFirstPrimaryKey() bool
	CanDoBinarySearch() bool
	MayBeInRange(usedKeySize int, indexLeft []*FieldRef, indexRight []*FieldRef, dataTypes []int) (bool, error)
	CheckInRange(rgs []*Range, dataTypes []int) (Mark, error)
	AlwaysInRange() (bool, error)
}

type KeyConditionImpl struct {
	pkSchema record.Schemas
	rpn      []*RPNElement
}

func NewKeyCondition(timeCondition, condition influxql.Expr, pkSchema record.Schemas) (*KeyConditionImpl, error) {
	kc := &KeyConditionImpl{pkSchema: pkSchema}
	cols := genIndexColumnsBySchema(pkSchema)
	// use "AND" to connect the time condition to other conditions.
	combineCondition := binaryfilterfunc.CombineConditionWithAnd(timeCondition, condition)
	rpnExpr := rpn.ConvertToRPNExpr(combineCondition)
	if err := kc.convertToRPNElem(rpnExpr, cols); err != nil {
		return nil, err
	}
	return kc, nil
}

func (kc *KeyConditionImpl) convertToRPNElem(
	rpnExpr *rpn.RPNExpr,
	cols []*ColumnRef,
) error {
	for i, expr := range rpnExpr.Val {
		switch v := expr.(type) {
		case influxql.Token:
			switch v {
			case influxql.AND:
				kc.rpn = append(kc.rpn, &RPNElement{op: rpn.AND})
			case influxql.OR:
				kc.rpn = append(kc.rpn, &RPNElement{op: rpn.OR})
			case influxql.EQ, influxql.LT, influxql.LTE, influxql.GT, influxql.GTE, influxql.NEQ, influxql.MATCHPHRASE:
			default:
				return errno.NewError(errno.ErrRPNOp, v)
			}
		case *influxql.VarRef:
			// a filter expression is converted into three elements. such as, (A = 'a') => (A 'a' =).
			// the first element is a field. rpnExpr.Val[i]
			// the second element is a value. rpnExpr.Val[i+1]
			// the third element is a operator. rpnExpr.Val[i+2]
			idx := kc.pkSchema.FieldIndex(v.Val)
			if idx < 0 {
				kc.rpn = append(kc.rpn, &RPNElement{op: rpn.AlwaysTrue})
				continue
			}
			if i+2 >= len(rpnExpr.Val) {
				return errno.NewError(errno.ErrRPNElemNum)
			}
			value := rpnExpr.Val[i+1]
			op, ok := rpnExpr.Val[i+2].(influxql.Token)
			if !ok {
				return errno.NewError(errno.ErrRPNElemOp)
			}
			if err := kc.genRPNElementByVal(value, op, cols, idx); err != nil {
				return err
			}
		case *influxql.StringLiteral, *influxql.NumberLiteral, *influxql.IntegerLiteral, *influxql.BooleanLiteral:
		default:
			return errno.NewError(errno.ErrRPNExpr, v)
		}
	}
	return nil
}

func (kc *KeyConditionImpl) genRPNElementByVal(
	rhs interface{},
	op influxql.Token,
	cols []*ColumnRef,
	idx int,
) error {
	rpnElem := &RPNElement{keyColumn: idx}
	value := NewFieldRef(cols, idx, 0)
	switch rhs := rhs.(type) {
	case *influxql.StringLiteral:
		value.cols[idx].column.AppendString(rhs.Val)
	case *influxql.NumberLiteral:
		value.cols[idx].column.AppendFloat(rhs.Val)
	case *influxql.IntegerLiteral:
		value.cols[idx].column.AppendInteger(rhs.Val)
	case *influxql.BooleanLiteral:
		value.cols[idx].column.AppendBoolean(rhs.Val)
	default:
		return errno.NewError(errno.ErrRPNElement, rhs)
	}
	if value.cols[idx].column.Len > 1 {
		value.row = value.cols[idx].column.Len - 1
	}
	if ok := genRPNElementByOp(op, value, rpnElem); ok {
		kc.rpn = append(kc.rpn, rpnElem)
	}
	return nil
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

// CheckInRange check Whether the condition and its negation are feasible
// in the direct product of single column ranges specified by hyper-rectangle.
func (kc *KeyConditionImpl) CheckInRange(
	rgs []*Range,
	dataTypes []int,
) (Mark, error) {
	var err error
	var singlePoint bool
	var rpnStack []Mark
	for _, elem := range kc.rpn {
		if elem.op == rpn.InRange || elem.op == rpn.NotInRange {
			rpnStack = kc.checkInRangeForRange(rgs, rpnStack, elem, dataTypes, singlePoint)
		} else if elem.op == rpn.InSet || elem.op == rpn.NotInSet {
			rpnStack, err = kc.checkInRangeForSet(rgs, rpnStack, elem, dataTypes, singlePoint)
			if err != nil {
				return Mark{}, err
			}
		} else if elem.op == rpn.AND {
			rpnStack, err = kc.checkInRangeForAnd(rpnStack)
			if err != nil {
				return Mark{}, err
			}
		} else if elem.op == rpn.OR {
			rpnStack, err = kc.checkInRangeForOr(rpnStack)
			if err != nil {
				return Mark{}, err
			}
		} else if elem.op == rpn.AlwaysTrue {
			rpnStack = append(rpnStack, NewMark(true, false))
		} else if elem.op == rpn.AlwaysFalse {
			rpnStack = append(rpnStack, NewMark(false, true))
		} else {
			return Mark{}, errno.NewError(errno.ErrUnknownOpInCondition)
		}
	}
	if len(rpnStack) != 1 {
		return Mark{}, errno.NewError(errno.ErrInvalidStackInCondition)
	}
	return rpnStack[0], nil
}

func (kc *KeyConditionImpl) checkInRangeForRange(
	rgs []*Range,
	rpnStack []Mark,
	elem *RPNElement,
	dataTypes []int,
	singlePoint bool,
) []Mark {
	keyRange := rgs[elem.keyColumn]
	if len(elem.monotonicChains) > 0 {
		newRange := kc.applyChainToRange(keyRange, elem.monotonicChains, dataTypes[elem.keyColumn], singlePoint)
		if newRange != nil {
			rpnStack = append(rpnStack, NewMark(true, true))
			return rpnStack
		}
		keyRange = newRange
	}
	intersects := elem.rg.intersectsRange(keyRange)
	contains := elem.rg.containsRange(keyRange)
	rpnStack = append(rpnStack, NewMark(intersects, !contains))
	if elem.op == rpn.NotInRange {
		rpnStack[len(rpnStack)-1] = rpnStack[len(rpnStack)-1].Not()
	}
	return rpnStack
}

func (kc *KeyConditionImpl) checkInRangeForSet(
	rgs []*Range,
	rpnStack []Mark,
	elem *RPNElement,
	dataTypes []int,
	singlePoint bool,
) ([]Mark, error) {
	if elem.setIndex.Not() {
		return nil, errno.NewError(errno.ErrRPNSetInNotCreated)
	}
	rpnStack = append(rpnStack, elem.setIndex.checkInRange(rgs, dataTypes, singlePoint))
	if elem.op == rpn.NotInSet {
		rpnStack[len(rpnStack)-1] = rpnStack[len(rpnStack)-1].Not()
	}
	return rpnStack, nil
}

func (kc *KeyConditionImpl) checkInRangeForAnd(
	rpnStack []Mark,
) ([]Mark, error) {
	if len(rpnStack) == 0 {
		return nil, errno.NewError(errno.ErrRPNIsNullForAnd)
	}
	rg1 := (rpnStack)[len(rpnStack)-1]
	rpnStack = rpnStack[:len(rpnStack)-1]
	rpnStack[len(rpnStack)-1] = rpnStack[len(rpnStack)-1].And(rg1)
	return rpnStack, nil
}

func (kc *KeyConditionImpl) checkInRangeForOr(
	rpnStack []Mark,
) ([]Mark, error) {
	if len(rpnStack) == 0 {
		return nil, errno.NewError(errno.ErrRPNIsNullForOR)
	}
	rg1 := rpnStack[len(rpnStack)-1]
	rpnStack = rpnStack[:len(rpnStack)-1]
	rpnStack[len(rpnStack)-1] = rpnStack[len(rpnStack)-1].Or(rg1)
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
	initMask Mark,
	callBack checkInRangeFunc,
) (Mark, error) {
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
		res, completed, err = kc.checkRangeLeftBound(keySize, leftKeys, rightKeys, rgs, dataTypes, prefixSize, initMask, res, callBack)
		if err != nil || completed {
			return res, err
		}
	}

	// [x2] x (-inf .. y2]
	if rightBounded {
		res, completed, err = kc.checkRangeRightBound(keySize, leftKeys, rightKeys, rgs, dataTypes, prefixSize, initMask, res, callBack)
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
	initMask Mark,
	rgs []*Range,
	dataTypes []int,
	prefixSize int,
	callBack checkInRangeFunc,
) (Mark, bool, error) {
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
		return Mark{}, false, err
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
	initMask Mark,
	res Mark,
	callBack checkInRangeFunc,
) (Mark, bool, error) {
	rgs[prefixSize] = NewRange(leftKeys[prefixSize], leftKeys[prefixSize], true, true)
	mark, err := kc.checkInAnyRange(keySize, leftKeys, rightKeys, true, false, rgs, dataTypes, prefixSize+1, initMask, callBack)
	if err != nil {
		return res, false, err
	}
	res = res.Or(mark)
	if res.isComplete() {
		return res, true, nil
	}
	return res, false, nil
}

func (kc *KeyConditionImpl) checkRangeRightBound(
	keySize int,
	leftKeys []*FieldRef,
	rightKeys []*FieldRef,
	rgs []*Range,
	dataTypes []int,
	prefixSize int,
	initMask Mark,
	res Mark,
	callBack checkInRangeFunc,
) (Mark, bool, error) {
	rgs[prefixSize] = NewRange(rightKeys[prefixSize], rightKeys[prefixSize], true, true)
	mark, err := kc.checkInAnyRange(keySize, leftKeys, rightKeys, false, true, rgs, dataTypes, prefixSize+1, initMask, callBack)
	if err != nil {
		return mark, false, err
	}
	res = res.Or(mark)
	if res.isComplete() {
		return mark, true, nil
	}
	return mark, false, nil
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
		func(rgs []*Range) (Mark, error) {
			res, err := kc.CheckInRange(rgs, dataTypes)
			return res, err
		})
	if err != nil {
		return false, err
	}
	return mark.canBeTrue, nil
}

// AlwaysInRange checks that the index can not be used, pruning in advance to improve efficiency.
func (kc *KeyConditionImpl) AlwaysInRange() (bool, error) {
	var rpnStack []bool
	for _, elem := range kc.rpn {
		if elem.op == rpn.UNKNOWN {
			return true, nil
		} else if elem.op == rpn.InRange || elem.op == rpn.NotInRange || elem.op == rpn.InSet || elem.op == rpn.NotInSet {
			rpnStack = append(rpnStack, false)
		} else if elem.op == rpn.NOT {
			// Not as a logical operator followed by an expression
		} else if elem.op == rpn.AND {
			if len(rpnStack) == 0 {
				return false, errno.NewError(errno.ErrRPNIsNullForAnd)
			}
			v1 := rpnStack[len(rpnStack)-1]
			rpnStack = rpnStack[:len(rpnStack)-1]
			v2 := rpnStack[len(rpnStack)-1]
			rpnStack[len(rpnStack)-1] = v1 && v2
		} else if elem.op == rpn.OR {
			if len(rpnStack) == 0 {
				return false, errno.NewError(errno.ErrRPNIsNullForOR)
			}
			v1 := rpnStack[len(rpnStack)-1]
			rpnStack = rpnStack[:len(rpnStack)-1]
			v2 := rpnStack[len(rpnStack)-1]
			rpnStack[len(rpnStack)-1] = v1 || v2
		} else {
			return false, errno.NewError(errno.ErrUnknownOpInCondition)
		}
	}
	if len(rpnStack) != 1 {
		return false, errno.NewError(errno.ErrInvalidStackInCondition)
	}
	return rpnStack[0], nil
}

func (kc *KeyConditionImpl) HavePrimaryKey() bool {
	return len(kc.rpn) > 0
}

func (kc *KeyConditionImpl) GetMaxKeyIndex() int {
	res := -1
	for i := range kc.rpn {
		if kc.rpn[i].op <= rpn.NotInSet {
			res = hybridqp.MaxInt(res, kc.rpn[i].keyColumn)
		}
	}
	return res
}

func (kc *KeyConditionImpl) IsFirstPrimaryKey() bool {
	return kc.GetMaxKeyIndex() == 0
}

func (kc *KeyConditionImpl) CanDoBinarySearch() bool {
	return kc.IsFirstPrimaryKey()
}

func (kc *KeyConditionImpl) GetRPN() []*RPNElement {
	return kc.rpn
}

func (kc *KeyConditionImpl) SetRPN(rpn []*RPNElement) {
	kc.rpn = rpn
}

type SKCondition interface {
	IsExist(blockId int64, reader rpn.SKBaseReader) (bool, error)
}

type SKConditionImpl struct {
	schema   record.Schemas
	rpn      []*rpn.SKRPNElement
	rpnStack []bool
}

func NewSKCondition(rpnExpr *rpn.RPNExpr, schema record.Schemas) (SKCondition, error) {
	c := &SKConditionImpl{schema: schema, rpnStack: make([]bool, 0, len(rpnExpr.Val))}
	if err := c.convertToRPNElem(rpnExpr); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *SKConditionImpl) convertToRPNElem(rpnExpr *rpn.RPNExpr) error {
	for i, expr := range rpnExpr.Val {
		switch v := expr.(type) {
		case influxql.Token:
			switch v {
			case influxql.AND:
				c.rpn = append(c.rpn, &rpn.SKRPNElement{RPNOp: rpn.AND})
			case influxql.OR:
				c.rpn = append(c.rpn, &rpn.SKRPNElement{RPNOp: rpn.OR})
			case influxql.EQ, influxql.LT, influxql.LTE, influxql.GT, influxql.GTE, influxql.NEQ, influxql.MATCHPHRASE:
			default:
				return errno.NewError(errno.ErrRPNOp, v)
			}
		case *influxql.VarRef:
			if v.Val == logparser.DefaultFieldForFullText {
				if err := c.genRPNElementByFullText(v.Val, rpnExpr.Val[i+1], influxql.MATCHPHRASE); err != nil {
					return err
				}
				continue
			}
			idx := c.schema.FieldIndex(v.Val)
			if idx < 0 {
				c.rpn = append(c.rpn, &rpn.SKRPNElement{RPNOp: rpn.AlwaysTrue})
				continue
			}
			if i+2 >= len(rpnExpr.Val) {
				return errno.NewError(errno.ErrRPNElemNum)
			}
			value := rpnExpr.Val[i+1]
			op, ok := rpnExpr.Val[i+2].(influxql.Token)
			if !ok {
				return errno.NewError(errno.ErrRPNElemOp)
			}
			if err := c.genRPNElementByVal(v.Val, value, op); err != nil {
				return err
			}
		case *influxql.StringLiteral, *influxql.NumberLiteral, *influxql.IntegerLiteral, *influxql.BooleanLiteral:
		default:
			return errno.NewError(errno.ErrRPNExpr, v)
		}
	}
	return nil
}

func (c *SKConditionImpl) genRPNElementByFullText(key string, value interface{}, op influxql.Token) error {
	e := &rpn.SKRPNElement{RPNOp: rpn.InRange, Key: key, Op: op}
	v, ok := value.(*influxql.StringLiteral)
	if !ok {
		return errno.NewError(errno.ErrValueTypeFullTextIndex)
	}
	e.Value = v.Val
	e.Ty = influxql.String
	c.rpn = append(c.rpn, e)
	return nil
}

func (c *SKConditionImpl) genRPNElementByVal(key string, value interface{}, op influxql.Token) error {
	e := &rpn.SKRPNElement{RPNOp: rpn.InRange, Key: key, Op: op}
	switch val := value.(type) {
	case *influxql.StringLiteral:
		e.Value = val.Val
		e.Ty = influxql.String
	case *influxql.IntegerLiteral:
		e.Value = val.Val
		e.Ty = influxql.Integer
	case *influxql.NumberLiteral:
		e.Value = val.Val
		e.Ty = influxql.Float
	case *influxql.BooleanLiteral:
		e.Value = val.Val
		e.Ty = influxql.Boolean
	default:
		return errno.NewError(errno.ErrRPNElement, value)
	}
	c.rpn = append(c.rpn, e)
	return nil
}

func (c *SKConditionImpl) IsExist(blockId int64, reader rpn.SKBaseReader) (bool, error) {
	c.rpnStack = c.rpnStack[:0]
	for _, elem := range c.rpn {
		switch elem.RPNOp {
		case rpn.InRange:
			ok, err := reader.IsExist(blockId, elem)
			if err != nil {
				return false, err
			}
			c.rpnStack = append(c.rpnStack, ok)
		case rpn.AlwaysTrue:
			c.rpnStack = append(c.rpnStack, true)
		case rpn.AlwaysFalse:
			c.rpnStack = append(c.rpnStack, false)
		case rpn.AND:
			if len(c.rpnStack) < 2 {
				return false, errno.NewError(errno.ErrRPNIsNullForAnd)
			}
			v1 := c.rpnStack[len(c.rpnStack)-1]
			c.rpnStack = c.rpnStack[:len(c.rpnStack)-1]
			v2 := c.rpnStack[len(c.rpnStack)-1]
			c.rpnStack[len(c.rpnStack)-1] = v1 && v2
		case rpn.OR:
			if len(c.rpnStack) < 2 {
				return false, errno.NewError(errno.ErrRPNIsNullForOR)
			}
			v1 := c.rpnStack[len(c.rpnStack)-1]
			c.rpnStack = c.rpnStack[:len(c.rpnStack)-1]
			v2 := c.rpnStack[len(c.rpnStack)-1]
			c.rpnStack[len(c.rpnStack)-1] = v1 || v2
		default:
			return false, errno.NewError(errno.ErrUnknownOpInCondition)
		}
	}
	if len(c.rpnStack) != 1 {
		return false, errno.NewError(errno.ErrInvalidStackInCondition)
	}
	return c.rpnStack[0], nil
}
