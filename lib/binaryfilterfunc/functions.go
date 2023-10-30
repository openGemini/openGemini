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

package binaryfilterfunc

import (
	"errors"

	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

func init() {
	initIdxTypeFun()
}

func LeftRotate(expr influxql.Expr) influxql.Expr {
	exp, ok := expr.(*influxql.BinaryExpr)
	if !ok {
		return expr
	}

	headExpr := exp.RHS
	e, ok := headExpr.(*influxql.BinaryExpr)
	if ok {
		exp.RHS = e.LHS
	} else {
		return expr
	}
	e.LHS = exp

	return e
}

func RightRotate(expr influxql.Expr) influxql.Expr {
	exp, ok := expr.(*influxql.BinaryExpr)
	if !ok {
		return expr
	}

	headExpr := exp.LHS
	e, ok := headExpr.(*influxql.BinaryExpr)
	if ok {
		exp.LHS = e.RHS
	} else {
		return expr
	}
	e.RHS = exp

	return e
}

func MoveOrOpToRoot(expr influxql.Expr) influxql.Expr {
	root := RotateOrOpToRoot(expr)
	rootExpr, ok := root.(*influxql.BinaryExpr)
	if ok {
		if rootExpr.LHS != nil {
			rootExpr.LHS = MoveOrOpToRoot(rootExpr.LHS)
		}
		if rootExpr.RHS != nil {
			rootExpr.RHS = MoveOrOpToRoot(rootExpr.RHS)
		}
	}
	return root
}

func RotateOrOpToRoot(expr influxql.Expr) influxql.Expr {
	exp, ok := expr.(*influxql.BinaryExpr)
	if !ok {
		return expr
	}

	if exp.Op == influxql.OR {
		return exp
	}
	exp.LHS = RotateOrOpToRoot(exp.LHS)
	exp.RHS = RotateOrOpToRoot(exp.RHS)

	var newExp influxql.Expr
	newExp = exp
	leftExpr, leftOk := exp.LHS.(*influxql.BinaryExpr)
	if !leftOk {
		return expr
	}

	rightExpr, rightOk := exp.RHS.(*influxql.BinaryExpr)
	if !rightOk {
		return expr
	}

	if leftOk && leftExpr.Op == influxql.OR {
		newExp = RightRotate(exp)
	} else if rightOk && rightExpr.Op == influxql.OR {
		newExp = LeftRotate(exp)
	}
	return newExp
}

func SplitWithOrOperation(expr influxql.Expr) []influxql.Expr {
	var res []influxql.Expr
	exp, ok := expr.(*influxql.BinaryExpr)
	if !ok || exp.Op != influxql.OR {
		res = append(res, expr)
		return res
	}

	res = append(res, SplitWithOrOperation(exp.LHS)...)
	res = append(res, SplitWithOrOperation(exp.RHS)...)
	return res
}

type IdxFunction struct {
	Idx      int
	Function func(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte
	Compare  interface{}
	Op       influxql.Token
}

type IdxFunctions []IdxFunction

type CondFunctions []IdxFunctions

func InitCondFunctions(expr influxql.Expr, schema *record.Schemas) (CondFunctions, error) {
	if expr == nil {
		return nil, nil
	}

	var err error

	andExprs := SplitWithOrOperation(expr)
	var funcs CondFunctions
	for i := range andExprs {
		var f IdxFunctions
		f, err = generateIdxFunctions(andExprs[i], f, schema)
		if err != nil {
			return nil, err
		}
		funcs = append(funcs, f)
	}

	return funcs, nil
}

func generateIdxFunctions(expr influxql.Expr, funcs IdxFunctions, schema *record.Schemas) (IdxFunctions, error) {
	var switchOp bool
	if expr == nil {
		return funcs, nil
	}

	var err error
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		if expr.Op == influxql.AND {
			funcs, err = generateIdxFunctions(expr.LHS, funcs, schema)
			if err != nil {
				return nil, err
			}

			funcs, err = generateIdxFunctions(expr.RHS, funcs, schema)
			if err != nil {
				return nil, err
			}
		} else {
			expr, switchOp, err = formatBinaryExpr(expr)
			if err != nil {
				return nil, err
			}

			funcs, err = getIdxFunction(expr, funcs, switchOp, schema, expr.Op)
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, errors.New("complex binary expression unsupported")
	}

	return funcs, err
}

const (
	StringFunc int = iota
	FloatFunc
	IntFunc
	BoolFunc
	ColBottom
)

const (
	GT int = iota
	LT
	GTE
	LTE
	EQ
	NEQ
	BOTTOM
)

var operationMap = map[influxql.Token]int{
	influxql.GT:  GT,
	influxql.LT:  LT,
	influxql.GTE: GTE,
	influxql.LTE: LTE,
	influxql.EQ:  EQ,
	influxql.NEQ: NEQ,
}

var switchOpMap = map[int]int{
	GT:  LT,
	LT:  GT,
	GTE: LTE,
	LTE: GTE,
	EQ:  EQ,
	NEQ: NEQ,
}

var idxTypeFun [BOTTOM][ColBottom]func(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte

func initIdxTypeFun() {
	nilFunc := func(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte {
		return nil
	}
	idxTypeFun = [BOTTOM][ColBottom]func(col *record.ColVal, compare interface{}, bitMap, pos []byte, offset int) []byte{
		{GetStringGTConditionBitMap, GetFloatGTConditionBitMap, GetIntegerGTConditionBitMap, nilFunc},
		{GetStringLTConditionBitMap, GetFloatLTConditionBitMap, GetIntegerLTConditionBitMap, nilFunc},
		{GetStringGTEConditionBitMap, GetFloatGTEConditionBitMap, GetIntegerGTEConditionBitMap, nilFunc},
		{GetStringLTEConditionBitMap, GetFloatLTEConditionBitMap, GetIntegerLTEConditionBitMap, nilFunc},
		{GetStringEQConditionBitMap, GetFloatEQConditionBitMap, GetIntegerEQConditionBitMap, GetBooleanEQConditionBitMap},
		{GetStringNEQConditionBitMap, GetFloatNEQConditionBitMap, GetIntegerNEQConditionBitMap, GetBooleanNEQConditionBitMap}}
}

func getIdxFunction(expr *influxql.BinaryExpr, funcs IdxFunctions, switchOp bool, schema *record.Schemas, op influxql.Token) (IdxFunctions, error) {
	leftExpr, ok := expr.LHS.(*influxql.VarRef)
	if !ok {
		return nil, errors.New("error type switch")
	}

	opType := operationMap[op]
	switchIdx := switchOpMap[opType]
	f := IdxFunction{
		Idx: schema.FieldIndex(leftExpr.Val),
	}

	switch e := expr.RHS.(type) {
	case *influxql.StringLiteral:
		f.Compare = e.Val
		if !switchOp {
			f.Function = idxTypeFun[opType][StringFunc]
		} else {
			f.Function = idxTypeFun[switchIdx][StringFunc]
		}

		funcs = append(funcs, f)
	case *influxql.IntegerLiteral:
		f.Compare = e.Val
		if !switchOp {
			f.Function = idxTypeFun[opType][IntFunc]
		} else {
			f.Function = idxTypeFun[switchIdx][IntFunc]
		}

		funcs = append(funcs, f)
	case *influxql.NumberLiteral:
		f.Compare = e.Val
		if !switchOp {
			f.Function = idxTypeFun[opType][FloatFunc]
		} else {
			f.Function = idxTypeFun[switchIdx][FloatFunc]
		}

		funcs = append(funcs, f)
	case *influxql.BooleanLiteral:
		f.Function = idxTypeFun[opType][BoolFunc]
		f.Compare = e.Val
		funcs = append(funcs, f)
	default:
		return nil, errors.New("complex binary expression unsupported")
	}
	return funcs, nil
}

func formatBinaryExpr(expr influxql.Expr) (*influxql.BinaryExpr, bool, error) {
	switchOp := false
	binaryExpr, ok := expr.(*influxql.BinaryExpr)
	if !ok {
		return nil, false, errors.New("complex binary expression unsupported")
	}

	_, ok = binaryExpr.LHS.(*influxql.VarRef)
	if !ok {
		if _, ok = binaryExpr.RHS.(*influxql.VarRef); !ok {
			return nil, false, errors.New("complex binary expression unsupported")
		} else {
			binaryExpr.LHS, binaryExpr.RHS = binaryExpr.RHS, binaryExpr.LHS
			switchOp = true
		}
	}

	if !formatRHS(binaryExpr) {
		return nil, false, errors.New("binary condition not valid")
	}

	return binaryExpr, switchOp, nil
}

func formatRHS(binaryExpr *influxql.BinaryExpr) bool {
	if binaryExpr.LHS.(*influxql.VarRef).Type == influxql.Float {
		if _, ok := binaryExpr.RHS.(*influxql.NumberLiteral); !ok {
			rhs, ok := binaryExpr.RHS.(*influxql.IntegerLiteral)
			// integer is valid for LHS with float
			if ok {
				binaryExpr.RHS = &influxql.NumberLiteral{Val: float64(rhs.Val)}
			} else {
				return false
			}
		}
	}
	return true
}

func RewriteTimeCompareVal(expr influxql.Expr, valuer *influxql.NowValuer) {
	if op, ok := expr.(*influxql.BinaryExpr); ok {
		if op.Op != influxql.AND && op.Op != influxql.OR {
			if leftVal, lok := op.LHS.(*influxql.VarRef); lok {
				if leftVal.Val == record.TimeField {
					op.RHS = influxql.Reduce(op.RHS, valuer)
					switch timeCol := op.RHS.(type) {
					case *influxql.StringLiteral:
						t, _ := timeCol.ToTimeLiteral(valuer.Location)
						var val int64
						if t.Val.IsZero() {
							val = influxql.MinTime
						} else {
							val = t.Val.UnixNano()
						}
						op.RHS = &influxql.IntegerLiteral{Val: val}
					case *influxql.TimeLiteral:
						op.RHS = &influxql.IntegerLiteral{Val: timeCol.Val.UnixNano()}
					case *influxql.DurationLiteral:
						op.RHS = &influxql.IntegerLiteral{Val: timeCol.Val.Nanoseconds()}
					case *influxql.NumberLiteral:
						op.RHS = &influxql.IntegerLiteral{Val: int64(timeCol.Val)}
					case *influxql.IntegerLiteral:
					default:
						panic("unsupported data type for time filter")
					}
				}
			}
			return
		}
		RewriteTimeCompareVal(op.LHS, valuer)
		RewriteTimeCompareVal(op.RHS, valuer)
	}
}

func GetTimeCondition(tr util.TimeRange, schema record.Schemas, tcIdx int) influxql.Expr {
	if tcIdx < 0 || tcIdx >= len(schema) {
		return nil
	}
	if tr.Min == tr.Max {
		return &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: schema[tcIdx].Name, Type: influxql.Integer},
			RHS: &influxql.IntegerLiteral{Val: tr.Min},
		}
	}
	if tr.Min != influxql.MinTime && tr.Max != influxql.MaxTime {
		return &influxql.BinaryExpr{
			Op: influxql.AND,
			LHS: &influxql.BinaryExpr{
				Op:  influxql.GTE,
				LHS: &influxql.VarRef{Val: schema[tcIdx].Name, Type: influxql.Integer},
				RHS: &influxql.IntegerLiteral{Val: tr.Min},
			},
			RHS: &influxql.BinaryExpr{
				Op:  influxql.LTE,
				LHS: &influxql.VarRef{Val: schema[tcIdx].Name, Type: influxql.Integer},
				RHS: &influxql.IntegerLiteral{Val: tr.Max},
			},
		}
	}
	if tr.Min != influxql.MinTime {
		return &influxql.BinaryExpr{
			Op:  influxql.GTE,
			LHS: &influxql.VarRef{Val: schema[tcIdx].Name, Type: influxql.Integer},
			RHS: &influxql.IntegerLiteral{Val: tr.Min},
		}
	}
	if tr.Max != influxql.MaxTime {
		return &influxql.BinaryExpr{
			Op:  influxql.LTE,
			LHS: &influxql.VarRef{Val: schema[tcIdx].Name, Type: influxql.Integer},
			RHS: &influxql.IntegerLiteral{Val: tr.Max},
		}
	}
	return nil
}

func CombineConditionWithAnd(lhs, rhs influxql.Expr) influxql.Expr {
	if lhs == nil {
		return rhs
	}
	if rhs == nil {
		return lhs
	}
	return &influxql.BinaryExpr{Op: influxql.AND, LHS: lhs, RHS: rhs}
}

type RPNElement struct {
	op rpn.Op
	rg IdxFunction
}

type ConditionImpl struct {
	isSimpleExpr bool
	numFilter    int
	schema       record.Schemas
	rpn          []*RPNElement
	rpnStack     []*RPNElement
}

func NewCondition(timeCondition, condition influxql.Expr, schema record.Schemas) (*ConditionImpl, error) {
	var err error
	c := &ConditionImpl{schema: schema, isSimpleExpr: true}
	// use "AND" to connect the time condition to other conditions.
	combineCondition := CombineConditionWithAnd(timeCondition, condition)
	rpnExpr := rpn.ConvertToRPNExpr(combineCondition)
	if err = c.convertToRPNElem(rpnExpr); err != nil {
		return nil, err
	}
	if c.numFilter, err = c.getNumFilter(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *ConditionImpl) convertToRPNElem(rpnExpr *rpn.RPNExpr) error {
	for i, expr := range rpnExpr.Val {
		switch v := expr.(type) {
		case influxql.Token:
			switch v {
			case influxql.AND:
				c.rpn = append(c.rpn, &RPNElement{op: rpn.AND})
			case influxql.OR:
				c.isSimpleExpr = false
				c.rpn = append(c.rpn, &RPNElement{op: rpn.OR})
			case influxql.EQ, influxql.LT, influxql.LTE, influxql.GT, influxql.GTE, influxql.NEQ:
			default:
				return errno.NewError(errno.ErrRPNOp, v)
			}
		case *influxql.VarRef:
			idx := c.schema.FieldIndex(v.Val)
			if idx < 0 {
				return errno.NewError(errno.ErrRPNElemSchema)
			}
			if i+2 >= len(rpnExpr.Val) {
				return errno.NewError(errno.ErrRPNElemNum)
			}
			value := rpnExpr.Val[i+1]
			op, ok := rpnExpr.Val[i+2].(influxql.Token)
			if !ok {
				return errno.NewError(errno.ErrRPNElemOp)
			}
			if err := c.genRPNElementByVal(value, op, idx); err != nil {
				return err
			}
		case *influxql.StringLiteral, *influxql.NumberLiteral, *influxql.IntegerLiteral, *influxql.BooleanLiteral:
		default:
			return errno.NewError(errno.ErrRPNExpr, v)
		}
	}
	return nil
}

func (c *ConditionImpl) genRPNElementByVal(value interface{}, op influxql.Token, idx int) error {
	elem := &RPNElement{op: rpn.InRange, rg: IdxFunction{Idx: idx, Op: op}}
	switch val := value.(type) {
	case *influxql.StringLiteral:
		elem.rg.Compare = val.Val
		elem.rg.Function = idxTypeFun[operationMap[op]][StringFunc]
	case *influxql.IntegerLiteral:
		elem.rg.Compare = val.Val
		elem.rg.Function = idxTypeFun[operationMap[op]][IntFunc]
	case *influxql.NumberLiteral:
		elem.rg.Compare = val.Val
		elem.rg.Function = idxTypeFun[operationMap[op]][FloatFunc]
	case *influxql.BooleanLiteral:
		elem.rg.Compare = val.Val
		elem.rg.Function = idxTypeFun[operationMap[op]][BoolFunc]
	default:
		return errno.NewError(errno.ErrRPNElement, value)
	}
	c.rpn = append(c.rpn, elem)
	return nil
}

func (c *ConditionImpl) HaveFilter() bool {
	return len(c.rpn) > 0
}

func (c *ConditionImpl) NumFilter() int {
	return c.numFilter
}

func (c *ConditionImpl) getNumFilter() (int, error) {
	if !c.HaveFilter() {
		return 0, nil
	}
	if c.isSimpleExpr {
		return 1, nil
	}

	var count int
	for _, elem := range c.rpn {
		switch elem.op {
		case rpn.InRange, rpn.NotInRange:
			c.rpnStack = append(c.rpnStack, elem)
		case rpn.AND:
			if len(c.rpnStack) == 1 {
				c.rpnStack = c.rpnStack[:len(c.rpnStack)-1]
			} else if len(c.rpnStack) >= 2 {
				count++
				c.rpnStack = c.rpnStack[:len(c.rpnStack)-2]
			}
		case rpn.OR:
			if len(c.rpnStack) == 1 {
				count++
				c.rpnStack = c.rpnStack[:len(c.rpnStack)-1]
			} else if len(c.rpnStack) >= 2 {
				count += 2
				c.rpnStack = c.rpnStack[:len(c.rpnStack)-2]
			}
		default:
			return 0, errno.NewError(errno.ErrRPNOp, elem.op)
		}
	}
	c.rpnStack = c.rpnStack[:0]
	return count, nil
}

func (c *ConditionImpl) Filter(rec *record.Record, filterBitmap *bitmap.FilterBitmap) error {
	if c.isSimpleExpr {
		if err := c.filterSimplexExpr(rec, filterBitmap); err != nil {
			return err
		}
	} else {
		if err := c.filterCompoundExpr(rec, filterBitmap); err != nil {
			return err
		}
	}
	rowNum, offset := rec.RowNums(), rec.TimeColumn().BitMapOffset
	b := filterBitmap.Bitmap[0].Val
	for i := 0; i < rowNum; i++ {
		if !bitmap.IsNil(b, i+offset) {
			filterBitmap.ReserveId = append(filterBitmap.ReserveId, i)
		}
	}
	return nil
}

func (c *ConditionImpl) filterSimplexExpr(rec *record.Record, filterBitmap *bitmap.FilterBitmap) error {
	for _, elem := range c.rpn {
		switch elem.op {
		case rpn.InRange, rpn.NotInRange:
			col := rec.ColVals[elem.rg.Idx]
			if len(filterBitmap.Bitmap[0].Val) == 0 {
				filterBitmap.Bitmap[0].Val = append(filterBitmap.Bitmap[0].Val, col.Bitmap...)
			}
			filterBitmap.Bitmap[0].Val = elem.rg.Function(&col, elem.rg.Compare, col.Bitmap, filterBitmap.Bitmap[0].Val, col.BitMapOffset)
		case rpn.AND, rpn.OR:
		default:
			return errno.NewError(errno.ErrRPNOp, elem.op)
		}
	}
	return nil
}

func (c *ConditionImpl) filterCompoundExpr(rec *record.Record, filterBitmap *bitmap.FilterBitmap) error {
	// idx indicates the bitmap position in use.
	var idx int
	var err error
	for _, elem := range c.rpn {
		switch elem.op {
		case rpn.InRange:
			c.rpnStack = append(c.rpnStack, elem)
		case rpn.AND:
			idx, err = c.filterForAnd(rec, filterBitmap, idx)
			if err != nil {
				return err
			}
		case rpn.OR:
			idx, err = c.filterForOr(rec, filterBitmap, idx)
			if err != nil {
				return err
			}
		default:
			return errno.NewError(errno.ErrUnknownOpInCondition)
		}
	}
	if idx != 0 {
		return errno.NewError(errno.ErrInvalidStackInCondition)
	}
	return nil
}

func (c *ConditionImpl) filterForOr(rec *record.Record, filterBitmap *bitmap.FilterBitmap, idx int) (int, error) {
	switch len(c.rpnStack) {
	case 0:
		if idx < 1 {
			return 0, errno.NewError(errno.ErrRPNIsNullForOR)
		}
		filterBitmap.Bitmap[idx-1].Or(filterBitmap.Bitmap[idx])
		filterBitmap.Bitmap[idx].Val = filterBitmap.Bitmap[idx].Val[:0]
		idx--
	case 1:
		if idx < 0 {
			return 0, errno.NewError(errno.ErrRPNIsNullForOR)
		}
		e1 := c.rpnStack[len(c.rpnStack)-1]
		col := rec.ColVals[e1.rg.Idx]
		// Apply for a new bitmap.
		idx++
		b := filterBitmap.Bitmap[idx]
		b.Val = append(b.Val, col.Bitmap...)
		// Save the compare result in the new bitmap.
		b.Val = e1.rg.Function(&col, e1.rg.Compare, col.Bitmap, b.Val, col.BitMapOffset)
		filterBitmap.Bitmap[idx-1].Or(filterBitmap.Bitmap[idx])
		// Clear intermediate results
		c.rpnStack = c.rpnStack[:len(c.rpnStack)-1]
		filterBitmap.Bitmap[idx].Val = filterBitmap.Bitmap[idx].Val[:0]
		idx--
	default:
		e1, e2 := c.rpnStack[len(c.rpnStack)-1], c.rpnStack[len(c.rpnStack)-2]
		col := rec.ColVals[e1.rg.Idx]
		// Apply for a new bitmap.
		if len(filterBitmap.Bitmap[idx].Val) > 0 {
			idx++
		}
		b1 := filterBitmap.Bitmap[idx]
		b1.Val = append(b1.Val, col.Bitmap...)
		// Save the compare result in the new bitmap.
		b1.Val = e1.rg.Function(&col, e1.rg.Compare, col.Bitmap, b1.Val, col.BitMapOffset)
		// Apply for a new bitmap.
		idx++
		b2 := filterBitmap.Bitmap[idx]
		b2.Val = append(b2.Val, col.Bitmap...)
		// Save the compare result in the new bitmap.
		col = rec.ColVals[e2.rg.Idx]
		b2.Val = e2.rg.Function(&col, e2.rg.Compare, col.Bitmap, b2.Val, col.BitMapOffset)
		b1.Or(b2)
		// Clear intermediate results
		c.rpnStack = c.rpnStack[:len(c.rpnStack)-2]
		filterBitmap.Bitmap[idx].Val = filterBitmap.Bitmap[idx].Val[:0]
		idx--
	}
	return idx, nil
}

func (c *ConditionImpl) filterForAnd(rec *record.Record, filterBitmap *bitmap.FilterBitmap, idx int) (int, error) {
	switch len(c.rpnStack) {
	case 0:
		if idx < 1 {
			return 0, errno.NewError(errno.ErrRPNIsNullForAnd)
		}
		filterBitmap.Bitmap[idx-1].And(filterBitmap.Bitmap[idx])
		filterBitmap.Bitmap[idx].Val = filterBitmap.Bitmap[idx].Val[:0]
		idx--
	case 1:
		if idx < 0 {
			return 0, errno.NewError(errno.ErrRPNIsNullForAnd)
		}
		e1 := c.rpnStack[len(c.rpnStack)-1]
		col := rec.ColVals[e1.rg.Idx]
		b := filterBitmap.Bitmap[idx]
		b.Val = e1.rg.Function(&col, e1.rg.Compare, col.Bitmap, b.Val, col.BitMapOffset)
		c.rpnStack = c.rpnStack[:len(c.rpnStack)-1]
	default:
		e1, e2 := c.rpnStack[len(c.rpnStack)-1], c.rpnStack[len(c.rpnStack)-2]
		col := rec.ColVals[e1.rg.Idx]
		// Apply for a new bitmap.
		if len(filterBitmap.Bitmap[idx].Val) > 0 {
			idx++
		}
		b := filterBitmap.Bitmap[idx]
		b.Val = append(b.Val, col.Bitmap...)
		// Save the AND result in the new bitmap.
		b.Val = e1.rg.Function(&col, e1.rg.Compare, col.Bitmap, b.Val, col.BitMapOffset)
		col = rec.ColVals[e2.rg.Idx]
		b.Val = e2.rg.Function(&col, e2.rg.Compare, col.Bitmap, b.Val, col.BitMapOffset)
		c.rpnStack = c.rpnStack[:len(c.rpnStack)-2]
	}
	return idx, nil
}
