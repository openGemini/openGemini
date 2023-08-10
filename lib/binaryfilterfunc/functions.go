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

	"github.com/openGemini/openGemini/lib/record"
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

	return binaryExpr, switchOp, nil
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
