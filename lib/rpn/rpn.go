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

package rpn

import "github.com/openGemini/openGemini/open_src/influx/influxql"

type Op uint8

// Atoms of a Boolean expression.
const (
	InRange Op = iota
	NotInRange
	InSet
	NotInSet
	NOT // operators of the logical expression.
	AND
	OR
	UNKNOWN // unsupported type value.
)

type RPNExpr struct {
	Val []interface{}
}

var switchMap = map[influxql.Token]influxql.Token{
	influxql.GT:  influxql.LT,
	influxql.LT:  influxql.GT,
	influxql.GTE: influxql.LTE,
	influxql.LTE: influxql.GTE,
	influxql.EQ:  influxql.EQ,
	influxql.NEQ: influxql.NEQ,
}

func ConvertToRPNExpr(expr influxql.Expr) *RPNExpr {
	rpnExpr := &RPNExpr{}
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		if _, ok := expr.RHS.(*influxql.VarRef); ok {
			expr.Op = switchMap[expr.Op]
			expr.RHS, expr.LHS = expr.LHS, expr.RHS
		}
		leftExpr := ConvertToRPNExpr(expr.LHS)
		rightExpr := ConvertToRPNExpr(expr.RHS)
		rpnExpr.Val = append(rpnExpr.Val, leftExpr.Val...)
		rpnExpr.Val = append(rpnExpr.Val, rightExpr.Val...)
		rpnExpr.Val = append(rpnExpr.Val, expr.Op)
	case *influxql.ParenExpr:
		innerExpr := ConvertToRPNExpr(expr.Expr)
		rpnExpr.Val = append(rpnExpr.Val, innerExpr.Val...)
	case *influxql.VarRef:
		rpnExpr.Val = append(rpnExpr.Val, expr)
	case *influxql.StringLiteral, *influxql.IntegerLiteral, *influxql.NumberLiteral, *influxql.BooleanLiteral:
		rpnExpr.Val = append(rpnExpr.Val, expr)
	default:
	}
	return rpnExpr
}
