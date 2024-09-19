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

package influxql

import (
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type FilterMapValue struct {
	DataType     int
	FloatValue   float64
	IntegerValue int64
	BooleanValue bool
	StringValue  string
	IsNil        bool
}

// FilterMapValuer is a valuer that substitutes values for the mapped interface.
type FilterMapValuer map[string]*FilterMapValue

func (m FilterMapValuer) SetFilterMapValue(name string, value interface{}) {
	if value == nil {
		m[name] = nil
		return
	}
	res, ok := m[name]
	if !ok || res == nil {
		res = &FilterMapValue{}
		m[name] = res
	}
	switch v := value.(type) {
	case *string:
		res.IsNil = true
		res.DataType = influx.Field_Type_String
		return
	case *int64:
		res.IsNil = true
		res.DataType = influx.Field_Type_Int
		return
	case *float64:
		res.IsNil = true
		res.DataType = influx.Field_Type_Float
		return
	case *bool:
		res.IsNil = true
		res.DataType = influx.Field_Type_Boolean
		return
	case string:
		res.IsNil = false
		res.DataType = influx.Field_Type_String
		res.StringValue = v
		return
	case int64:
		res.IsNil = false
		res.DataType = influx.Field_Type_Int
		res.IntegerValue = v
		return
	case float64:
		res.IsNil = false
		res.DataType = influx.Field_Type_Float
		res.FloatValue = v
		return
	case bool:
		res.IsNil = false
		res.DataType = influx.Field_Type_Boolean
		res.BooleanValue = v
		return
	}
}

// Value returns the value for a key in the FilterMapValuer.
func (m FilterMapValuer) Value(key string) (interface{}, bool) {
	v, ok := m[key]
	if v == nil {
		return nil, ok
	}
	switch v.DataType {
	case influx.Field_Type_Int:
		if v.IsNil {
			return (*int64)(nil), ok
		}
		return v.IntegerValue, ok
	case influx.Field_Type_Float:
		if v.IsNil {
			return (*float64)(nil), ok
		}
		return v.FloatValue, ok
	case influx.Field_Type_String:
		if v.IsNil {
			return (*string)(nil), ok
		}
		return v.StringValue, ok
	case influx.Field_Type_Boolean:
		if v.IsNil {
			return (*bool)(nil), ok
		}
		return v.BooleanValue, ok
	}
	return nil, ok
}

func (m FilterMapValuer) SetValuer(v Valuer, index int) {

}

func (m FilterMapValuer) FilterMapEvalBool(valuer ValuerEval, expr Expr) bool {
	ok, res := m.EvalBoolNumeric(expr)
	if ok {
		return res
	}
	return valuer.EvalBool(expr)
}

func (m FilterMapValuer) EvalBoolNumeric(expr Expr) (bool, bool) {
	switch expr := expr.(type) {
	case *BinaryExpr:
		exprL := expr.LHS
		exprR := expr.RHS
		switch exprL := exprL.(type) {
		case *VarRef:
			switch exprR := exprR.(type) {
			case *IntegerLiteral, *NumberLiteral:
				if value, ok := m[exprL.Val]; ok && !value.IsNil && (value.DataType == influx.Field_Type_Int || value.DataType == influx.Field_Type_Float) {
					lhs := value.FloatValue
					if value.DataType == influx.Field_Type_Int {
						lhs = float64(value.IntegerValue)
					}
					var rhs float64
					switch exprR := exprR.(type) {
					case *IntegerLiteral:
						rhs = float64(exprR.Val)
					case *NumberLiteral:
						rhs = float64(exprR.Val)
					default:
						return false, false
					}
					switch expr.Op {
					case EQ:
						return true, lhs == rhs
					case NEQ:
						return true, lhs != rhs
					case LT:
						return true, lhs < rhs
					case LTE:
						return true, lhs <= rhs
					case GT:
						return true, lhs > rhs
					case GTE:
						return true, lhs >= rhs
					default:
						return false, false
					}
				}
			default:
				return false, false
			}
		default:
			return false, false
		}
	default:
		return false, false
	}
	return false, false
}
