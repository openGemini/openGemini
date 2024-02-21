package query

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

import (
	"fmt"
	"strings"

	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

var (
	_ = RegistryMaterializeFunction("str", &strFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("strlen", &strLenFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
	_ = RegistryMaterializeFunction("substr", &subStrFunc{
		BaseInfo: BaseInfo{FuncType: STRING},
	})
)

func GetStringFunction(name string) MaterializeFunc {
	materialize, ok := GetFunctionFactoryInstance().FindMaterFunc(name)
	if ok && materialize.GetFuncType() == STRING {
		return materialize
	}
	return nil
}

func compileAllStringArgs(expr *influxql.Call, c *compiledField) error {
	// Compile all the argument expressions that are not just literals.
	for _, arg := range expr.Args {
		if _, ok := arg.(influxql.Literal); ok {
			continue
		}
		if err := c.compileExpr(arg); err != nil {
			return err
		}
	}
	return nil
}

type strFunc struct {
	BaseInfo
}

func (s *strFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if got := len(expr.Args); got != 2 {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, 2, got)
	}
	if _, ok := expr.Args[1].(*influxql.StringLiteral); !ok {
		return fmt.Errorf("expected string argument in str()")
	}
	return compileAllStringArgs(expr, c)
}

func (s *strFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0, arg1 influxql.DataType
	if len(args) != 2 {
		return influxql.Unknown, fmt.Errorf("invalid argument number in %s(): %d", name, len(args))
	}
	arg0, arg1 = args[0], args[1]

	switch arg0 {
	case influxql.String:
		// Pass through to verify the second argument.
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}

	switch arg1 {
	case influxql.String:
		return influxql.Boolean, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, arg0)
	}
}

func (s *strFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) != 2 {
		return nil, false
	}
	arg0, ok := args[0].(string)
	if !ok {
		return nil, true
	}
	arg1, ok := args[1].(string)
	if !ok {
		return nil, true
	}
	return strings.Contains(arg0, arg1), true
}

type strLenFunc struct {
	BaseInfo
}

func (s *strLenFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileAllStringArgs(expr, c)
}

func (s *strLenFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0 influxql.DataType
	if len(args) != 1 {
		return influxql.Unknown, fmt.Errorf("invalid argument number in %s(): %d", name, len(args))
	}
	arg0 = args[0]
	switch arg0 {
	case influxql.String:
		return influxql.Integer, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}
}

func (s *strLenFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) != 1 {
		return nil, false
	}
	if arg0, ok := args[0].(string); ok {
		return int64(len(arg0)), true
	}
	return nil, true
}

// subStr
type subStrFunc struct {
	BaseInfo
}

func (s *subStrFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	const NARGS = 1
	// Did we get the expected number of args?
	if got := len(expr.Args); expr.Name == "substr" && (len(expr.Args) < 2 || len(expr.Args) > 3) {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, NARGS, got)
	}

	if got := len(expr.Args); expr.Name != "substr" && got != 1 {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, NARGS, got)
	}

	if second, ok := expr.Args[1].(*influxql.IntegerLiteral); !ok || second.Val < 0 {
		return fmt.Errorf("expected non-gegative integer argument in substr()")
	}
	if len(expr.Args) == 3 {
		if third, ok := expr.Args[2].(*influxql.IntegerLiteral); !ok || third.Val < 0 {
			return fmt.Errorf("expected non-gegative integer argument in substr()")
		}
	}
	return compileAllStringArgs(expr, c)
}

func (s *subStrFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0, arg1, arg2 influxql.DataType
	if len(args) < 2 || len(args) > 3 {
		return influxql.Unknown, fmt.Errorf("invalid argument number in %s(): %d", name, len(args))
	}
	arg0, arg1 = args[0], args[1]
	if len(args) == 3 {
		arg2 = args[2]
	}

	switch arg0 {
	case influxql.String:
		// Pass through to verify the second argument.
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}

	switch arg1 {
	case influxql.Integer:
		if len(args) == 2 {
			return influxql.String, nil
		}
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, arg0)
	}

	switch arg2 {
	case influxql.Integer:
		return influxql.String, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the third argument in %s(): %s", name, arg0)
	}
}

func (s *subStrFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	var (
		arg0 string
		arg1 int64
		ok   bool
	)
	arg0, ok = args[0].(string)
	if !ok {
		return nil, true
	}
	arg1, ok = args[1].(int64)
	if !ok {
		return nil, true
	}
	if len(args) == 2 {
		return SubStrTwoParaFunc(arg0, arg1), true
	}
	if len(args) == 3 {
		arg2, ok := args[2].(int64)
		if !ok {
			return nil, true
		}
		return SubStrThreeParaFunc(arg0, arg1, arg2), true
	}
	return nil, false
}

func SubStrTwoParaFunc(srcStr string, start int64) string {
	if start > int64(len(srcStr)) {
		return ""
	}
	oriStr := srcStr[start:]
	newStr := make([]byte, len(oriStr))
	copy(newStr, oriStr)
	return util.Bytes2str(newStr)
}

func SubStrThreeParaFunc(srcStr string, start, subStrLen int64) string {
	if start >= int64(len(srcStr)) {
		return ""
	}
	var oriStr string
	if start+subStrLen >= int64(len(srcStr)) {
		oriStr = srcStr[start:]
	} else {
		oriStr = srcStr[start : start+subStrLen]
	}
	newStr := make([]byte, len(oriStr))
	copy(newStr, oriStr)
	return util.Bytes2str(newStr)
}

// type mapper
type StringFunctionTypeMapper struct{}

func (m StringFunctionTypeMapper) MapType(_ *influxql.Measurement, _ string) influxql.DataType {
	return influxql.Unknown
}

func (m StringFunctionTypeMapper) MapTypeBatch(_ *influxql.Measurement, _ map[string]*influxql.FieldNameSpace, _ *influxql.Schema) error {
	return nil
}

func (m StringFunctionTypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	if stringFunc := GetStringFunction(name); stringFunc != nil {
		return stringFunc.CallTypeFunc(name, args)
	}
	return influxql.Unknown, nil
}

// valuer
type StringValuer struct{}

var _ influxql.CallValuer = StringValuer{}

func (StringValuer) Value(_ string) (interface{}, bool) {
	return nil, false
}

func (StringValuer) SetValuer(_ influxql.Valuer, _ int) {
}

func (v StringValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if stringFunc := GetStringFunction(name); stringFunc != nil {
		return stringFunc.CallFunc(name, args)
	}
	return nil, false
}
