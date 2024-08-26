// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package query

import (
	"fmt"
	"regexp"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/common/model"
)

func init() {
	RegistryLabelFunction("label_replace", &labelReplaceFunc{
		BaseInfo: BaseInfo{FuncType: LABEL},
	})
	RegistryLabelFunction("label_join", &labelJoinFunc{
		BaseInfo: BaseInfo{FuncType: LABEL},
	})
}

func GetLabelFunction(name string) LabelFunc {
	label, ok := GetFunctionFactoryInstance().label[name]
	if ok && label.GetFuncType() == LABEL {
		return label
	}
	return nil
}

type labelReplaceFunc struct {
	BaseInfo
}

func (s *labelReplaceFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	var got int
	if got = len(expr.Args); got != 5 {
		return fmt.Errorf("invalid number of arguments for %s, expected 5, got %d", expr.Name, got)
	}
	for i := 1; i < got; i++ {
		if _, ok := expr.Args[i].(*influxql.StringLiteral); !ok {
			return fmt.Errorf("expected string argument in label_replace()")
		}
	}

	if regexStr, ok := expr.Args[4].(*influxql.StringLiteral); ok {
		if _, err := regexp.Compile("^(?:" + regexStr.Val + ")$"); err != nil {
			return fmt.Errorf("invalid regular expression in label_replace(): %s", regexStr.Val)
		}

	}

	if dst, ok := expr.Args[1].(*influxql.StringLiteral); ok {
		if !model.LabelNameRE.MatchString(dst.Val) {
			return fmt.Errorf("invalid destination label name in label_replace(): %s", dst.Val)
		}
	}

	return compileAllStringArgs(expr, c)
}

func (s *labelReplaceFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type labelJoinFunc struct {
	BaseInfo
}

func (s *labelJoinFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	var got int
	if got = len(expr.Args); got < 3 {
		return fmt.Errorf("invalid number of arguments for %s, at least 3, got %d", expr.Name, got)
	}
	if ok, argVal := checkLabelName(expr.Args[1]); ok {
		return fmt.Errorf("invalid source label name in label_join(): %s", argVal)
	}
	for i := 3; i < len(expr.Args); i++ {
		if ok, argVal := checkLabelName(expr.Args[i]); ok {
			return fmt.Errorf("invalid source label name in label_join(): %s", argVal)
		}
	}
	return nil
}

func (s *labelJoinFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

func checkLabelName(expr influxql.Expr) (bool, string) {
	arg, ok := expr.(*influxql.StringLiteral)
	return ok && !model.LabelName(arg.Val).IsValid(), arg.Val
}

// type mapper
type LabelFunctionTypeMapper struct{}

func (m LabelFunctionTypeMapper) MapType(_ *influxql.Measurement, _ string) influxql.DataType {
	return influxql.Unknown
}

func (m LabelFunctionTypeMapper) MapTypeBatch(_ *influxql.Measurement, _ map[string]*influxql.FieldNameSpace, _ *influxql.Schema) error {
	return nil
}

func (m LabelFunctionTypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	if labelFunc := GetLabelFunction(name); labelFunc != nil {
		return labelFunc.CallTypeFunc(name, args)
	}
	return influxql.Unknown, nil
}
