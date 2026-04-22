// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package op

import (
	"fmt"
	"strings"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type ToLowerOp struct {
	BaseOp
}

func NewToLowerOp() *ToLowerOp {
	op := &ToLowerOp{}
	op.init(op, "tolower", TO_LOWER_OP, 1)
	return op
}

func (op *ToLowerOp) Clone() Op {
	clone := &ToLowerOp{}
	clone.init(clone, op.name, op.id, op.arity)
	return clone
}

func (op *ToLowerOp) Eval(args ...interface{}) (interface{}, error) {
	if arg, ok := args[0].(string); ok {
		return strings.ToLower(arg), nil
	}
	return nil, fmt.Errorf("invalid args(%v) for %s operator", args, op.name)
}

func (op *ToLowerOp) Type(args ...influxql.DataType) (influxql.DataType, error) {
	if op.arity != len(args) {
		return influxql.Unknown, fmt.Errorf("invalid arity of %s operator, expected %d, got %d", op.name, op.arity, len(args))
	}

	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("only type %v of %s operator", influxql.String, op.name)
	}

	return influxql.String, nil
}

func (op *ToLowerOp) Compile(call *influxql.Call) error {
	nargs := len(call.Args)
	if nargs != op.arity {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", op.name, op.arity, nargs)
	}
	return nil
}

type ToUpperOp struct {
	BaseOp
}

func NewToUpperOp() *ToUpperOp {
	op := &ToUpperOp{}
	op.init(op, "toupper", TO_UPPER_OP, 1)
	return op
}

func (op *ToUpperOp) Clone() Op {
	clone := &ToUpperOp{}
	clone.init(clone, op.name, op.id, op.arity)
	return clone
}

func (op *ToUpperOp) Eval(args ...interface{}) (interface{}, error) {
	if arg, ok := args[0].(string); ok {
		return strings.ToUpper(arg), nil
	}
	return nil, fmt.Errorf("invalid args(%v) for %s operator", args, op.name)
}

func (op *ToUpperOp) Type(args ...influxql.DataType) (influxql.DataType, error) {
	if op.arity != len(args) {
		return influxql.Unknown, fmt.Errorf("invalid arity of %s operator, expected %d, got %d", op.name, op.arity, len(args))
	}

	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("only type %v of %s operator", influxql.String, op.name)
	}

	return influxql.String, nil
}

func (op *ToUpperOp) Compile(call *influxql.Call) error {
	nargs := len(call.Args)
	if nargs != op.arity {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", op.name, op.arity, nargs)
	}
	return nil
}
