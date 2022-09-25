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

package op

import (
	"fmt"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

type SumOp struct {
	BaseOp
	factory RoutineFactory
}

func NewSumOp(factory RoutineFactory) *SumOp {
	op := &SumOp{
		factory: factory,
	}
	op.init(op, "sum", SUM_OP, 1)
	return op
}

func (op *SumOp) Clone() Op {
	clone := &SumOp{}
	clone.init(clone, op.name, op.id, op.arity)
	return clone
}

func (op *SumOp) Factory() RoutineFactory {
	return op.factory
}

func (op *SumOp) Type(args ...influxql.DataType) (influxql.DataType, error) {
	if op.arity != len(args) {
		return influxql.Unknown, fmt.Errorf("invalid arity of %s operator, expected %d, got %d", op.name, op.arity, len(args))
	}

	if args[0] == influxql.String || args[0] == influxql.Boolean {
		return influxql.Unknown, fmt.Errorf("unsupported type %v or %v of %s operator", influxql.String, influxql.Boolean, op.name)
	}

	return args[0], nil
}

func (op *SumOp) Compile(call *influxql.Call) error {
	nargs := len(call.Args)
	if nargs != op.arity {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", op.name, op.arity, nargs)
	}
	return nil
}

type CountOp struct {
	BaseOp
	factory RoutineFactory
}

func NewCountOp(factory RoutineFactory) *CountOp {
	op := &CountOp{
		factory: factory,
	}
	op.init(op, "count", COUNT_OP, 1)
	return op
}

func (op *CountOp) Clone() Op {
	clone := &CountOp{}
	clone.init(clone, op.name, op.id, op.arity)
	return clone
}

func (op *CountOp) Factory() RoutineFactory {
	return op.factory
}

func (op *CountOp) Type(args ...influxql.DataType) (influxql.DataType, error) {
	if op.arity != len(args) {
		return influxql.Unknown, fmt.Errorf("invalid arity of %s operator, expected %d, got %d", op.name, op.arity, len(args))
	}

	return influxql.Integer, nil
}

func (op *CountOp) Compile(call *influxql.Call) error {
	nargs := len(call.Args)
	if nargs != op.arity {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", op.name, op.arity, nargs)
	}
	return nil
}
