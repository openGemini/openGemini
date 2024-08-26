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

package op

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type Op interface {
	Name() string
	ID() uint64
	Arity() int
	EqualTo(other Op) bool
	Clone() Op
	Dump() string
	Type(...influxql.DataType) (influxql.DataType, error)
	Compile(call *influxql.Call) error
}

type BaseOp struct {
	derived Op
	name    string
	id      uint64
	arity   int
}

// invoke at constructor of inherited struct
func (op *BaseOp) init(derived Op, name string, id uint64, arity int) {
	op.derived = derived
	op.name = name
	op.id = id
	op.arity = arity
}

func (op *BaseOp) Name() string {
	return op.name
}

func (op *BaseOp) ID() uint64 {
	return op.id
}

func (op *BaseOp) Arity() int {
	return op.arity
}

func (op *BaseOp) EqualTo(other Op) bool {
	return op.name == other.Name() && op.id == other.ID() && op.arity == other.Arity()
}

// must be overrided at inherited struct
func (op *BaseOp) Clone() Op {
	return op.derived.Clone()
}

// must be overrided at inherited struct
func (op *BaseOp) Compile(call *influxql.Call) error {
	return op.derived.Compile(call)
}

// must be overrided at inherited struct
func (op *BaseOp) Type(args ...influxql.DataType) (influxql.DataType, error) {
	return op.derived.Type(args...)
}

func (op *BaseOp) Dump() string {
	return fmt.Sprintf("[%s](%d, %d)", op.name, op.id, op.arity)
}

type ProjectOp interface {
	Eval(...interface{}) (interface{}, error)
}

type RoutineFactory interface {
	Create(...interface{}) (interface{}, error)
}

type FuncRoutineFactory func(...interface{}) (interface{}, error)

func (f FuncRoutineFactory) Create(args ...interface{}) (interface{}, error) {
	return f(args...)
}

type AggregateOp interface {
	Factory() RoutineFactory
}
type UDAFOp interface {
	AggregateOp
	CanPushDownSeries() bool
}
