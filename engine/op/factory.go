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
	"sync"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

var opFactory *OpFactory
var once sync.Once

func GetOpFactory() *OpFactory {
	once.Do(func() {
		opFactory = NewOpFactory()
	})
	return opFactory
}

type OpFactory struct {
	pmap map[string]Op
}

func NewOpFactory() *OpFactory {
	return &OpFactory{
		pmap: make(map[string]Op),
	}
}

func (c *OpFactory) AddOp(op Op) error {
	if _, ok := c.pmap[op.Name()]; ok {
		return fmt.Errorf("duplicated udf %s", op.Name())
	}
	c.pmap[op.Name()] = op
	return nil
}

func (c *OpFactory) FindOp(name string) (Op, bool) {
	if op, ok := c.pmap[name]; ok {
		return op, true
	}
	return nil, false
}

func (c *OpFactory) FindProjectOp(name string) (ProjectOp, bool) {
	if op, ok := c.FindOp(name); ok {
		if pop, ok := op.(ProjectOp); ok {
			return pop, true
		}
		return nil, false
	}
	return nil, false
}

func (c *OpFactory) FindAggregateOp(name string) (AggregateOp, bool) {
	if op, ok := c.FindOp(name); ok {
		if aop, ok := op.(AggregateOp); ok {
			return aop, true
		}
		return nil, false
	}
	return nil, false
}

func CompileOp(expr *influxql.Call) error {
	if op, ok := GetOpFactory().FindOp(expr.Name); ok {
		return op.Compile(expr)
	} else {
		return fmt.Errorf("unsupported operator %s to compile", expr.Name)
	}
}

type TypeMapper struct{}

func (m TypeMapper) MapType(_ *influxql.Measurement, _ string) influxql.DataType {
	return influxql.Unknown
}

func (m TypeMapper) MapTypeBatch(_ *influxql.Measurement, _ map[string]influxql.DataType, _ *influxql.Schema) error {
	return nil
}

func (m TypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	if op, ok := GetOpFactory().FindOp(name); ok {
		return op.Type(args...)
	} else {
		return influxql.Unknown, nil
	}
}

type Valuer struct{}

var _ influxql.CallValuer = Valuer{}

func (Valuer) Value(key string) (interface{}, bool) {
	return nil, false
}

func (Valuer) SetValuer(v influxql.Valuer, index int) {

}

func (v Valuer) Call(name string, args []interface{}) (interface{}, bool) {
	if op, ok := GetOpFactory().FindProjectOp(name); ok {
		if value, err := op.Eval(args...); err != nil {
			return nil, false
		} else {
			return value, true
		}
	} else {
		return nil, false
	}
}

func IsProjectOp(call *influxql.Call) bool {
	if _, ok := GetOpFactory().FindProjectOp(call.Name); ok {
		return true
	}
	return false
}

func IsAggregateOp(call *influxql.Call) bool {
	if _, ok := GetOpFactory().FindAggregateOp(call.Name); ok {
		return true
	}
	return false
}
