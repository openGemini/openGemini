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

package engine

import (
	"fmt"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func init() {
	RegistryAggOp("min_prom", &MinPromOp{})
	RegistryAggOp("max_prom", &MaxPromOp{})
	RegistryAggOp("count_prom", &FloatCountPromOp{})
}

type AggOperator interface {
	CreateRoutine(*AggParams) (Routine, error)
}

type AggParams struct {
	inSchema  record.Schemas
	outSchema record.Schemas
	opt       hybridqp.ExprOptions
	auxOp     []*auxProcessor
}

func NewAggParams(inSchema, outSchema record.Schemas, opt hybridqp.ExprOptions, auxOp []*auxProcessor) *AggParams {
	return &AggParams{inSchema: inSchema, outSchema: outSchema, opt: opt, auxOp: auxOp}
}

var AggFactory = make(map[string]AggOperator)

func GetAggOperator(name string) AggOperator {
	return AggFactory[name]
}

func RegistryAggOp(name string, aggOp AggOperator) {
	_, ok := AggFactory[name]
	if ok {
		return
	}
	AggFactory[name] = aggOp
}

type BasePromOp struct {
	name string
	fn   floatColFloatReduce
	fv   floatColFloatMerge
}

func NewBasePromOp(name string, fn floatColFloatReduce, fv floatColFloatMerge) *BasePromOp {
	return &BasePromOp{name: name, fn: fn, fv: fv}
}

func (c *BasePromOp) CreateRoutine(p *AggParams) (Routine, error) {
	inOrdinal := p.inSchema.FieldIndex(p.opt.Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val)
	outOrdinal := p.outSchema.FieldIndex(p.opt.Ref.Val)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for %s iterator", c.name)
	}
	return NewRoutineImpl(newFloatColFloatReducer(c.fn, c.fv, p.auxOp), inOrdinal, outOrdinal), nil
}

type MinPromOp struct {
	BasePromOp
}

func (c *MinPromOp) CreateRoutine(p *AggParams) (Routine, error) {
	c.BasePromOp = *NewBasePromOp("min_prom", MinPromReduce, MinPromMerge)
	return c.BasePromOp.CreateRoutine(p)
}

type MaxPromOp struct {
	BasePromOp
}

func (c *MaxPromOp) CreateRoutine(p *AggParams) (Routine, error) {
	c.BasePromOp = *NewBasePromOp("max_prom", MaxPromReduce, MaxPromMerge)
	return c.BasePromOp.CreateRoutine(p)
}

type FloatCountPromOp struct {
	BasePromOp
}

func (c *FloatCountPromOp) CreateRoutine(p *AggParams) (Routine, error) {
	c.BasePromOp = *NewBasePromOp("count_prom", FloatCountPromReduce, FloatCountPromMerge)
	return c.BasePromOp.CreateRoutine(p)
}
