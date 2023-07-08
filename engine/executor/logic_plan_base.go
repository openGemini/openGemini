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
//nolint

package executor

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	internal "github.com/openGemini/openGemini/open_src/influx/query/proto"
)

type LogicalPlanBase struct {
	id     uint64
	schema hybridqp.Catalog
	rt     hybridqp.RowDataType
	ops    []hybridqp.ExprOptions
	trait  hybridqp.Trait

	inputs     []hybridqp.QueryNode
	digest     bool
	digestName []byte
	digestBuff bytes.Buffer
}

type LogicalPlaner interface {
	RowDataType() hybridqp.RowDataType
	RowExprOptions() []hybridqp.ExprOptions
	SetInputs(inputs []hybridqp.QueryNode)
	LogicPlanType() internal.LogicPlanType
}

func NewLogicalPlanBase(schema hybridqp.Catalog, rt hybridqp.RowDataType, ops []hybridqp.ExprOptions) *LogicalPlanBase {
	return &LogicalPlanBase{
		id:     hybridqp.GenerateNodeId(),
		schema: schema,
		rt:     rt,
		ops:    ops,
	}
}

func (p *LogicalPlanBase) ForwardInit(input hybridqp.QueryNode) {
	inrefs := input.RowDataType().MakeRefs()
	refs := make([]influxql.VarRef, 0, len(inrefs))
	p.ops = make([]hybridqp.ExprOptions, 0, len(inrefs))

	for _, ref := range inrefs {
		clone := ref

		p.ops = append(p.ops, hybridqp.ExprOptions{Expr: &clone, Ref: ref})

		refs = append(refs, ref)
	}

	p.rt = hybridqp.NewRowDataTypeImpl(refs...)
}

func (p *LogicalPlanBase) InitRef(input hybridqp.QueryNode) {
	p.ops = input.RowExprOptions()
	p.rt = input.RowDataType()
}

func (p *LogicalPlanBase) Trait() hybridqp.Trait {
	return p.trait
}

func (p *LogicalPlanBase) ApplyTrait(trait hybridqp.Trait) {
	p.trait = trait
}

func (p *LogicalPlanBase) ExplainIterms(writer LogicalPlanWriter) {
	for _, op := range p.ops {
		writer.Item(op.Ref.String(), op.Expr.String())
	}
}

func (p *LogicalPlanBase) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalPlanBase) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalPlanBase) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalPlanBase) Dummy() bool {
	return false
}

func (p *LogicalPlanBase) ID() uint64 {
	return p.id
}

func (p *LogicalPlanBase) DeriveOperations() {
	panic("impl me in derive type")
}

func (p *LogicalPlanBase) PreAggInit() {
	schema := p.schema

	refsLen := len(schema.Refs()) + len(schema.OrigCalls())

	paras := make([]*Para, 0, refsLen)
	for _, call := range schema.OrigCalls() {
		derivedRef := schema.DerivedOrigCall(call)
		paras = append(paras, &Para{Expr: call, VarRef: &derivedRef})
	}

	isCallArgs := func(ref *influxql.VarRef) bool {
		for _, call := range schema.OrigCalls() {
			for _, expr := range call.Args {
				if ref == schema.Refs()[expr.String()] {
					return true
				}
			}
		}
		return false
	}

	for _, ref := range schema.Refs() {
		if !schema.IsRefInQueryFields(ref) && isCallArgs(ref) {
			continue
		}
		derivedRef := schema.DerivedRef(ref)
		paras = append(paras, &Para{Expr: ref, VarRef: &derivedRef})
	}

	refs := make([]influxql.VarRef, 0, len(paras))
	p.ops = make([]hybridqp.ExprOptions, 0, len(paras))

	sort.Sort(ParaPointers(paras))
	for i := range paras {
		clone := influxql.CloneExpr(paras[i].Expr)
		p.ops = append(p.ops, hybridqp.ExprOptions{Expr: clone, Ref: *paras[i].VarRef})
		refs = append(refs, *paras[i].VarRef)
	}

	p.rt = hybridqp.NewRowDataTypeImpl(refs...)
}

func (p *LogicalPlanBase) initForSink() {
	schema := p.schema
	tableExprs := make([]*influxql.VarRef, 0, len(schema.Refs()))

	for _, ref := range schema.Refs() {
		tableExprs = append(tableExprs, ref)
	}
	sort.Sort(influxql.VarRefPointers(tableExprs))

	refs := make([]influxql.VarRef, 0, len(tableExprs))
	p.ops = make([]hybridqp.ExprOptions, 0, len(tableExprs))

	for i, expr := range tableExprs {
		clone := influxql.CloneVarRef(expr)
		derivedRef := schema.DerivedRef(tableExprs[i])
		p.ops = append(p.ops, hybridqp.ExprOptions{Expr: clone, Ref: derivedRef})
		refs = append(refs, derivedRef)
	}

	p.rt = hybridqp.NewRowDataTypeImpl(refs...)
}

func (p *LogicalPlanBase) Children() []hybridqp.QueryNode {
	if p.inputs == nil {
		return nil
	}
	return p.inputs
}

func NewLogicalPlanSingle(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalPlanSingle {
	inputs := []hybridqp.QueryNode{input}
	if input == nil {
		inputs = nil
	}
	logicalPlanSingle := &LogicalPlanSingle{LogicalPlanBase{
		id:     hybridqp.GenerateNodeId(),
		schema: schema,
		rt:     nil,
		ops:    nil,
		inputs: inputs,
	}}
	return logicalPlanSingle
}

type LogicalPlanSingle struct {
	LogicalPlanBase
}

func (p *LogicalPlanSingle) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical")
	}
	p.inputs = children
	p.digest = false
}

func (p *LogicalPlanSingle) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.inputs[0] = child
	p.digest = false
}

func (p *LogicalPlanSingle) SetHoltWintersType(setOps bool, fields influxql.Fields) {
	for i, ref := range fields {
		rVal, ok := ref.Expr.(*influxql.VarRef)
		if !ok {
			continue
		}
		v := rVal.Val
		if strings.HasPrefix(v, "holt_winters") || p.schema.IsHoltWinters(v) {
			p.rt.SetDataType(i, influxql.Float)
			if val, ok := p.ops[i].Expr.(*influxql.VarRef); ok {
				val.SetDataType(influxql.Float)
				if setOps {
					val.SetVal(v)
				}
			}
			p.ops[i].Ref.SetDataType(influxql.Float)
		}
	}
}

type LogicalPlanMulti struct {
	LogicalPlanBase
}

func (p *LogicalPlanMulti) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(p.inputs) != len(children) {
		panic(fmt.Sprintf("%d children in logical, but replace with %d children", len(p.inputs), len(children)))
	}

	for i := range p.inputs {
		p.inputs[i] = children[i]
		p.digest = false
	}
}

func (p *LogicalPlanMulti) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	p.inputs[ordinal] = child
	p.digest = false
}

func (p *LogicalPlanBase) SetSchema(schema hybridqp.Catalog) {
	p.schema = schema
}

func (p *LogicalPlanBase) SetInputs(inputs []hybridqp.QueryNode) {
	if len(inputs) == 0 {
		return
	}
	p.inputs = inputs
}

type Para struct {
	Expr   influxql.Expr
	VarRef *influxql.VarRef
}

type ParaPointers []*Para

// Len implements sort.Interface.
func (a ParaPointers) Len() int { return len(a) }

// Less implements sort.Interface.
func (a ParaPointers) Less(i, j int) bool {
	if a[i].VarRef.Val != a[j].VarRef.Val {
		return a[i].VarRef.Val < a[j].VarRef.Val
	}
	return a[i].VarRef.Type < a[j].VarRef.Type
}

// Swap implements sort.Interface.
func (a ParaPointers) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
