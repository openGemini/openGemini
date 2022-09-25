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
	"container/list"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

const tagSetAgg = 1

var (
	_ LogicalPlan = &LogicalAggregate{}
	_ LogicalPlan = &LogicalLimit{}
	_ LogicalPlan = &LogicalFilter{}
	_ LogicalPlan = &LogicalFilterBlank{}
	_ LogicalPlan = &LogicalMerge{}
	_ LogicalPlan = &LogicalSortMerge{}
	_ LogicalPlan = &LogicalDedupe{}
	_ LogicalPlan = &LogicalInterval{}
	_ LogicalPlan = &LogicalReader{}
	_ LogicalPlan = &LogicalTagSubset{}
	_ LogicalPlan = &LogicalFill{}
	_ LogicalPlan = &LogicalAlign{}
	_ LogicalPlan = &LogicalMst{}
	_ LogicalPlan = &LogicalProject{}
	_ LogicalPlan = &LogicalHttpSender{}
	_ LogicalPlan = &LogicalExchange{}
)

var mergeCall = map[string]bool{"percentile": true, "rate": true, "irate": true, "absent": true, "stddev": true, "mode": true, "median": true, "sample": true}

var sortedMergeCall = map[string]bool{
	"difference": true, "non_negative_difference": true,
	"derivative": true, "non_negative_derivative": true,
	"elapsed": true, "integral": true, "moving_average": true, "cumulative_sum": true,
}

var (
	enableFileCursor bool = false
)

func EnableFileCursor(en bool) {
	enableFileCursor = en
}

func GetEnableFileCursor() bool {
	return enableFileCursor
}

// Names returns a list of field names.
func WalkRefs(fields influxql.Fields) []influxql.VarRef {
	refs := make(map[influxql.VarRef]struct{})

	var walk func(exp influxql.Expr)
	walk = func(exp influxql.Expr) {
		switch expr := exp.(type) {
		case *influxql.VarRef:
			refs[*expr] = struct{}{}
		case *influxql.Call:
			for _, expr := range expr.Args {
				if ref, ok := expr.(*influxql.VarRef); ok {
					refs[*ref] = struct{}{}
				}
			}
		case *influxql.BinaryExpr:
			walk(expr.LHS)
			walk(expr.RHS)
		case *influxql.ParenExpr:
			walk(expr.Expr)
		default:
			logger.GetLogger().Warn("logic_plan WalkRefs exp type unrecognized")
		}
	}

	for _, f := range fields {
		walk(f.Expr)
	}

	// Turn the map into a slice.
	v := make([]influxql.VarRef, 0, len(refs))
	for ref := range refs {
		v = append(v, ref)
	}
	return v
}

func ValidateFieldsFromPlans(plans []hybridqp.QueryNode) bool {
	if len(plans) == 0 {
		return false
	}

	base := plans[0]
	for i, plan := range plans {
		if i == 0 {
			continue
		}

		if len(base.RowDataType().Fields()) != len(plan.RowDataType().Fields()) {
			return false
		}
	}

	return true
}

func GetTypeName(i interface{}) string {
	subs := strings.Split(reflect.TypeOf(i).String(), ".")
	return subs[len(subs)-1]
}

func GetType(i interface{}) string {
	return reflect.TypeOf(i).String()
}

type LogicalPlanWriter interface {
	Explain(LogicalPlan)
	Item(string, interface{})
	String() string
}

type LogicalPlan interface {
	hybridqp.QueryNode
	Explain(LogicalPlanWriter)
	Schema() hybridqp.Catalog
}

type ValuePair struct {
	First  string
	Second interface{}
}

func NewValuePair(first string, second interface{}) *ValuePair {
	return &ValuePair{
		First:  first,
		Second: second,
	}
}

type Spacer struct {
	Space int
}

func NewSpacer() *Spacer {
	return &Spacer{
		Space: 0,
	}
}

func (s *Spacer) Add(num int) {
	s.Space += num
}

func (s *Spacer) Sub(num int) {
	s.Space -= num
	if s.Space < 0 {
		s.Space = 0
	}
}

func (s *Spacer) String() string {
	d := make([]byte, s.Space)
	for i := 0; i < s.Space; i++ {
		d[i] = ' '
	}

	return string(d)
}

type LogicalPlanWriterImpl struct {
	Builder *strings.Builder
	Values  *list.List
	Spacer  *Spacer
}

func NewLogicalPlanWriterImpl(builder *strings.Builder) *LogicalPlanWriterImpl {
	return &LogicalPlanWriterImpl{
		Builder: builder,
		Values:  list.New(),
		Spacer:  NewSpacer(),
	}
}

func (w *LogicalPlanWriterImpl) String() string {
	return w.Builder.String()
}

func (w *LogicalPlanWriterImpl) Explain(node LogicalPlan) {
	w.Builder.WriteString(w.Spacer.String())
	w.Builder.WriteString(node.String())

	j := 0

	e := w.Values.Front()
	for {
		if e == nil {
			break
		}

		if j == 0 {
			w.Builder.WriteString("(")
		} else {
			w.Builder.WriteString(",")
		}

		j++

		vp, ok := e.Value.(*ValuePair)
		if !ok {
			logger.GetLogger().Warn("LogicalPlanWriterImpl Explain element value isn't *ValuePair")
			r := e
			e = e.Next()
			w.Values.Remove(r)
			continue
		}
		w.Builder.WriteString(vp.First)
		w.Builder.WriteString("=[")
		w.Builder.WriteString(fmt.Sprintf("%v", vp.Second))
		w.Builder.WriteString("]")

		r := e
		e = e.Next()
		w.Values.Remove(r)
	}

	if j > 0 {
		w.Builder.WriteString(")")
	}

	w.Builder.WriteString("\n")

	w.Spacer.Add(2)
	w.ExplainChildren(node.Children())
	w.Spacer.Sub(2)
}

func (w *LogicalPlanWriterImpl) ExplainChildren(children []hybridqp.QueryNode) {
	for _, c := range children {
		c.(LogicalPlan).Explain(w)
	}
}

func (w *LogicalPlanWriterImpl) Item(term string, value interface{}) {
	vp := NewValuePair(term, value)
	w.Values.PushBack(vp)
}

type LogicalPlanBase struct {
	id     uint64
	schema hybridqp.Catalog
	rt     hybridqp.RowDataType
	ops    []hybridqp.ExprOptions
	trait  hybridqp.Trait
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

type LogicalAggregate struct {
	input           hybridqp.QueryNode
	calls           map[string]*influxql.Call
	callsOrder      []string
	isCountDistinct bool
	aggType         int
	LogicalPlanBase
}

func NewCountDistinctAggregate(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalAggregate {
	agg := NewLogicalAggregate(input, schema)
	agg.isCountDistinct = true
	return agg
}

func NewLogicalAggregate(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalAggregate {
	agg := &LogicalAggregate{
		input:           input,
		calls:           make(map[string]*influxql.Call),
		callsOrder:      nil,
		isCountDistinct: false,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	agg.callsOrder = make([]string, 0, len(agg.schema.Calls()))
	var ok bool
	for k, c := range agg.schema.Calls() {
		agg.calls[k], ok = influxql.CloneExpr(c).(*influxql.Call)
		if !ok {
			logger.GetLogger().Warn("NewLogicalAggregate call type isn't *influxql.Call")
			return nil
		}
		agg.callsOrder = append(agg.callsOrder, k)
	}
	sort.Strings(agg.callsOrder)

	agg.init()

	return agg
}

func NewLogicalTagSetAggregate(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalAggregate {
	agg := &LogicalAggregate{
		aggType:         tagSetAgg,
		input:           input,
		calls:           make(map[string]*influxql.Call),
		callsOrder:      nil,
		isCountDistinct: false,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	agg.callsOrder = make([]string, 0, len(agg.schema.Calls()))
	var ok bool
	for k, c := range agg.schema.Calls() {
		agg.calls[k], ok = influxql.CloneExpr(c).(*influxql.Call)
		if !ok {
			logger.GetLogger().Warn("NewLogicalAggregate call type isn't *influxql.Call")
			return nil
		}
		agg.callsOrder = append(agg.callsOrder, k)
	}
	sort.Strings(agg.callsOrder)

	agg.init()

	return agg
}

func (p *LogicalAggregate) DeriveOperations() {
	if p.isCountDistinct {
		p.initCountDistinct()
		return
	}
	p.init()
}

func (p *LogicalAggregate) initCountDistinct() {
	if p.schema.CountDistinct() == nil {
		panic("init count distinct with out call")
	}
	inrefs := p.input.RowDataType().MakeRefs()
	if len(inrefs) != 1 {
		panic("only one ref in input of count distinct")
	}
	if inrefs[0].String() != p.schema.CountDistinct().Args[0].String() {
		panic(fmt.Sprintf("ref of input(%v) is different with ref of count distinct(%v)",
			inrefs[0].String(), p.schema.CountDistinct().Args[0].String()))
	}

	refs := make([]influxql.VarRef, 0, len(inrefs))
	p.ops = make([]hybridqp.ExprOptions, 0, len(inrefs))

	ref := p.schema.Mapping()[p.schema.CountDistinct()]
	count := influxql.CloneExpr(p.schema.CountDistinct())
	refs = append(refs, ref)
	p.ops = append(p.ops, hybridqp.ExprOptions{Expr: count, Ref: ref})

	p.rt = hybridqp.NewRowDataTypeImpl(refs...)
}

func (p *LogicalAggregate) init() {
	refs := p.input.RowDataType().MakeRefs()

	m := make(map[string]influxql.VarRef)
	for _, ref := range refs {
		m[ref.Val] = ref
	}

	mc := make(map[string]hybridqp.ExprOptions)

	for k, c := range p.calls {
		ref := p.schema.Mapping()[p.schema.Calls()[k]]
		m[ref.Val] = ref

		mc[ref.Val] = hybridqp.ExprOptions{Expr: influxql.CloneExpr(c), Ref: ref}

		for _, arg := range c.Args {
			switch n := arg.(type) {
			case *influxql.VarRef:
				if ref.Val != n.Val {
					if !p.schema.IsRefInSymbolFields(n) {
						delete(m, n.Val)
					}
				}
			default:
			}
		}
	}

	refs = make([]influxql.VarRef, 0, len(m))
	for _, ref := range m {
		if _, ok := mc[ref.Val]; !ok {
			clone := ref
			mc[ref.Val] = hybridqp.ExprOptions{Expr: &clone, Ref: ref}
		}
		refs = append(refs, ref)
	}

	sort.Sort(influxql.VarRefs(refs))

	p.rt = hybridqp.NewRowDataTypeImpl(refs...)

	p.ops = make([]hybridqp.ExprOptions, 0, len(mc))

	for _, r := range refs {
		p.ops = append(p.ops, mc[r.Val])
	}
}

func (p *LogicalAggregate) ForwardCallArgs() {
	for k, call := range p.calls {
		if len(call.Args) > 0 {
			ref := p.schema.Mapping()[p.schema.Calls()[k]]
			call.Args[0] = &ref
		}
	}
}

func (p *LogicalAggregate) CountToSum() {
	for _, call := range p.calls {
		if call.Name == "count" {
			call.Name = "sum"
		}
	}
}

func (p *LogicalAggregate) Clone() hybridqp.QueryNode {
	clone := &LogicalAggregate{}
	*clone = *p
	clone.calls = make(map[string]*influxql.Call)
	var ok bool
	for k, c := range p.calls {
		clone.calls[k], ok = influxql.CloneExpr(c).(*influxql.Call)
		if !ok {
			logger.GetLogger().Warn("LogicalAggregate clone: type isn't *influxql.Call")
		}
	}
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalAggregate) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalAggregate) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical aggregate")
	}
	p.input = children[0]
}

func (p *LogicalAggregate) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalAggregate) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalAggregate) String() string {
	return GetTypeName(p)
}

func (p *LogicalAggregate) Type() string {
	return GetType(p)
}

func (p *LogicalAggregate) Digest() string {
	var buffer bytes.Buffer
	firstCall := true
	for _, order := range p.callsOrder {
		call := p.calls[order]
		if firstCall {
			buffer.WriteString(call.String())
			firstCall = false
			continue
		}
		buffer.WriteString(",")
		buffer.WriteString(call.String())
	}
	return fmt.Sprintf("%s(%d)[%d](%s)(%s)", GetTypeName(p), p.aggType, p.input.ID(), p.schema.Fields(), buffer.String())
}

func (p *LogicalAggregate) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalAggregate) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalAggregate) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalAggregate) Dummy() bool {
	return false
}

type LogicalSlidingWindow struct {
	input      hybridqp.QueryNode
	calls      map[string]*influxql.Call
	callsOrder []string
	LogicalPlanBase
}

func NewLogicalSlidingWindow(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSlidingWindow {
	agg := &LogicalSlidingWindow{
		input:      input,
		calls:      make(map[string]*influxql.Call),
		callsOrder: nil,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	agg.callsOrder = make([]string, 0, len(agg.schema.Calls()))
	var ok bool
	for k, c := range agg.schema.Calls() {
		agg.calls[k], ok = influxql.CloneExpr(c).(*influxql.Call)
		if !ok {
			logger.GetLogger().Warn("NewLogicalSlidingWindow call type isn't *influxql.Call")
		}
		agg.callsOrder = append(agg.callsOrder, k)
	}
	sort.Strings(agg.callsOrder)

	agg.init()

	return agg
}

func (p *LogicalSlidingWindow) DeriveOperations() {
	p.init()
}

func (p *LogicalSlidingWindow) init() {
	refs := p.input.RowDataType().MakeRefs()

	m := make(map[string]influxql.VarRef)
	for _, ref := range refs {
		m[ref.Val] = ref
	}

	mc := make(map[string]hybridqp.ExprOptions)

	for k, c := range p.calls {
		ref := p.schema.Mapping()[p.schema.Calls()[k]]
		m[ref.Val] = ref

		mc[ref.Val] = hybridqp.ExprOptions{Expr: influxql.CloneExpr(c), Ref: ref}

		for _, arg := range c.Args {
			switch n := arg.(type) {
			case *influxql.VarRef:
				if ref.Val != n.Val {
					if !p.schema.IsRefInSymbolFields(n) {
						delete(m, n.Val)
					}
				}
			default:
			}
		}
	}

	refs = make([]influxql.VarRef, 0, len(m))
	for _, ref := range m {
		if _, ok := mc[ref.Val]; !ok {
			clone := ref
			mc[ref.Val] = hybridqp.ExprOptions{Expr: &clone, Ref: ref}
		}
		refs = append(refs, ref)
	}

	sort.Sort(influxql.VarRefs(refs))

	p.rt = hybridqp.NewRowDataTypeImpl(refs...)

	p.ops = make([]hybridqp.ExprOptions, 0, len(mc))

	for _, r := range refs {
		p.ops = append(p.ops, mc[r.Val])
	}
}

func (p *LogicalSlidingWindow) ForwardCallArgs() {
	for k, call := range p.calls {
		if len(call.Args) > 0 {
			ref := p.schema.Mapping()[p.schema.Calls()[k]]
			call.Args[0] = &ref
		}
	}
}

func (p *LogicalSlidingWindow) CountToSum() {
	for _, call := range p.calls {
		if call.Name == "count" {
			call.Name = "sum"
		}
	}
}

func (p *LogicalSlidingWindow) Clone() hybridqp.QueryNode {
	clone := &LogicalSlidingWindow{}
	*clone = *p
	clone.calls = make(map[string]*influxql.Call)
	var ok bool
	for k, c := range p.calls {
		clone.calls[k], ok = influxql.CloneExpr(c).(*influxql.Call)
		if !ok {
			logger.GetLogger().Warn("LogicalSlidingWindow clone: type isn't *influxql.Call")
		}
	}
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalSlidingWindow) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalSlidingWindow) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical aggregate")
	}
	p.input = children[0]
}

func (p *LogicalSlidingWindow) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalSlidingWindow) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSlidingWindow) String() string {
	return GetTypeName(p)
}

func (p *LogicalSlidingWindow) Type() string {
	return GetType(p)
}

func (p *LogicalSlidingWindow) Digest() string {
	var buffer bytes.Buffer
	firstCall := true
	for _, order := range p.callsOrder {
		call := p.calls[order]
		if firstCall {
			buffer.WriteString(call.String())
			firstCall = false
			continue
		}
		buffer.WriteString(",")
		buffer.WriteString(call.String())
	}
	return fmt.Sprintf("%s[%d](%s)(%s)", GetTypeName(p), p.input.ID(), p.schema.Fields(), buffer.String())
}

func (p *LogicalSlidingWindow) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalSlidingWindow) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalSlidingWindow) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalSlidingWindow) Dummy() bool {
	return false
}

type LogicalFill struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalFill(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalFill {
	fill := &LogicalFill{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	fill.init()

	return fill
}

func (p *LogicalFill) DeriveOperations() {
	p.init()
}

func (p *LogicalFill) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalFill) Clone() hybridqp.QueryNode {
	clone := &LogicalFill{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalFill) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalFill) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical fill")
	}
	p.input = children[0]
}

func (p *LogicalFill) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalFill) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalFill) String() string {
	return GetTypeName(p)
}

func (p *LogicalFill) Type() string {
	return GetType(p)
}

func (p *LogicalFill) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalFill) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalFill) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalFill) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalFill) Dummy() bool {
	return false
}

type LogicalLimit struct {
	input     hybridqp.QueryNode
	LimitPara LimitTransformParameters
	LogicalPlanBase
}

func NewLogicalLimit(input hybridqp.QueryNode, schema hybridqp.Catalog, parameters LimitTransformParameters) *LogicalLimit {
	limit := &LogicalLimit{
		input:     input,
		LimitPara: parameters,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	limit.init()

	return limit
}

func (p *LogicalLimit) DeriveOperations() {
	p.init()
}

func (p *LogicalLimit) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalLimit) Clone() hybridqp.QueryNode {
	clone := &LogicalLimit{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalLimit) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalLimit) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical limit")
	}
	p.input = children[0]
}

func (p *LogicalLimit) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalLimit) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalLimit) String() string {
	return GetTypeName(p)
}

func (p *LogicalLimit) Type() string {
	return GetType(p)
}

func (p *LogicalLimit) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalLimit) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalLimit) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalLimit) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalLimit) Dummy() bool {
	return false
}

type LogicalFilter struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalFilter(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalFilter {
	filter := &LogicalFilter{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	filter.init()

	return filter
}

func (p *LogicalFilter) DeriveOperations() {
	p.init()
}

func (p *LogicalFilter) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalFilter) Clone() hybridqp.QueryNode {
	clone := &LogicalFilter{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalFilter) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalFilter) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical filter")
	}
	p.input = children[0]
}

func (p *LogicalFilter) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalFilter) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalFilter) String() string {
	return GetTypeName(p)
}

func (p *LogicalFilter) Type() string {
	return GetType(p)
}

func (p *LogicalFilter) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalFilter) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalFilter) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalFilter) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalFilter) Dummy() bool {
	return false
}

type LogicalMerge struct {
	inputs []hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalMerge(inputs []hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalMerge {
	merge := &LogicalMerge{
		inputs: inputs,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	merge.init()

	return merge
}

func (p *LogicalMerge) DeriveOperations() {
	p.init()
}

func (p *LogicalMerge) init() {
	if !ValidateFieldsFromPlans(p.inputs) {
		panic("validate all input of merge failed")
	}

	input := p.inputs[0]

	p.ForwardInit(input)
}

func (p *LogicalMerge) Clone() hybridqp.QueryNode {
	clone := &LogicalMerge{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalMerge) Children() []hybridqp.QueryNode {
	nodes := make([]hybridqp.QueryNode, 0, len(p.inputs))
	nodes = append(nodes, p.inputs...)
	return nodes
}

func (p *LogicalMerge) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(p.inputs) != len(children) {
		panic(fmt.Sprintf("%d children in logical merge, but replace with %d children", len(p.inputs), len(children)))
	}

	for i := range p.inputs {
		p.inputs[i] = children[i]
	}
}

func (p *LogicalMerge) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	p.inputs[ordinal] = child
}

func (p *LogicalMerge) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalMerge) String() string {
	return GetTypeName(p)
}

func (p *LogicalMerge) Type() string {
	return GetType(p)
}

func (p *LogicalMerge) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.inputs[0].ID())
}

func (p *LogicalMerge) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalMerge) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalMerge) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalMerge) Dummy() bool {
	return false
}

type LogicalSortMerge struct {
	inputs []hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalSortMerge(inputs []hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSortMerge {
	merge := &LogicalSortMerge{
		inputs: inputs,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	merge.init()

	return merge
}

func (p *LogicalSortMerge) DeriveOperations() {
	p.init()
}

func (p *LogicalSortMerge) init() {
	if !ValidateFieldsFromPlans(p.inputs) {
		panic("validate all input of sort merge failed")
	}

	input := p.inputs[0]

	p.ForwardInit(input)
}

func (p *LogicalSortMerge) Clone() hybridqp.QueryNode {
	clone := &LogicalSortMerge{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalSortMerge) Children() []hybridqp.QueryNode {
	nodes := make([]hybridqp.QueryNode, 0, len(p.inputs))
	nodes = append(nodes, p.inputs...)
	return nodes
}

func (p *LogicalSortMerge) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(p.inputs) != len(children) {
		panic(fmt.Sprintf("%d children in logical sort merge, but replace with %d children", len(p.inputs), len(children)))
	}

	for i := range p.inputs {
		p.inputs[i] = children[i]
	}
}

func (p *LogicalSortMerge) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	p.inputs[ordinal] = child
}

func (p *LogicalSortMerge) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSortMerge) String() string {
	return GetTypeName(p)
}

func (p *LogicalSortMerge) Type() string {
	return GetType(p)
}

func (p *LogicalSortMerge) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.inputs[0].ID())
}

func (p *LogicalSortMerge) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalSortMerge) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalSortMerge) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalSortMerge) Dummy() bool {
	return false
}

type LogicalSortAppend struct {
	inputs []hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalSortAppend(inputs []hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSortAppend {
	merge := &LogicalSortAppend{
		inputs: inputs,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	merge.init()

	return merge
}

func (p *LogicalSortAppend) DeriveOperations() {
	p.init()
}

func (p *LogicalSortAppend) init() {
	if !ValidateFieldsFromPlans(p.inputs) {
		panic("validate all input of sort merge failed")
	}

	input := p.inputs[0]

	p.ForwardInit(input)
}

func (p *LogicalSortAppend) Clone() hybridqp.QueryNode {
	clone := &LogicalSortAppend{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalSortAppend) Children() []hybridqp.QueryNode {
	nodes := make([]hybridqp.QueryNode, 0, len(p.inputs))
	nodes = append(nodes, p.inputs...)
	return nodes
}

func (p *LogicalSortAppend) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(p.inputs) != len(children) {
		panic(fmt.Sprintf("%d children in logical sort merge, but replace with %d children", len(p.inputs), len(children)))
	}

	for i := range p.inputs {
		p.inputs[i] = children[i]
	}
}

func (p *LogicalSortAppend) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	p.inputs[ordinal] = child
}

func (p *LogicalSortAppend) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSortAppend) String() string {
	return GetTypeName(p)
}

func (p *LogicalSortAppend) Type() string {
	return GetType(p)
}

func (p *LogicalSortAppend) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.inputs[0].ID())
}

func (p *LogicalSortAppend) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalSortAppend) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalSortAppend) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalSortAppend) Dummy() bool {
	return false
}

type LogicalDedupe struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalDedupe(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalDedupe {
	dedupe := &LogicalDedupe{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	dedupe.init()

	return dedupe
}

func (p *LogicalDedupe) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalDedupe) Clone() hybridqp.QueryNode {
	clone := &LogicalDedupe{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalDedupe) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalDedupe) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical dequpe")
	}
	p.input = children[0]
}

func (p *LogicalDedupe) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalDedupe) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalDedupe) String() string {
	return GetTypeName(p)
}

func (p *LogicalDedupe) Type() string {
	return GetType(p)
}

func (p *LogicalDedupe) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalDedupe) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalDedupe) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalDedupe) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalDedupe) Dummy() bool {
	return false
}

type LogicalInterval struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalInterval(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalInterval {
	interval := &LogicalInterval{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	interval.init()

	return interval
}

func (p *LogicalInterval) DeriveOperations() {
	p.init()
}

func (p *LogicalInterval) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalInterval) Clone() hybridqp.QueryNode {
	clone := &LogicalInterval{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalInterval) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalInterval) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalInterval) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalInterval) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalInterval) String() string {
	return GetTypeName(p)
}

func (p *LogicalInterval) Type() string {
	return GetType(p)
}

func (p *LogicalInterval) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalInterval) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalInterval) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalInterval) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalInterval) Dummy() bool {
	return false
}

type LogicalFilterBlank struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalFilterBlank(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalFilterBlank {
	interval := &LogicalFilterBlank{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	interval.init()

	return interval
}

func (p *LogicalFilterBlank) DeriveOperations() {
	p.init()
}

func (p *LogicalFilterBlank) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalFilterBlank) Clone() hybridqp.QueryNode {
	clone := &LogicalFilterBlank{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalFilterBlank) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalFilterBlank) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalFilterBlank) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalFilterBlank) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalFilterBlank) String() string {
	return GetTypeName(p)
}

func (p *LogicalFilterBlank) Type() string {
	return GetType(p)
}

func (p *LogicalFilterBlank) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalFilterBlank) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalFilterBlank) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalFilterBlank) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalFilterBlank) Dummy() bool {
	return false
}

type LogicalAlign struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalAlign(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalAlign {
	align := &LogicalAlign{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	align.init()

	return align
}

func (p *LogicalAlign) DeriveOperations() {
	p.init()
}

func (p *LogicalAlign) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalAlign) Clone() hybridqp.QueryNode {
	clone := &LogicalAlign{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalAlign) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalAlign) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalAlign) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalAlign) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalAlign) String() string {
	return GetTypeName(p)
}

func (p *LogicalAlign) Type() string {
	return GetType(p)
}

func (p *LogicalAlign) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalAlign) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalAlign) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalAlign) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalAlign) Dummy() bool {
	return false
}

type LogicalMst struct {
	LogicalPlanBase
}

func NewLogicalMst(rt hybridqp.RowDataType) *LogicalMst {
	mst := &LogicalMst{
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: nil,
			rt:     rt,
			ops:    nil,
		},
	}

	return mst
}

func (p *LogicalMst) Clone() hybridqp.QueryNode {
	clone := &LogicalMst{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalMst) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{}
}

func (p *LogicalMst) ReplaceChildren(children []hybridqp.QueryNode) {

}

func (p *LogicalMst) ReplaceChild(ordinal int, child hybridqp.QueryNode) {

}

func (p *LogicalMst) Explain(writer LogicalPlanWriter) {
	writer.Explain(p)
}

func (p *LogicalMst) String() string {
	return GetTypeName(p)
}

func (p *LogicalMst) Type() string {
	return GetType(p)
}

func (p *LogicalMst) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), 0)
}

func (p *LogicalMst) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalMst) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalMst) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalMst) Dummy() bool {
	return true
}

type LogicalSeries struct {
	LogicalPlanBase
	mstName string
}

func NewLogicalSeries(schema hybridqp.Catalog) *LogicalSeries {
	series := &LogicalSeries{
		LogicalPlanBase: LogicalPlanBase{
			schema: schema,
			id:     hybridqp.GenerateNodeId(),
			rt:     nil,
			ops:    nil,
		},
	}
	if schema.Options().(*query.ProcessorOptions).Sources != nil {
		series.mstName = schema.Options().(*query.ProcessorOptions).Sources[0].String()
	}
	series.init()

	return series
}

func (p *LogicalSeries) DeriveOperations() {
	p.init()
}

func (p *LogicalSeries) init() {
	schema := p.schema

	tableExprs := make([]influxql.Expr, 0, len(schema.Refs()))
	derivedRefs := make([]influxql.VarRef, 0, len(schema.Refs()))

	for _, ref := range schema.Refs() {
		tableExprs = append(tableExprs, ref)
		derivedRefs = append(derivedRefs, schema.DerivedRef(ref))
	}

	refs := make([]influxql.VarRef, 0, len(tableExprs))
	p.ops = make([]hybridqp.ExprOptions, 0, len(tableExprs))

	for i, expr := range tableExprs {
		clone := influxql.CloneExpr(expr)
		p.ops = append(p.ops, hybridqp.ExprOptions{Expr: clone, Ref: derivedRefs[i]})
		refs = append(refs, derivedRefs[i])
	}

	p.rt = hybridqp.NewRowDataTypeImpl(refs...)
}

func (p *LogicalSeries) Clone() hybridqp.QueryNode {
	clone := &LogicalSeries{}
	*clone = *p
	return clone
}

func (p *LogicalSeries) Children() []hybridqp.QueryNode {
	return nil
}

func (p *LogicalSeries) ReplaceChildren(children []hybridqp.QueryNode) {
	panic("no child in logical series")
}

func (p *LogicalSeries) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	panic(fmt.Sprintf("index %d out of range %d", ordinal, 0))
}

func (p *LogicalSeries) ExplainIterms(writer LogicalPlanWriter) {
	p.LogicalPlanBase.ExplainIterms(writer)
	writer.Item("mstName", p.mstName)
	writer.Item("ID", p.id)
}

func (p *LogicalSeries) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSeries) String() string {
	return GetTypeName(p)
}
func (p *LogicalSeries) Type() string {
	return GetType(p)
}

func (p *LogicalSeries) Digest() string {
	return fmt.Sprintf("%s[%s][%d]", GetTypeName(p), p.mstName, p.id)
}

func (p *LogicalSeries) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalSeries) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalSeries) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalSeries) Dummy() bool {
	return false
}

type LogicalReader struct {
	input      hybridqp.QueryNode
	cursor     []interface{}
	hasPreAgg  bool
	dimensions []string
	mstName    string
	LogicalPlanBase
}

func NewLogicalReader(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalReader {
	reader := &LogicalReader{
		input:      input,
		cursor:     nil,
		hasPreAgg:  false,
		dimensions: schema.Options().GetOptDimension(),
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	if schema.Sources() != nil {
		reader.mstName = schema.Sources()[0].String()
	} else if schema.Options().(*query.ProcessorOptions).Sources != nil {
		reader.mstName = schema.Options().(*query.ProcessorOptions).Sources[0].String()
	}

	reader.init()

	return reader
}

func (p *LogicalReader) MstName() string {
	return p.mstName
}

func (p *LogicalReader) DeriveOperations() {
	p.init()
}

func (p *LogicalReader) init() {
	if p.input == nil {
		p.initPreAgg()
	} else {
		p.ForwardInit(p.input)
	}
}

func (p *LogicalReader) HasPreAgg() bool {
	return p.hasPreAgg
}

func (p *LogicalReader) initPreAgg() {
	schema := p.schema

	tableExprs := make([]influxql.Expr, 0, len(schema.Refs())+len(schema.OrigCalls()))
	derivedRefs := make([]influxql.VarRef, 0, len(schema.Refs())+len(schema.OrigCalls()))
	for _, call := range schema.OrigCalls() {
		tableExprs = append(tableExprs, call)
		derivedRefs = append(derivedRefs, schema.DerivedOrigCall(call))
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
		tableExprs = append(tableExprs, ref)
		derivedRefs = append(derivedRefs, schema.DerivedRef(ref))
	}

	refs := make([]influxql.VarRef, 0, len(tableExprs))
	p.ops = make([]hybridqp.ExprOptions, 0, len(tableExprs))

	for i, expr := range tableExprs {
		clone := influxql.CloneExpr(expr)
		p.ops = append(p.ops, hybridqp.ExprOptions{Expr: clone, Ref: derivedRefs[i]})
		refs = append(refs, derivedRefs[i])
	}

	p.rt = hybridqp.NewRowDataTypeImpl(refs...)
}

func (p *LogicalReader) Clone() hybridqp.QueryNode {
	clone := &LogicalReader{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalReader) SetCursor(cursor []interface{}) {
	p.cursor = cursor
}

func (p *LogicalReader) Children() []hybridqp.QueryNode {
	if p.input == nil {
		return nil
	}
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalReader) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical reader")
	}
	p.input = children[0]
}

func (p *LogicalReader) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalReader) ExplainIterms(writer LogicalPlanWriter) {
	var builder strings.Builder
	for i, d := range p.dimensions {
		if i != 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(d)
	}
	writer.Item("dimensions", builder.String())
	writer.Item("mstName", p.mstName)
	writer.Item("ID", p.id)
}

func (p *LogicalReader) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	p.LogicalPlanBase.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalReader) String() string {
	return GetTypeName(p)
}

func (p *LogicalReader) Type() string {
	return GetType(p)
}

func (p *LogicalReader) Digest() string {
	if p.input != nil {
		return fmt.Sprintf("%s[%d,%s][%d]", GetTypeName(p), p.input.ID(), p.mstName, p.id)
	} else {
		return fmt.Sprintf("%s[%s][%d]", GetTypeName(p), p.mstName, p.id)
	}
}

func (p *LogicalReader) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalReader) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalReader) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalReader) Dummy() bool {
	return false
}

func (p *LogicalReader) Cursors() []interface{} {
	return p.cursor
}

type LogicalSubQuery struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalSubQuery(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSubQuery {
	subQuery := &LogicalSubQuery{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	subQuery.init()

	return subQuery
}

func (p *LogicalSubQuery) DeriveOperations() {
	p.init()
}

func (p *LogicalSubQuery) init() {
	schema := p.schema

	tableExprs := make([]influxql.Expr, 0, len(schema.Refs()))
	derivedRefs := make([]influxql.VarRef, 0, len(schema.Refs()))

	for _, ref := range schema.Refs() {
		tableExprs = append(tableExprs, ref)
		derivedRefs = append(derivedRefs, schema.DerivedRef(ref))
	}

	refs := make([]influxql.VarRef, 0, len(tableExprs))
	p.ops = make([]hybridqp.ExprOptions, 0, len(tableExprs))

	for i, expr := range tableExprs {
		clone := influxql.CloneExpr(expr)
		p.ops = append(p.ops, hybridqp.ExprOptions{Expr: clone, Ref: derivedRefs[i]})
		refs = append(refs, derivedRefs[i])
	}

	p.rt = hybridqp.NewRowDataTypeImpl(refs...)
}

func (p *LogicalSubQuery) Clone() hybridqp.QueryNode {
	clone := &LogicalSubQuery{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalSubQuery) Children() []hybridqp.QueryNode {
	if p.input == nil {
		return nil
	}
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalSubQuery) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical reader")
	}
	p.input = children[0]
}

func (p *LogicalSubQuery) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalSubQuery) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSubQuery) String() string {
	return GetTypeName(p)
}

func (p *LogicalSubQuery) Type() string {
	return GetType(p)
}

func (p *LogicalSubQuery) Digest() string {
	if p.input != nil {
		return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
	} else {
		return fmt.Sprintf("%s[]", GetTypeName(p))
	}
}

func (p *LogicalSubQuery) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalSubQuery) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalSubQuery) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalSubQuery) Dummy() bool {
	return false
}

type LogicalTagSubset struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalTagSubset(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalTagSubset {
	tagSubset := &LogicalTagSubset{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	tagSubset.init()

	return tagSubset
}

func (p *LogicalTagSubset) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalTagSubset) Clone() hybridqp.QueryNode {
	clone := &LogicalTagSubset{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalTagSubset) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalTagSubset) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalTagSubset) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalTagSubset) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalTagSubset) String() string {
	return GetTypeName(p)
}

func (p *LogicalTagSubset) Type() string {
	return GetType(p)
}

func (p *LogicalTagSubset) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalTagSubset) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalTagSubset) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalTagSubset) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalTagSubset) Dummy() bool {
	return false
}

type LogicalProject struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalProject(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalProject {
	project := &LogicalProject{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	project.init()

	return project
}

func (p *LogicalProject) DeriveOperations() {
	p.init()
}

func (p *LogicalProject) init() {
	p.rt = hybridqp.NewRowDataTypeImpl(p.schema.FieldsRef()...)

	p.ops = make([]hybridqp.ExprOptions, 0, len(p.schema.Fields()))

	for i, f := range p.schema.Fields() {
		p.ops = append(p.ops, hybridqp.ExprOptions{Expr: influxql.CloneExpr(f.Expr), Ref: p.schema.FieldsRef()[i]})
	}
}

func (p *LogicalProject) Clone() hybridqp.QueryNode {
	clone := &LogicalProject{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalProject) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalProject) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalProject) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalProject) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalProject) String() string {
	return GetTypeName(p)
}

func (p *LogicalProject) Type() string {
	return GetType(p)
}

func (p *LogicalProject) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalProject) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalProject) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalProject) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalProject) Dummy() bool {
	return false
}

type ExchangeType uint8

const (
	UNKNOWN_EXCHANGE ExchangeType = iota
	NODE_EXCHANGE
	SHARD_EXCHANGE
	READER_EXCHANGE
	SERIES_EXCHANGE
)

type ExchangeRole uint8

const (
	UNKNOWN_ROLE ExchangeRole = iota
	CONSUMER_ROLE
	PRODUCER_ROLE
)

type LogicalExchange struct {
	input   hybridqp.QueryNode
	eType   ExchangeType
	eRole   ExchangeRole
	eTraits []hybridqp.Trait
	LogicalPlanBase
}

func NewLogicalExchange(input hybridqp.QueryNode, eType ExchangeType, eTraits []hybridqp.Trait, schema hybridqp.Catalog) *LogicalExchange {
	exchange := &LogicalExchange{
		input:   input,
		eType:   eType,
		eRole:   CONSUMER_ROLE,
		eTraits: eTraits,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	exchange.init()

	return exchange
}

func (p *LogicalExchange) AddTrait(trait interface{}) {
	p.eTraits = append(p.eTraits, trait)
}

func (p *LogicalExchange) ToProducer() {
	p.eRole = PRODUCER_ROLE
}

func (p *LogicalExchange) ExchangeType() ExchangeType {
	return p.eType
}

func (p *LogicalExchange) ExchangeRole() ExchangeRole {
	return p.eRole
}

func (p *LogicalExchange) DeriveOperations() {
	p.init()
}

func (p *LogicalExchange) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalExchange) Clone() hybridqp.QueryNode {
	clone := &LogicalExchange{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalExchange) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalExchange) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalExchange) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalExchange) ExplainIterms(writer LogicalPlanWriter) {
	writer.Item("type", p.eType)
	writer.Item("traits", len(p.eTraits))
}

func (p *LogicalExchange) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalExchange) String() string {
	return GetTypeName(p)
}

func (p *LogicalExchange) Type() string {
	return GetType(p)
}

func (p *LogicalExchange) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

type LogicalGroupBy struct {
	input      hybridqp.QueryNode
	dimensions []string
	LogicalPlanBase
}

func NewLogicalGroupBy(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalGroupBy {
	groupby := &LogicalGroupBy{
		input:      input,
		dimensions: schema.Options().GetOptDimension(),
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	groupby.init()

	return groupby
}

func (p *LogicalGroupBy) DeriveOperations() {
	p.init()
}

func (p *LogicalGroupBy) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalGroupBy) Clone() hybridqp.QueryNode {
	clone := &LogicalGroupBy{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalGroupBy) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalGroupBy) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalGroupBy) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalGroupBy) ExplainIterms(writer LogicalPlanWriter) {
	var builder strings.Builder
	for i, d := range p.dimensions {
		if i != 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(d)
	}
	writer.Item("dimensions", builder.String())
}

func (p *LogicalGroupBy) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalGroupBy) String() string {
	return GetTypeName(p)
}

func (p *LogicalGroupBy) Type() string {
	return GetType(p)
}

func (p *LogicalGroupBy) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

type LogicalOrderBy struct {
	input      hybridqp.QueryNode
	dimensions []string
	LogicalPlanBase
}

func NewLogicalOrderBy(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalOrderBy {
	Orderby := &LogicalOrderBy{
		input:      input,
		dimensions: schema.Options().GetOptDimension(),
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	Orderby.init()

	return Orderby
}

func (p *LogicalOrderBy) DeriveOperations() {
	p.init()
}

func (p *LogicalOrderBy) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalOrderBy) Clone() hybridqp.QueryNode {
	clone := &LogicalOrderBy{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalOrderBy) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalOrderBy) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalOrderBy) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalOrderBy) ExplainIterms(writer LogicalPlanWriter) {
	var builder strings.Builder
	for i, d := range p.dimensions {
		if i != 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(d)
	}
	writer.Item("dimensions", builder.String())
}

func (p *LogicalOrderBy) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalOrderBy) String() string {
	return GetTypeName(p)
}

func (p *LogicalOrderBy) Type() string {
	return GetType(p)
}

func (p *LogicalOrderBy) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

type LogicalHttpSender struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalHttpSender(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalHttpSender {
	HttpSender := &LogicalHttpSender{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	HttpSender.init()

	return HttpSender
}

func (p *LogicalHttpSender) DeriveOperations() {
	p.init()
}

func (p *LogicalHttpSender) init() {
	p.rt = hybridqp.NewRowDataTypeImpl(p.schema.FieldsRef()...)

	p.ops = make([]hybridqp.ExprOptions, 0, len(p.schema.Fields()))

	for i, f := range p.schema.Fields() {
		p.ops = append(p.ops, hybridqp.ExprOptions{Expr: influxql.CloneExpr(f.Expr), Ref: p.schema.FieldsRef()[i]})
	}
}

func (p *LogicalHttpSender) Clone() hybridqp.QueryNode {
	clone := &LogicalHttpSender{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalHttpSender) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalHttpSender) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalHttpSender) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalHttpSender) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalHttpSender) String() string {
	return GetTypeName(p)
}

func (p *LogicalHttpSender) Type() string {
	return GetType(p)
}

func (p *LogicalHttpSender) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalHttpSender) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalHttpSender) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalHttpSender) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalHttpSender) Dummy() bool {
	return false
}

type LogicalHttpSenderHint struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalHttpSenderHint(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalHttpSenderHint {
	HttpSender := &LogicalHttpSenderHint{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	HttpSender.init()

	return HttpSender
}

func (p *LogicalHttpSenderHint) DeriveOperations() {
	p.init()
}

func (p *LogicalHttpSenderHint) init() {
	p.rt = hybridqp.NewRowDataTypeImpl(p.schema.FieldsRef()...)

	p.ops = make([]hybridqp.ExprOptions, 0, len(p.schema.Fields()))

	for i, f := range p.schema.Fields() {
		p.ops = append(p.ops, hybridqp.ExprOptions{Expr: influxql.CloneExpr(f.Expr), Ref: p.schema.FieldsRef()[i]})
	}
}

func (p *LogicalHttpSenderHint) Clone() hybridqp.QueryNode {
	clone := &LogicalHttpSenderHint{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalHttpSenderHint) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalHttpSenderHint) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalHttpSenderHint) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalHttpSenderHint) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalHttpSenderHint) String() string {
	return GetTypeName(p)
}

func (p *LogicalHttpSenderHint) Type() string {
	return GetType(p)
}

func (p *LogicalHttpSenderHint) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalHttpSenderHint) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalHttpSenderHint) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalHttpSenderHint) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalHttpSenderHint) Dummy() bool {
	return false
}

type QueryNodeStack []hybridqp.QueryNode

func NewQueryNodeStack(capacity int) *QueryNodeStack {
	stack := make(QueryNodeStack, 0, capacity)
	return &stack
}

func (s *QueryNodeStack) Empty() bool {
	return len(*s) <= 0
}

func (s *QueryNodeStack) Size() int {
	return len(*s)
}

func (s *QueryNodeStack) Push(node hybridqp.QueryNode) {
	*s = append(*s, node)
}

func (s *QueryNodeStack) Peek() hybridqp.QueryNode {
	return (*s)[len(*s)-1]
}

func (s *QueryNodeStack) Pop() hybridqp.QueryNode {
	last := len(*s) - 1
	node := (*s)[last]
	*s = (*s)[:last]

	return node
}

type LogicalPlanBuilder interface {
	Build() (hybridqp.QueryNode, error)
	Push(hybridqp.QueryNode) LogicalPlanBuilder
	Aggregate() LogicalPlanBuilder
	SlidingWindow() LogicalPlanBuilder
	CountDistinct() LogicalPlanBuilder
	Limit(parameters LimitTransformParameters) LogicalPlanBuilder
	Filter() LogicalPlanBuilder
	Merge() LogicalPlanBuilder
	SortMerge() LogicalPlanBuilder
	SortAppend() LogicalPlanBuilder
	Dedupe() LogicalPlanBuilder
	Interval() LogicalPlanBuilder
	IndexScan() LogicalPlanBuilder
	FilterBlank() LogicalPlanBuilder
	Fill() LogicalPlanBuilder
	Reader() LogicalPlanBuilder
	GroupBy() LogicalPlanBuilder
	OrderBy() LogicalPlanBuilder
	SubQuery() LogicalPlanBuilder
	TagSubset() LogicalPlanBuilder
	Project() LogicalPlanBuilder
	HttpSender() LogicalPlanBuilder
	Exchange(eType ExchangeType, eTraits []hybridqp.Trait) LogicalPlanBuilder
	CreateSeriesPlan() (hybridqp.QueryNode, error)
	CreateMeasurementPlan(hybridqp.QueryNode) (hybridqp.QueryNode, error)
	CreateShardPlan(hybridqp.QueryNode) (hybridqp.QueryNode, error)
	CreateNodePlan(hybridqp.QueryNode, []hybridqp.Trait) (hybridqp.QueryNode, error)
	CreateLimit(hybridqp.QueryNode) (hybridqp.QueryNode, error)
	CreateAggregate(hybridqp.QueryNode) (hybridqp.QueryNode, error)
}

type ExprRewriteRule interface {
	Rewrite(hybridqp.Catalog)
	String() string
}

type ExprRewriteRules []ExprRewriteRule

func (rs ExprRewriteRules) Rewrite(schema hybridqp.Catalog) {
	for _, r := range rs {
		r.Rewrite(schema)
	}
}

type LogicalPlanBuilderImpl struct {
	schema hybridqp.Catalog
	stack  QueryNodeStack
	rules  ExprRewriteRules
	err    error
}

func NewLogicalPlanBuilderImpl(schema hybridqp.Catalog) *LogicalPlanBuilderImpl {
	return &LogicalPlanBuilderImpl{
		schema: schema,
		stack:  nil,
		err:    nil,
	}
}

func (b *LogicalPlanBuilderImpl) Build() (hybridqp.QueryNode, error) {
	if b.err != nil {
		return nil, b.err
	}

	if b.stack.Size() == 0 {
		return nil, errors.New("no plans in builder")
	}

	if b.stack.Size() > 1 {
		return nil, errors.New("more than one plan in builder")
	}

	return b.stack.Pop(), nil
}

func (b *LogicalPlanBuilderImpl) Push(node hybridqp.QueryNode) LogicalPlanBuilder {
	b.stack.Push(node)
	return b
}

func (b *LogicalPlanBuilderImpl) Aggregate() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalAggregate(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) TagSetAggregate() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalTagSetAggregate(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) SlidingWindow() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalSlidingWindow(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) CountDistinct() LogicalPlanBuilder {
	if b.schema.CountDistinct() != nil {
		last := b.stack.Pop()
		plan := NewCountDistinctAggregate(last, b.schema)
		b.stack.Push(plan)
	}
	return b
}

func (b *LogicalPlanBuilderImpl) Limit(para LimitTransformParameters) LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalLimit(last, b.schema, para)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) Filter() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalFilter(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) Rewrite() LogicalPlanBuilder {
	return b
}

func (b *LogicalPlanBuilderImpl) Merge() LogicalPlanBuilder {
	reWrite := true
	for i := range b.schema.Calls() {
		if mergeCall[b.schema.Calls()[i].Name] {
			reWrite = false
		}
	}
	if reWrite {
		b.rules.Rewrite(b.schema)
	}

	inputs := make([]hybridqp.QueryNode, 0, b.stack.Size())
	for {
		if b.stack.Empty() {
			break
		}

		inputs = append(inputs, b.stack.Pop())
	}

	if len(inputs) == 0 {
		return b
	}

	if len(inputs) == 1 {
		b.stack.Push(inputs[0])
		return b
	}
	var plan LogicalPlan
	if len(b.schema.Calls()) != 0 {
		plan = NewLogicalMerge(inputs, b.schema)
	} else {
		plan = NewLogicalSortMerge(inputs, b.schema)
	}

	b.stack.Push(plan)

	return b
}

func (b *LogicalPlanBuilderImpl) SortMerge() LogicalPlanBuilder {
	b.rules.Rewrite(b.schema)

	inputs := make([]hybridqp.QueryNode, 0, b.stack.Size())
	for {
		if b.stack.Empty() {
			break
		}

		inputs = append(inputs, b.stack.Pop())
	}

	if len(inputs) == 0 {
		return b
	}

	if len(inputs) == 1 {
		b.stack.Push(inputs[0])
		return b
	}

	plan := NewLogicalSortMerge(inputs, b.schema)
	b.stack.Push(plan)

	return b
}

func (b *LogicalPlanBuilderImpl) SortAppend() LogicalPlanBuilder {
	b.rules.Rewrite(b.schema)

	inputs := make([]hybridqp.QueryNode, 0, b.stack.Size())
	for {
		if b.stack.Empty() {
			break
		}

		inputs = append(inputs, b.stack.Pop())
	}

	if len(inputs) == 0 {
		return b
	}

	if len(inputs) == 1 {
		b.stack.Push(inputs[0])
		return b
	}

	plan := NewLogicalSortMerge(inputs, b.schema)
	b.stack.Push(plan)

	return b
}

func (b *LogicalPlanBuilderImpl) Dedupe() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalDedupe(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) Interval() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalInterval(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) IndexScan() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalIndexScan(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) FilterBlank() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalFilterBlank(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) Fill() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalFill(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) Series() LogicalPlanBuilder {
	plan := NewLogicalSeries(b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) Reader() LogicalPlanBuilder {
	var last hybridqp.QueryNode
	if !b.stack.Empty() {
		last = b.stack.Pop()
	}
	plan := NewLogicalReader(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) GroupBy() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalGroupBy(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) OrderBy() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalOrderBy(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) SubQuery() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalSubQuery(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) TagSubset() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalTagSubset(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) Project() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalProject(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) HttpSender() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalHttpSender(last, b.schema)
	b.stack.Push(plan)
	return b
}
func (b *LogicalPlanBuilderImpl) HttpSenderHint() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalHttpSenderHint(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) Exchange(eType ExchangeType, eTraits []hybridqp.Trait) LogicalPlanBuilder {
	last := b.stack.Pop()

	plan := NewLogicalExchange(last, eType, eTraits, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) CreateScanPlan(mstPlan hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if mstPlan == nil {
		return nil, nil
	}
	b.Push(mstPlan)
	b.IndexScan()
	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateSeriesPlan() (hybridqp.QueryNode, error) {
	if b.schema.MatchPreAgg() && b.schema.Options().GetHintType() != hybridqp.ExactStatisticQuery {
		return nil, nil
	}
	b.Series()

	if GetEnableFileCursor() && b.schema.HasInSeriesAgg() {
		if b.schema.HasCall() && b.schema.CanCallsPushdown() && !b.schema.ContainSeriesIgnoreCall() {
			b.TagSetAggregate()
		}
	}

	b.Exchange(SERIES_EXCHANGE, nil)

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateMeasurementPlan(seriesPlan hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if seriesPlan != nil {
		b.Push(seriesPlan)
	}

	b.Reader()

	b.Exchange(READER_EXCHANGE, nil)

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateShardPlan(scanPlan hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if scanPlan == nil {
		return nil, nil
	}

	b.Push(scanPlan)

	b.Exchange(SHARD_EXCHANGE, nil)

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateNodePlan(shardPlan hybridqp.QueryNode, eTraits []hybridqp.Trait) (hybridqp.QueryNode, error) {
	if shardPlan == nil {
		return nil, nil
	}

	b.Push(shardPlan)

	b.Exchange(NODE_EXCHANGE, eTraits)

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateAggregate(input hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if input == nil {
		return nil, nil
	}

	b.Push(input)

	if b.schema.HasCall() {
		b.Aggregate()
	}

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateSlideWindow(input hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if input == nil {
		return nil, nil
	}

	b.Push(input)

	if b.schema.HasSlidingWindowCall() {
		b.SlidingWindow()
	}

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateInterval(input hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if input == nil {
		return nil, nil
	}

	b.Push(input)

	if b.schema.HasInterval() {
		b.Interval()
	}

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateProject(input hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if input == nil {
		return nil, nil
	}

	b.Push(input)

	b.Project()

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateLimit(input hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if input == nil {
		return nil, nil
	}

	b.Push(input)

	if b.schema.HasLimit() {
		limitType := b.schema.LimitType()
		limit, offset := b.schema.LimitAndOffset()
		b.Limit(LimitTransformParameters{Limit: limit + offset, Offset: 0, LimitType: limitType})
	}

	return b.Build()
}

type LogicalPlanRewriter interface {
	rewrite()
}

type LogicalPlanRewriterImpl struct {
}

func (w *LogicalPlanRewriterImpl) rewrite() {}

type LogicalPlanVisitor interface {
	Visit(hybridqp.QueryNode) LogicalPlanVisitor
}

func Walk(v LogicalPlanVisitor, plan hybridqp.QueryNode) {
	if plan == nil {
		return
	}

	if v = v.Visit(plan); v == nil {
		return
	}

	for _, p := range plan.Children() {
		Walk(v, p)
	}
}

type LogicalDummyShard struct {
	readers [][]interface{}
	LogicalPlanBase
}

func NewLogicalDummyShard(readers [][]interface{}) *LogicalDummyShard {
	return &LogicalDummyShard{
		readers: readers,
	}
}

func (p *LogicalDummyShard) Readers() [][]interface{} {
	return p.readers
}

func (p *LogicalDummyShard) Clone() hybridqp.QueryNode {
	clone := &LogicalDummyShard{}
	*clone = *p
	return clone
}

func (p *LogicalDummyShard) Children() []hybridqp.QueryNode {
	return nil
}

func (p *LogicalDummyShard) ReplaceChildren(children []hybridqp.QueryNode) {
}

func (p *LogicalDummyShard) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
}

func (p *LogicalDummyShard) ExplainIterms(writer LogicalPlanWriter) {
}

func (p *LogicalDummyShard) Explain(writer LogicalPlanWriter) {
}

func (p *LogicalDummyShard) String() string {
	return GetTypeName(p)
}

func (p *LogicalDummyShard) Type() string {
	return GetType(p)
}

func (p *LogicalDummyShard) Digest() string {
	return GetTypeName(p)
}

type LogicalIndexScan struct {
	input hybridqp.QueryNode
	LogicalPlanBase
}

func NewLogicalIndexScan(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalIndexScan {
	project := &LogicalIndexScan{
		input: input,
		LogicalPlanBase: LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
		},
	}

	project.init()

	return project
}

func (p *LogicalIndexScan) DeriveOperations() {
	p.init()
}

func (p *LogicalIndexScan) init() {
	p.ForwardInit(p.input)
}

func (p *LogicalIndexScan) Clone() hybridqp.QueryNode {
	clone := &LogicalIndexScan{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalIndexScan) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.input}
}

func (p *LogicalIndexScan) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) > 1 {
		panic("only one child in logical interval")
	}
	p.input = children[0]
}

func (p *LogicalIndexScan) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 0 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	p.input = child
}

func (p *LogicalIndexScan) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalIndexScan) String() string {
	return GetTypeName(p)
}

func (p *LogicalIndexScan) Type() string {
	return GetType(p)
}

func (p *LogicalIndexScan) Digest() string {
	return fmt.Sprintf("%s[%d]", GetTypeName(p), p.input.ID())
}

func (p *LogicalIndexScan) RowDataType() hybridqp.RowDataType {
	return p.rt
}

func (p *LogicalIndexScan) RowExprOptions() []hybridqp.ExprOptions {
	return p.ops
}

func (p *LogicalIndexScan) Schema() hybridqp.Catalog {
	return p.schema
}

func (p *LogicalIndexScan) Dummy() bool {
	return false
}
