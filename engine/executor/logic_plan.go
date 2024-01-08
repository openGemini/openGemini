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
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
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
	_ LogicalPlan = &LogicalFullJoin{}
	_ LogicalPlan = &LogicalSort{}
	_ LogicalPlan = &LogicalHashMerge{}
	_ LogicalPlan = &LogicalSparseIndexScan{}
	_ LogicalPlan = &LogicalColumnStoreReader{}
	_ LogicalPlan = &LogicalJoin{}
)

var mergeCall = map[string]bool{"percentile": true, "rate": true, "irate": true,
	"absent": true, "stddev": true, "mode": true, "median": true, "sample": true,
	"percentile_approx": true,
}

var sortedMergeCall = map[string]bool{
	"difference": true, "non_negative_difference": true,
	"derivative": true, "non_negative_derivative": true,
	"elapsed": true, "integral": true, "moving_average": true, "cumulative_sum": true,
}

type AggLevel uint8

const (
	UnknownLevel AggLevel = iota
	SourceLevel
	MiddleLevel
	SinkLevel
)

var (
	enableFileCursor bool = true
)

func EnableFileCursor(en bool) {
	enableFileCursor = en
}

func GetEnableFileCursor() bool {
	return enableFileCursor
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

type LogicalIncAgg struct {
	aggType    int
	calls      map[string]*influxql.Call
	callsOrder []string
	LogicalPlanSingle
}

func NewLogicalIncAgg(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalIncAgg {
	agg := &LogicalIncAgg{
		calls:             make(map[string]*influxql.Call),
		callsOrder:        nil,
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	agg.callsOrder = make([]string, 0, len(agg.schema.Calls()))
	var ok bool
	for k, c := range agg.schema.Calls() {
		agg.calls[k], ok = influxql.CloneExpr(c).(*influxql.Call)
		if !ok {
			logger.GetLogger().Warn("NewLogicalIncAgg call type isn't *influxql.Call")
			return nil
		}
		agg.callsOrder = append(agg.callsOrder, k)
	}
	sort.Strings(agg.callsOrder)
	agg.init()
	return agg
}

func (p *LogicalIncAgg) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	agg := NewLogicalIncAgg(inputs[0], schema)
	return agg
}

func (p *LogicalIncAgg) DeriveOperations() {
	p.init()
}

func (p *LogicalIncAgg) init() {
	refs := p.inputs[0].RowDataType().MakeRefs()
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

func (p *LogicalIncAgg) ForwardCallArgs() {
	for k, call := range p.calls {
		if len(call.Args) > 0 {
			ref := p.schema.Mapping()[p.schema.Calls()[k]]
			p.digest = false
			call.Args[0] = &ref
		}
	}
}

func (p *LogicalIncAgg) CountToSum() {
	for _, call := range p.calls {
		if call.Name == "count" {
			call.Name = "sum"
			p.digest = false
		}
	}
}

func (p *LogicalIncAgg) Clone() hybridqp.QueryNode {
	clone := &LogicalIncAgg{}
	*clone = *p
	clone.calls = make(map[string]*influxql.Call)
	var ok bool
	for k, c := range p.calls {
		clone.calls[k], ok = influxql.CloneExpr(c).(*influxql.Call)
		if !ok {
			logger.GetLogger().Warn("LogicalIncAgg clone: type isn't *influxql.Call")
		}
	}
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalIncAgg) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalIncAgg) Type() string {
	return GetType(p)
}

func (p *LogicalIncAgg) Digest() string {
	if !p.digest {
		p.digest = true
		buildDigest(&p.digestBuff, p.String(), p.aggType, p.inputs[0].ID(),
			p.schema.Fields(), p.calls, p.callsOrder)
	}
	return p.digestBuff.String()
}

type LogicalIncHashAgg struct {
	calls      map[string]*influxql.Call
	callsOrder []string
	LogicalPlanSingle
}

func NewLogicalIncHashAgg(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalIncHashAgg {
	agg := &LogicalIncHashAgg{
		calls:             make(map[string]*influxql.Call),
		callsOrder:        nil,
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	agg.callsOrder = make([]string, 0, len(agg.schema.Calls()))
	var ok bool
	for k, c := range agg.schema.Calls() {
		agg.calls[k], ok = influxql.CloneExpr(c).(*influxql.Call)
		if !ok {
			logger.GetLogger().Warn("NewLogicalIncHashAgg call type isn't *influxql.Call")
			return nil
		}
		agg.callsOrder = append(agg.callsOrder, k)
	}
	sort.Strings(agg.callsOrder)

	agg.init()

	return agg
}

func (p *LogicalIncHashAgg) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	agg := NewLogicalIncHashAgg(inputs[0], schema)
	return agg
}

func (p *LogicalIncHashAgg) DeriveOperations() {
	p.init()
}

func (p *LogicalIncHashAgg) init() {
	refs := p.inputs[0].RowDataType().MakeRefs()
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

func (p *LogicalIncHashAgg) ForwardCallArgs() {
	for k, call := range p.calls {
		if len(call.Args) > 0 {
			ref := p.schema.Mapping()[p.schema.Calls()[k]]
			p.digest = false
			call.Args[0] = &ref
		}
	}
}

func (p *LogicalIncHashAgg) CountToSum() {
	for _, call := range p.calls {
		if call.Name == "count" {
			call.Name = "sum"
			p.digest = false
		}
	}
}

func (p *LogicalIncHashAgg) Clone() hybridqp.QueryNode {
	clone := &LogicalIncHashAgg{}
	*clone = *p
	clone.calls = make(map[string]*influxql.Call)
	var ok bool
	for k, c := range p.calls {
		clone.calls[k], ok = influxql.CloneExpr(c).(*influxql.Call)
		if !ok {
			logger.GetLogger().Warn("LogicalIncHashAgg clone: type isn't *influxql.Call")
		}
	}
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalIncHashAgg) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalIncHashAgg) Type() string {
	return GetType(p)
}

func (p *LogicalIncHashAgg) Digest() string {
	if !p.digest {
		p.digest = true
	} else {
		return p.digestBuff.String()
	}
	p.digestBuff.Reset()
	p.digestBuff.WriteString(p.String())
	p.digestBuff.WriteString("[")
	p.digestBuff.WriteString(strconv.FormatUint(p.inputs[0].ID(), 10))
	p.digestBuff.WriteString("]")
	p.digestBuff.WriteString("(")
	p.digestBuff.WriteString(p.schema.Fields().String())
	p.digestBuff.WriteString(")")
	p.digestBuff.WriteString("(")

	firstCall := true
	for _, order := range p.callsOrder {
		call := p.calls[order]
		if firstCall {
			call.WriteString(&p.digestBuff)
			firstCall = false
			continue
		}
		p.digestBuff.WriteString(",")
		call.WriteString(&p.digestBuff)
	}
	p.digestBuff.WriteString(")")
	return p.digestBuff.String()
}

type LogicalAggregate struct {
	isCountDistinct      bool
	isPercentileOGSketch bool
	aggType              int
	calls                map[string]*influxql.Call
	callsOrder           []string
	LogicalPlanSingle
}

func NewCountDistinctAggregate(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalAggregate {
	agg := NewLogicalAggregate(input, schema)
	agg.isCountDistinct = true
	return agg
}

func NewLogicalAggregate(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalAggregate {
	agg := &LogicalAggregate{
		calls:                make(map[string]*influxql.Call),
		callsOrder:           make([]string, 0, len(schema.Calls())),
		isCountDistinct:      false,
		isPercentileOGSketch: schema.HasPercentileOGSketch(),
		LogicalPlanSingle:    *NewLogicalPlanSingle(input, schema),
	}

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
	agg := NewLogicalAggregate(input, schema)
	if agg == nil {
		return nil
	}
	agg.aggType = tagSetAgg

	return agg
}

func (p *LogicalAggregate) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	agg := NewLogicalAggregate(inputs[0], schema)
	if p.aggType == tagSetAgg {
		agg.aggType = tagSetAgg
	} else if schema.CountDistinct() != nil {
		agg.isCountDistinct = true
	}
	return agg
}

func (p *LogicalAggregate) inferAggLevel() AggLevel {
	_, ok := p.Children()[0].(*HeuVertex)
	if !ok {
		exchange, ok := p.Children()[0].(*LogicalExchange)
		if ok {
			if exchange.eType == NODE_EXCHANGE || exchange.eType == SINGLE_SHARD_EXCHANGE {
				return SinkLevel
			}
			return MiddleLevel
		}
		_, ok = p.Children()[0].(*LogicalReader)
		if ok {
			return SourceLevel
		}
		return SourceLevel
	}
	exchange, ok := p.Children()[0].(*HeuVertex).node.(*LogicalExchange)
	if ok {
		if exchange.eType == NODE_EXCHANGE || exchange.eType == SINGLE_SHARD_EXCHANGE {
			return SinkLevel
		}
		return MiddleLevel
	}
	_, ok = p.Children()[0].(*HeuVertex).node.(*LogicalReader)
	if ok {
		return SourceLevel
	}
	return SourceLevel
}

func (p *LogicalAggregate) getOGSketchOp(k string, level AggLevel, cc map[string]*hybridqp.OGSketchCompositeOperator) *influxql.Call {
	switch level {
	case SourceLevel:
		return cc[k].GetInsertOp()
	case MiddleLevel:
		return cc[k].GetMergeOp()
	case SinkLevel:
		return cc[k].GetQueryPerOp()
	default:
		return cc[k].GetQueryPerOp()
	}
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
	inrefs := p.inputs[0].RowDataType().MakeRefs()
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
	var level AggLevel
	var cc map[string]*hybridqp.OGSketchCompositeOperator
	if p.isPercentileOGSketch {
		level = p.inferAggLevel()
		cc = p.schema.CompositeCall()
	}
	refs := p.inputs[0].RowDataType().MakeRefs()

	m := make(map[string]influxql.VarRef)
	for _, ref := range refs {
		m[ref.Val] = ref
	}

	mc := make(map[string]hybridqp.ExprOptions)

	for k, c := range p.calls {
		var ref influxql.VarRef
		if p.isPercentileOGSketch && c.Name == PercentileOGSketch {
			c = p.getOGSketchOp(k, level, cc)
			ref = p.schema.Mapping()[c]
		} else {
			ref = p.schema.Mapping()[p.schema.Calls()[k]]
		}
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
	if p.isPercentileOGSketch {
		return
	}
	for k, call := range p.calls {
		if len(call.Args) > 0 {
			ref := p.schema.Mapping()[p.schema.Calls()[k]]
			p.digest = false
			call.Args[0] = &ref
		}
	}
}

func (p *LogicalAggregate) CountToSum() {
	for _, call := range p.calls {
		if call.Name == "count" {
			call.Name = "sum"
			p.digest = false
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

func (p *LogicalAggregate) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalAggregate) Type() string {
	return GetType(p)
}

func (p *LogicalAggregate) Digest() string {
	if !p.digest {
		p.digest = true
		buildDigest(&p.digestBuff, p.String(), p.aggType, p.inputs[0].ID(),
			p.schema.Fields(), p.calls, p.callsOrder)
	}
	return p.digestBuff.String()
}

type LogicalSlidingWindow struct {
	calls      map[string]*influxql.Call
	callsOrder []string
	LogicalPlanSingle
}

func NewLogicalSlidingWindow(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSlidingWindow {
	agg := &LogicalSlidingWindow{
		calls:             make(map[string]*influxql.Call),
		callsOrder:        make([]string, 0, len(schema.Calls())),
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

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

// impl me
func (p *LogicalSlidingWindow) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalSlidingWindow) DeriveOperations() {
	p.init()
}

func (p *LogicalSlidingWindow) init() {
	refs := p.inputs[0].RowDataType().MakeRefs()

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
			p.digest = false
			call.Args[0] = &ref
		}
	}
}

func (p *LogicalSlidingWindow) CountToSum() {
	for _, call := range p.calls {
		if call.Name == "count" {
			call.Name = "sum"
			p.digest = false
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

func (p *LogicalSlidingWindow) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSlidingWindow) Type() string {
	return GetType(p)
}

func (p *LogicalSlidingWindow) Digest() string {
	if !p.digest {
		p.digest = true
		buildDigest(&p.digestBuff, p.String(), 0, p.inputs[0].ID(),
			p.schema.Fields(), p.calls, p.callsOrder)
	}
	return p.digestBuff.String()
}

type LogicalFill struct {
	LogicalPlanSingle
}

func NewLogicalFill(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalFill {
	fill := &LogicalFill{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	fill.init()

	return fill
}

func (p *LogicalFill) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return NewLogicalFill(inputs[0], schema)
}

func (p *LogicalFill) DeriveOperations() {
	p.init()
}

func (p *LogicalFill) init() {
	p.InitRef(p.inputs[0])
}

func (p *LogicalFill) Clone() hybridqp.QueryNode {
	clone := &LogicalFill{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalFill) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalFill) Type() string {
	return GetType(p)
}

func (p *LogicalFill) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalLimit struct {
	LimitPara LimitTransformParameters
	LogicalPlanSingle
}

func NewLogicalLimit(input hybridqp.QueryNode, schema hybridqp.Catalog, parameters LimitTransformParameters) *LogicalLimit {
	limit := &LogicalLimit{
		LimitPara:         parameters,
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	limit.init()

	limit.SetHoltWintersType(true, limit.rt.Fields())
	return limit
}

func (p *LogicalLimit) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	limitType := schema.LimitType()
	limit, offset := schema.LimitAndOffset()
	return NewLogicalLimit(inputs[0], schema, LimitTransformParameters{Limit: limit, Offset: offset, LimitType: limitType})
}

func (p *LogicalLimit) DeriveOperations() {
	p.init()
}

func (p *LogicalLimit) init() {
	p.ForwardInit(p.inputs[0])
}

func (p *LogicalLimit) Clone() hybridqp.QueryNode {
	clone := &LogicalLimit{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalLimit) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalLimit) Type() string {
	return GetType(p)
}

func (p *LogicalLimit) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalFilter struct {
	LogicalPlanSingle
}

func NewLogicalFilter(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalFilter {
	filter := &LogicalFilter{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	filter.init()

	return filter
}

// impl me
func (p *LogicalFilter) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalFilter) DeriveOperations() {
	p.init()
}

func (p *LogicalFilter) init() {
	p.InitRef(p.inputs[0])
}

func (p *LogicalFilter) Clone() hybridqp.QueryNode {
	clone := &LogicalFilter{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalFilter) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalFilter) Type() string {
	return GetType(p)
}

func (p *LogicalFilter) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalMerge struct {
	LogicalPlanMulti
}

func NewLogicalMerge(inputs []hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalMerge {
	merge := &LogicalMerge{
		LogicalPlanMulti: LogicalPlanMulti{LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
			inputs: inputs,
		}},
	}

	merge.init()

	return merge
}

// impl me
func (p *LogicalMerge) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalMerge) DeriveOperations() {
	p.init()
}

func (p *LogicalMerge) init() {
	if !ValidateFieldsFromPlans(p.inputs) {
		panic("validate all input of merge failed")
	}

	input := p.inputs[0]

	p.InitRef(input)
}

func (p *LogicalMerge) Clone() hybridqp.QueryNode {
	clone := &LogicalMerge{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalMerge) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalMerge) Type() string {
	return GetType(p)
}

func (p *LogicalMerge) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalSortMerge struct {
	LogicalPlanMulti
}

func NewLogicalSortMerge(inputs []hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSortMerge {
	merge := &LogicalSortMerge{
		LogicalPlanMulti: LogicalPlanMulti{LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
			inputs: inputs,
		}},
	}

	merge.init()

	return merge
}

// impl me
func (p *LogicalSortMerge) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalSortMerge) DeriveOperations() {
	p.init()
}

func (p *LogicalSortMerge) init() {
	if !ValidateFieldsFromPlans(p.inputs) {
		panic("validate all input of sort merge failed")
	}

	input := p.inputs[0]

	p.InitRef(input)
}

func (p *LogicalSortMerge) Clone() hybridqp.QueryNode {
	clone := &LogicalSortMerge{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalSortMerge) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSortMerge) Type() string {
	return GetType(p)
}

func (p *LogicalSortMerge) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalSortAppend struct {
	LogicalPlanMulti
}

func NewLogicalSortAppend(inputs []hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSortAppend {
	merge := &LogicalSortAppend{
		LogicalPlanMulti: LogicalPlanMulti{LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
			inputs: inputs,
		}},
	}

	merge.init()

	return merge
}

// impl me
func (p *LogicalSortAppend) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalSortAppend) DeriveOperations() {
	p.init()
}

func (p *LogicalSortAppend) init() {
	if !ValidateFieldsFromPlans(p.inputs) {
		panic("validate all input of sort merge failed")
	}

	input := p.inputs[0]

	p.InitRef(input)
}

func (p *LogicalSortAppend) Clone() hybridqp.QueryNode {
	clone := &LogicalSortAppend{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalSortAppend) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSortAppend) Type() string {
	return GetType(p)
}

func (p *LogicalSortAppend) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalJoin struct {
	LogicalPlanMulti
}

func NewLogicalJoin(inputs []hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalJoin {
	join := &LogicalJoin{
		LogicalPlanMulti: LogicalPlanMulti{LogicalPlanBase{
			id:     hybridqp.GenerateNodeId(),
			schema: schema,
			rt:     nil,
			ops:    nil,
			inputs: inputs,
		}},
	}
	join.init()
	return join
}

// impl me
func (p *LogicalJoin) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalJoin) DeriveOperations() {
	p.init()
}

func (p *LogicalJoin) init() {
	if !ValidateFieldsFromPlans(p.inputs) {
		panic("validate all input of join failed")
	}
	input := p.inputs[0]
	p.InitRef(input)
}

func (p *LogicalJoin) Clone() hybridqp.QueryNode {
	clone := &LogicalJoin{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalJoin) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalJoin) Type() string {
	return GetType(p)
}

func (p *LogicalJoin) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalDedupe struct {
	LogicalPlanSingle
}

// NewLogicalDedupe unused
func NewLogicalDedupe(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalDedupe {
	dedupe := &LogicalDedupe{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	dedupe.init()

	return dedupe
}

// impl me
func (p *LogicalDedupe) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalDedupe) init() {
	p.InitRef(p.inputs[0])
}

func (p *LogicalDedupe) DeriveOperations() {}

func (p *LogicalDedupe) Clone() hybridqp.QueryNode {
	clone := &LogicalDedupe{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalDedupe) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalDedupe) Type() string {
	return GetType(p)
}

func (p *LogicalDedupe) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalInterval struct {
	LogicalPlanSingle
}

func NewLogicalInterval(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalInterval {
	interval := &LogicalInterval{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	interval.init()

	return interval
}

func (p *LogicalInterval) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return NewLogicalInterval(inputs[0], schema)
}

func (p *LogicalInterval) DeriveOperations() {
	p.init()
}

func (p *LogicalInterval) init() {
	p.InitRef(p.inputs[0])
}

func (p *LogicalInterval) Clone() hybridqp.QueryNode {
	clone := &LogicalInterval{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalInterval) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalInterval) Type() string {
	return GetType(p)
}

func (p *LogicalInterval) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalFilterBlank struct {
	LogicalPlanSingle
}

func NewLogicalFilterBlank(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalFilterBlank {
	interval := &LogicalFilterBlank{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	interval.init()

	return interval
}

// impl me
func (p *LogicalFilterBlank) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalFilterBlank) DeriveOperations() {
	p.init()
}

func (p *LogicalFilterBlank) init() {
	p.ForwardInit(p.inputs[0])
}

func (p *LogicalFilterBlank) Clone() hybridqp.QueryNode {
	clone := &LogicalFilterBlank{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalFilterBlank) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalFilterBlank) Type() string {
	return GetType(p)
}

func (p *LogicalFilterBlank) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalAlign struct {
	LogicalPlanSingle
}

func NewLogicalAlign(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalAlign {
	align := &LogicalAlign{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	align.init()

	return align
}

// impl me
func (p *LogicalAlign) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalAlign) DeriveOperations() {
	p.init()
}

func (p *LogicalAlign) init() {
	p.InitRef(p.inputs[0])
}

func (p *LogicalAlign) Clone() hybridqp.QueryNode {
	clone := &LogicalAlign{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalAlign) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalAlign) Type() string {
	return GetType(p)
}

func (p *LogicalAlign) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
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

// impl me
func (p *LogicalMst) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
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

func (p *LogicalMst) ReplaceChildren(children []hybridqp.QueryNode) {}

func (p *LogicalMst) ReplaceChild(ordinal int, child hybridqp.QueryNode) {}

func (p *LogicalMst) Explain(writer LogicalPlanWriter) {
	writer.Explain(p)
}

func (p *LogicalMst) Type() string {
	return GetType(p)
}

func (p *LogicalMst) Digest() string {
	return p.String()
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

func (p *LogicalSeries) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return NewLogicalSeries(schema)
}

func (p *LogicalSeries) DeriveOperations() {
	p.init()
}

func (p *LogicalSeries) init() {
	p.initForSink()
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

func (p *LogicalSeries) Type() string {
	return GetType(p)
}

func (p *LogicalSeries) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.ID())
	return string(p.digestName)
}

func explainIterms(writer LogicalPlanWriter, id uint64, mstName string, dimensions []string) {
	var builder strings.Builder
	for i, d := range dimensions {
		if i != 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(d)
	}
	writer.Item("dimensions", builder.String())
	writer.Item("mstName", mstName)
	writer.Item("ID", id)
}

type LogicalReader struct {
	cursor         []interface{}
	hasPreAgg      bool
	dimensions     []string
	mstName        string
	oneReaderState bool
	LogicalPlanSingle
}

func NewLogicalReader(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalReader {
	reader := &LogicalReader{
		cursor:            nil,
		hasPreAgg:         false,
		dimensions:        schema.Options().GetOptDimension(),
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	if schema.Sources() != nil {
		reader.mstName = schema.Sources()[0].String()
	} else if schema.Options().(*query.ProcessorOptions).Sources != nil {
		reader.mstName = schema.Options().(*query.ProcessorOptions).Sources[0].String()
	}

	reader.init()

	return reader
}

func (p *LogicalReader) SetOneReaderState(state bool) {
	p.oneReaderState = state
}

func (p *LogicalReader) GetOneReaderState() bool {
	return p.oneReaderState
}

func (p *LogicalReader) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	if len(inputs) == 0 {
		return NewLogicalReader(nil, schema)
	}
	return NewLogicalReader(inputs[0], schema)
}

func (p *LogicalReader) MstName() string {
	return p.mstName
}

func (p *LogicalReader) DeriveOperations() {
	p.init()
}

func (p *LogicalReader) init() {
	if p.inputs == nil || p.inputs[0] == nil {
		p.initPreAgg()
	} else {
		p.ForwardInit(p.inputs[0])
	}
}

func (p *LogicalReader) HasPreAgg() bool {
	return p.hasPreAgg
}

func (p *LogicalReader) initPreAgg() {
	(&p.LogicalPlanBase).PreAggInit()
}

func (p *LogicalReader) Clone() hybridqp.QueryNode {
	clone := &LogicalReader{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	clone.digest = false
	return clone
}

func (p *LogicalReader) SetCursor(cursor []interface{}) {
	p.cursor = cursor
}

func (p *LogicalReader) ExplainIterms(writer LogicalPlanWriter) {
	explainIterms(writer, p.id, p.mstName, p.dimensions)
}

func (p *LogicalReader) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	p.LogicalPlanBase.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalReader) Type() string {
	return GetType(p)
}

func (p *LogicalReader) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	if p.inputs != nil {
		p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	}
	p.digestName = encoding.MarshalUint64(p.digestName, p.ID())
	return string(p.digestName)
}

func (p *LogicalReader) Cursors() []interface{} {
	return p.cursor
}

type LogicalSubQuery struct {
	LogicalPlanSingle
}

func NewLogicalSubQuery(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSubQuery {
	subQuery := &LogicalSubQuery{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	subQuery.init()

	return subQuery
}

// impl me
func (p *LogicalSubQuery) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalSubQuery) DeriveOperations() {
	p.init()
}

func (p *LogicalSubQuery) init() {
	p.initForSink()
}

func (p *LogicalSubQuery) Clone() hybridqp.QueryNode {
	clone := &LogicalSubQuery{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalSubQuery) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSubQuery) Type() string {
	return GetType(p)
}

func (p *LogicalSubQuery) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	if p.inputs != nil {
		p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	}
	return string(p.digestName)
}

type LogicalTagSubset struct {
	LogicalPlanSingle
}

// NewLogicalTagSubset unused
func NewLogicalTagSubset(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalTagSubset {
	tagSubset := &LogicalTagSubset{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	tagSubset.init()

	return tagSubset
}

// impl me
func (p *LogicalTagSubset) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalTagSubset) DeriveOperations() {}

func (p *LogicalTagSubset) init() {
	p.InitRef(p.inputs[0])
}

func (p *LogicalTagSubset) Clone() hybridqp.QueryNode {
	clone := &LogicalTagSubset{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalTagSubset) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalTagSubset) Type() string {
	return GetType(p)
}

func (p *LogicalTagSubset) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalProject struct {
	LogicalPlanSingle
}

func NewLogicalProject(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalProject {
	project := &LogicalProject{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	project.init()

	return project
}

func (p *LogicalProject) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return NewLogicalProject(inputs[0], schema)
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

func (p *LogicalProject) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalProject) Type() string {
	return GetType(p)
}

func (p *LogicalProject) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type ExchangeType uint8

const (
	UNKNOWN_EXCHANGE ExchangeType = iota
	NODE_EXCHANGE
	SHARD_EXCHANGE
	SINGLE_SHARD_EXCHANGE
	READER_EXCHANGE
	SERIES_EXCHANGE
	SEGMENT_EXCHANGE
	PARTITION_EXCHANGE
)

type ExchangeRole uint8

const (
	UNKNOWN_ROLE ExchangeRole = iota
	CONSUMER_ROLE
	PRODUCER_ROLE
)

type LogicalExchange struct {
	LogicalPlanSingle
	LogicalExchangeBase
}

func NewLogicalExchange(input hybridqp.QueryNode, eType ExchangeType, eTraits []hybridqp.Trait, schema hybridqp.Catalog) *LogicalExchange {
	exchange := &LogicalExchange{
		LogicalPlanSingle:   *NewLogicalPlanSingle(input, schema),
		LogicalExchangeBase: *NewLogicalExchangeBase(eType, CONSUMER_ROLE, eTraits),
	}
	exchange.init()

	return exchange
}

func (p *LogicalExchange) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	var exchange *LogicalExchange
	if p.eType == NODE_EXCHANGE {
		exchange = NewLogicalExchange(inputs[0], p.eType, eTrait, schema)
	} else {
		exchange = NewLogicalExchange(inputs[0], p.eType, []hybridqp.Trait{}, schema)
	}
	exchange.eRole = p.eRole
	return exchange
}

func (p *LogicalExchange) AddTrait(trait interface{}) {
	p.eTraits = append(p.eTraits, trait)
}

func (p *LogicalExchange) Schema() hybridqp.Catalog {
	return p.LogicalPlanBase.schema
}

func (p *LogicalExchange) DeriveOperations() {
	p.init()
}

func (p *LogicalExchange) init() {
	p.InitRef(p.inputs[0])
}

func (p *LogicalExchange) Clone() hybridqp.QueryNode {
	clone := &LogicalExchange{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalExchange) ExplainIterms(writer LogicalPlanWriter) {
	writer.Item("type", p.eType)
	writer.Item("traits", len(p.eTraits))
}

func (p *LogicalExchange) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalExchange) Type() string {
	return GetType(p)
}

func (p *LogicalExchange) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalGroupBy struct {
	dimensions []string
	LogicalPlanSingle
}

func NewLogicalGroupBy(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalGroupBy {
	groupby := &LogicalGroupBy{
		dimensions:        schema.Options().GetOptDimension(),
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	groupby.init()

	return groupby
}

// impl me
func (p *LogicalGroupBy) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalGroupBy) DeriveOperations() {
	p.init()
}

func (p *LogicalGroupBy) init() {
	p.ForwardInit(p.inputs[0])
}

func (p *LogicalGroupBy) Clone() hybridqp.QueryNode {
	clone := &LogicalGroupBy{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
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

func (p *LogicalGroupBy) Type() string {
	return GetType(p)
}

func (p *LogicalGroupBy) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalOrderBy struct {
	dimensions []string
	LogicalPlanSingle
}

func NewLogicalOrderBy(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalOrderBy {
	Orderby := &LogicalOrderBy{
		dimensions:        schema.Options().GetOptDimension(),
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	Orderby.init()

	return Orderby
}

// impl me
func (p *LogicalOrderBy) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalOrderBy) DeriveOperations() {
	p.init()
}

func (p *LogicalOrderBy) init() {
	p.InitRef(p.inputs[0])
}

func (p *LogicalOrderBy) Clone() hybridqp.QueryNode {
	clone := &LogicalOrderBy{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
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

func (p *LogicalOrderBy) Type() string {
	return GetType(p)
}

func (p *LogicalOrderBy) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalHttpSender struct {
	LogicalPlanSingle
}

func NewLogicalHttpSender(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalHttpSender {
	HttpSender := &LogicalHttpSender{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	HttpSender.init()

	return HttpSender
}

func (p *LogicalHttpSender) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return NewLogicalHttpSender(inputs[0], schema)
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
	p.SetHoltWintersType(true, p.rt.Fields())
}

func (p *LogicalHttpSender) Clone() hybridqp.QueryNode {
	clone := &LogicalHttpSender{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalHttpSender) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalHttpSender) Type() string {
	return GetType(p)
}

func (p *LogicalHttpSender) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalHttpSenderHint struct {
	LogicalPlanSingle
}

func NewLogicalHttpSenderHint(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalHttpSenderHint {
	HttpSender := &LogicalHttpSenderHint{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	HttpSender.init()

	return HttpSender
}

// impl me
func (p *LogicalHttpSenderHint) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
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

func (p *LogicalHttpSenderHint) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalHttpSenderHint) Type() string {
	return GetType(p)
}

func (p *LogicalHttpSenderHint) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalTarget struct {
	LogicalPlanSingle

	mst *influxql.Measurement
}

func NewLogicalTarget(input hybridqp.QueryNode, schema hybridqp.Catalog, mst *influxql.Measurement) *LogicalTarget {
	target := &LogicalTarget{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
		mst:               mst,
	}

	target.init()

	return target
}

// impl me
func (p *LogicalTarget) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalTarget) TargetMeasurement() *influxql.Measurement {
	return p.mst
}

func (p *LogicalTarget) DeriveOperations() {
	p.init()
}

func (p *LogicalTarget) init() {
	p.rt = hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "written", Type: influxql.Integer})
}

func (p *LogicalTarget) Clone() hybridqp.QueryNode {
	clone := &LogicalTarget{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalTarget) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalTarget) Type() string {
	return GetType(p)
}

func (p *LogicalTarget) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type QueryNodeStack []hybridqp.QueryNode

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
	Dedupe() LogicalPlanBuilder
	Interval() LogicalPlanBuilder
	IndexScan() LogicalPlanBuilder
	FilterBlank() LogicalPlanBuilder
	Fill() LogicalPlanBuilder
	Reader(config.EngineType) LogicalPlanBuilder
	GroupBy() LogicalPlanBuilder
	OrderBy() LogicalPlanBuilder
	SubQuery() LogicalPlanBuilder
	TagSubset() LogicalPlanBuilder
	Project() LogicalPlanBuilder
	HttpSender() LogicalPlanBuilder
	SplitGroup() LogicalPlanBuilder
	Exchange(eType ExchangeType, eTraits []hybridqp.Trait) LogicalPlanBuilder
	CreateSeriesPlan() (hybridqp.QueryNode, error)
	CreateSegmentPlan(schema hybridqp.Catalog) (hybridqp.QueryNode, error)
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

func (b *LogicalPlanBuilderImpl) IncAgg() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalIncAgg(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) IncHashAgg() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalIncHashAgg(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) TagSetAggregate() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalTagSetAggregate(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) Sort() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalSort(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) SlidingWindow() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalSlidingWindow(last, b.schema)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) HoltWinters() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalHoltWinters(last, b.schema)
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

func (b *LogicalPlanBuilderImpl) HashMerge(eType ExchangeType, eTraits []hybridqp.Trait) LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalHashMerge(last, b.schema, eType, eTraits)
	b.stack.Push(plan)
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

func (b *LogicalPlanBuilderImpl) SparseIndexScan() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalSparseIndexScan(last, b.schema)
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

func (b *LogicalPlanBuilderImpl) Reader(engineType config.EngineType) LogicalPlanBuilder {
	var last hybridqp.QueryNode
	if !b.stack.Empty() {
		last = b.stack.Pop()
	}
	var plan LogicalPlan
	switch engineType {
	case config.TSSTORE:
		plan = NewLogicalReader(last, b.schema)
	case config.COLUMNSTORE:
		plan = NewLogicalColumnStoreReader(last, b.schema)
	default:
		panic("unsupported engine type")
	}
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

func (b *LogicalPlanBuilderImpl) Target(mst *influxql.Measurement) LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalTarget(last, b.schema, mst)
	b.stack.Push(plan)
	return b
}

func (b *LogicalPlanBuilderImpl) SplitGroup() LogicalPlanBuilder {
	last := b.stack.Pop()
	plan := NewLogicalSplitGroup(last, b.schema)
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

func (b *LogicalPlanBuilderImpl) CreateSparseIndexScanPlan(plan hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if plan == nil {
		return nil, nil
	}
	b.Push(plan)
	b.SparseIndexScan()
	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateScanPlan(mstPlan hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if mstPlan == nil {
		return nil, nil
	}
	b.Push(mstPlan)
	b.IndexScan()
	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateSegmentPlan(schema hybridqp.Catalog) (hybridqp.QueryNode, error) {
	b.Reader(config.COLUMNSTORE)
	if schema.HasCall() && schema.CanAggPushDown() {
		b.Exchange(READER_EXCHANGE, nil)
	}
	if len(schema.Options().GetDimensions()) > 0 && (!schema.HasCall() || schema.HasCall() && !schema.CanAggPushDown()) {
		b.Exchange(READER_EXCHANGE, nil)
	}
	b.Exchange(SEGMENT_EXCHANGE, nil)

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateSeriesPlan() (hybridqp.QueryNode, error) {
	if b.schema.MatchPreAgg() && b.schema.Options().GetHintType() != hybridqp.ExactStatisticQuery {
		return nil, nil
	}
	b.Series()

	if GetEnableFileCursor() && b.schema.HasOptimizeAgg() {
		if b.schema.HasCall() && b.schema.CanCallsPushdown() && !b.schema.ContainSeriesIgnoreCall() {
			b.TagSetAggregate()
		}
	}

	b.Exchange(SERIES_EXCHANGE, nil)

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateColStoreCursorPlan() (hybridqp.QueryNode, error) {
	b.Series()

	if b.schema.CanSeqAggPushDown() {
		b.TagSetAggregate()
	}

	b.Exchange(SERIES_EXCHANGE, nil)

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateColStoreReaderPlan(seriesPlan hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if seriesPlan != nil {
		b.Push(seriesPlan)
	}

	b.Reader(config.COLUMNSTORE)

	b.Exchange(READER_EXCHANGE, nil)

	return b.Build()
}

func (b *LogicalPlanBuilderImpl) CreateMeasurementPlan(seriesPlan hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if seriesPlan != nil {
		b.Push(seriesPlan)
	}

	b.Reader(config.TSSTORE)

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

func (b *LogicalPlanBuilderImpl) CreatePartitionPlan(scanPlan hybridqp.QueryNode) (hybridqp.QueryNode, error) {
	if scanPlan == nil {
		return nil, nil
	}

	b.Push(scanPlan)

	b.Exchange(PARTITION_EXCHANGE, nil)

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

// impl me
func (p *LogicalDummyShard) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
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

func (p *LogicalDummyShard) ReplaceChildren(children []hybridqp.QueryNode) {}

func (p *LogicalDummyShard) ReplaceChild(ordinal int, child hybridqp.QueryNode) {}

func (p *LogicalDummyShard) ExplainIterms(writer LogicalPlanWriter) {}

func (p *LogicalDummyShard) Explain(writer LogicalPlanWriter) {}

func (p *LogicalDummyShard) Type() string {
	return GetType(p)
}

func (p *LogicalDummyShard) Digest() string {
	return p.String()
}

type LogicalIndexScan struct {
	LogicalPlanSingle
}

func NewLogicalIndexScan(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalIndexScan {
	project := &LogicalIndexScan{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	project.init()

	return project
}

func (p *LogicalIndexScan) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return NewLogicalIndexScan(inputs[0], schema)
}

func (p *LogicalIndexScan) DeriveOperations() {
	p.init()
}

func (p *LogicalIndexScan) init() {
	p.InitRef(p.inputs[0])
}

func (p *LogicalIndexScan) Clone() hybridqp.QueryNode {
	clone := &LogicalIndexScan{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalIndexScan) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalIndexScan) Type() string {
	return GetType(p)
}

func (p *LogicalIndexScan) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalTSSPScan struct {
	dimensions []string
	newSeqs    []uint64
	mstName    string
	files      *immutable.TSSPFiles
	LogicalPlanSingle
}

func NewLogicalTSSPScan(schema hybridqp.Catalog) *LogicalTSSPScan {
	reader := &LogicalTSSPScan{
		dimensions:        schema.Options().GetOptDimension(),
		LogicalPlanSingle: *NewLogicalPlanSingle(nil, schema),
	}

	if schema.Sources() != nil {
		reader.mstName = schema.Sources()[0].String()
	} else if schema.Options().(*query.ProcessorOptions).Sources != nil {
		reader.mstName = schema.Options().(*query.ProcessorOptions).Sources[0].String()
	}

	reader.init()

	return reader
}

// impl me
func (p *LogicalTSSPScan) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalTSSPScan) GetFiles() *immutable.TSSPFiles {
	return p.files
}

func (p *LogicalTSSPScan) SetFiles(files *immutable.TSSPFiles) {
	p.files = files
}

func (p *LogicalTSSPScan) GetNewSeqs() []uint64 {
	return p.newSeqs
}

func (p *LogicalTSSPScan) SetNewSeqs(seqs []uint64) {
	p.newSeqs = seqs
}

func (p *LogicalTSSPScan) MstName() string {
	return p.mstName
}

func (p *LogicalTSSPScan) DeriveOperations() {
	p.init()
}

func (p *LogicalTSSPScan) init() {
	(&p.LogicalPlanBase).PreAggInit()
}

func (p *LogicalTSSPScan) Clone() hybridqp.QueryNode {
	clone := &LogicalTSSPScan{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	clone.digest = false
	return clone
}

func (p *LogicalTSSPScan) Children() []hybridqp.QueryNode {
	return nil
}

func (p *LogicalTSSPScan) ReplaceChildren(children []hybridqp.QueryNode) {}

func (p *LogicalTSSPScan) ReplaceChild(ordinal int, child hybridqp.QueryNode) {}

func (p *LogicalTSSPScan) ExplainIterms(writer LogicalPlanWriter) {
	explainIterms(writer, p.id, p.mstName, p.dimensions)
}

func (p *LogicalTSSPScan) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	p.LogicalPlanBase.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalTSSPScan) Type() string {
	return GetType(p)
}

func (p *LogicalTSSPScan) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.ID())
	return string(p.digestName)
}

type LogicalWriteIntoStorage struct {
	m          *immutable.MmsTables
	dimensions []string
	mstName    string
	LogicalPlanSingle
}

func NewLogicalWriteIntoStorage(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalWriteIntoStorage {
	reader := &LogicalWriteIntoStorage{
		dimensions:        schema.Options().GetOptDimension(),
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	if schema.Sources() != nil {
		reader.mstName = schema.Sources()[0].String()
	} else if schema.Options().(*query.ProcessorOptions).Sources != nil {
		reader.mstName = schema.Options().(*query.ProcessorOptions).Sources[0].String()
	}

	reader.init()

	return reader
}

// impl me
func (p *LogicalWriteIntoStorage) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalWriteIntoStorage) GetMmsTables() *immutable.MmsTables {
	return p.m
}

func (p *LogicalWriteIntoStorage) SetMmsTables(m *immutable.MmsTables) {
	p.m = m
}

func (p *LogicalWriteIntoStorage) MstName() string {
	return p.mstName
}

func (p *LogicalWriteIntoStorage) DeriveOperations() {
	p.init()
}

func (p *LogicalWriteIntoStorage) init() {
	refs := p.inputs[0].RowDataType().MakeRefs()
	m := make(map[string]influxql.VarRef)
	for _, ref := range refs {
		m[ref.Val] = ref
	}

	mc := make(map[string]hybridqp.ExprOptions)
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

func (p *LogicalWriteIntoStorage) Clone() hybridqp.QueryNode {
	clone := &LogicalWriteIntoStorage{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	clone.digest = false
	return clone
}

func (p *LogicalWriteIntoStorage) ReplaceChildren(children []hybridqp.QueryNode) {}

func (p *LogicalWriteIntoStorage) ReplaceChild(ordinal int, child hybridqp.QueryNode) {}

func (p *LogicalWriteIntoStorage) ExplainIterms(writer LogicalPlanWriter) {
	explainIterms(writer, p.id, p.mstName, p.dimensions)
}

func (p *LogicalWriteIntoStorage) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	p.LogicalPlanBase.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalWriteIntoStorage) Type() string {
	return GetType(p)
}

func (p *LogicalWriteIntoStorage) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.ID())
	return string(p.digestName)
}

type LogicalSequenceAggregate struct {
	calls           map[string]*influxql.Call
	callsOrder      []string
	isCountDistinct bool
	aggType         int
	LogicalPlanSingle
}

func NewLogicalSequenceAggregate(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSequenceAggregate {
	agg := &LogicalSequenceAggregate{
		calls:             make(map[string]*influxql.Call),
		callsOrder:        make([]string, 0, len(schema.Calls())),
		isCountDistinct:   false,
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

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

// impl me
func (p *LogicalSequenceAggregate) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalSequenceAggregate) DeriveOperations() {
	p.init()
}

func (p *LogicalSequenceAggregate) init() {
	refs := p.inputs[0].RowDataType().MakeRefs()

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

func (p *LogicalSequenceAggregate) Clone() hybridqp.QueryNode {
	clone := &LogicalSequenceAggregate{}
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

func (p *LogicalSequenceAggregate) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSequenceAggregate) Type() string {
	return GetType(p)
}

func (p *LogicalSequenceAggregate) Digest() string {
	// format fmt.Sprintf("%s(%d)[%d](%s)(%s)", GetTypeName(p), p.aggType, p.inputs[0].ID(), p.schema.Fields(), buffer.String())
	if !p.digest {
		p.digest = true
		buildDigest(&p.digestBuff, p.String(), p.aggType, p.inputs[0].ID(),
			p.schema.Fields(), p.calls, p.callsOrder)
	}
	return p.digestBuff.String()
}

type LogicalSplitGroup struct {
	LogicalPlanSingle
}

func NewLogicalSplitGroup(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSplitGroup {
	SplitGroup := &LogicalSplitGroup{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	SplitGroup.init()

	return SplitGroup
}

// impl me
func (p *LogicalSplitGroup) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalSplitGroup) DeriveOperations() {
	p.init()
}

func (p *LogicalSplitGroup) init() {
	p.initForSink()
}

func (p *LogicalSplitGroup) Clone() hybridqp.QueryNode {
	clone := &LogicalSplitGroup{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalSplitGroup) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSplitGroup) Type() string {
	return GetType(p)
}

func (p *LogicalSplitGroup) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	if p.inputs != nil {
		p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	}
	return string(p.digestName)
}

type LogicalFullJoin struct {
	left      hybridqp.QueryNode
	right     hybridqp.QueryNode
	condition influxql.Expr
	LogicalPlanBase
}

func NewLogicalFullJoin(left hybridqp.QueryNode, right hybridqp.QueryNode, condition influxql.Expr, schema hybridqp.Catalog) *LogicalFullJoin {
	project := &LogicalFullJoin{
		left:      left,
		right:     right,
		condition: condition,
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

// impl me
func (p *LogicalFullJoin) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalFullJoin) DeriveOperations() {
	p.init()
}

func (p *LogicalFullJoin) init() {
	p.InitRef(p.right)
	p.InitRef(p.left)
}

func (p *LogicalFullJoin) Clone() hybridqp.QueryNode {
	clone := &LogicalFullJoin{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalFullJoin) Children() []hybridqp.QueryNode {
	return []hybridqp.QueryNode{p.left, p.right}
}

func (p *LogicalFullJoin) ReplaceChildren(children []hybridqp.QueryNode) {
	if len(children) != 2 {
		panic("children count in LogicalFullJoin is not 2")
	}
	p.left = children[0]
	p.right = children[1]
}

func (p *LogicalFullJoin) ReplaceChild(ordinal int, child hybridqp.QueryNode) {
	if ordinal > 1 {
		panic(fmt.Sprintf("index %d out of range %d", ordinal, 1))
	}
	if ordinal == 0 {
		p.left = child
	} else {
		p.right = child
	}
}

func (p *LogicalFullJoin) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalFullJoin) Type() string {
	return GetType(p)
}

func (p *LogicalFullJoin) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.left.ID())
	p.digestName = encoding.MarshalUint64(p.digestName, p.right.ID())
	return string(p.digestName)
}

type LogicalHoltWinters struct {
	LogicalPlanSingle
}

func NewLogicalHoltWinters(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalHoltWinters {
	hw := &LogicalHoltWinters{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}
	hw.init()
	return hw
}

// impl me
func (p *LogicalHoltWinters) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalHoltWinters) DeriveOperations() {
	p.init()
}

func (p *LogicalHoltWinters) init() {
	p.ForwardInit(p.inputs[0])
	p.SetHoltWintersType(false, p.rt.Fields())
}

func (p *LogicalHoltWinters) Clone() hybridqp.QueryNode {
	clone := &LogicalHoltWinters{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalHoltWinters) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalHoltWinters) Type() string {
	return GetType(p)
}

func (p *LogicalHoltWinters) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

// Digest format: printf("%s(%d)[%d](%s)(%s)", name, typ, id, fields, calls)
func buildDigest(buf *bytes.Buffer, name string, typ int, id uint64, fields influxql.Fields,
	calls map[string]*influxql.Call, callsOrder []string) {

	buf.Reset()
	buf.WriteString(name)
	buf.WriteString("(")
	buf.WriteString(strconv.Itoa(typ))
	buf.WriteString(")[")
	buf.WriteString(strconv.FormatUint(id, 10))
	buf.WriteString("](")
	fields.WriteDigest(buf)
	buf.WriteString(")(")

	for i, order := range callsOrder {
		if i > 0 {
			buf.WriteString(",")
		}
		calls[order].WriteDigest(buf)
	}
	buf.WriteString(")")
}

type LogicalHashAgg struct {
	LogicalPlanSingle
	aggType    int
	calls      map[string]*influxql.Call
	callsOrder []string
	LogicalExchangeBase
	hashAggType HashAggType
}

func NewLogicalHashAgg(input hybridqp.QueryNode, schema hybridqp.Catalog, eType ExchangeType, eTraits []hybridqp.Trait) *LogicalHashAgg {
	agg := &LogicalHashAgg{
		calls:               make(map[string]*influxql.Call),
		callsOrder:          nil,
		LogicalPlanSingle:   *NewLogicalPlanSingle(input, schema),
		LogicalExchangeBase: *NewLogicalExchangeBase(eType, CONSUMER_ROLE, eTraits),
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
	agg.setHashAggType()
	return agg
}

func (p *LogicalHashAgg) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	var agg *LogicalHashAgg
	if p.eType == NODE_EXCHANGE {
		agg = NewLogicalHashAgg(inputs[0], schema, p.eType, eTrait)
	} else {
		agg = NewLogicalHashAgg(inputs[0], schema, p.eType, []hybridqp.Trait{})
	}
	agg.eRole = p.eRole
	return agg
}

func (p *LogicalHashAgg) setHashAggType() {
	if p.eType == NODE_EXCHANGE {
		p.hashAggType = Fill
	} else {
		p.hashAggType = Normal
	}
}

func (p *LogicalHashAgg) init() {

	refs := p.inputs[0].RowDataType().MakeRefs()

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

func (p *LogicalHashAgg) Clone() hybridqp.QueryNode {
	clone := &LogicalHashAgg{}
	*clone = *p
	clone.calls = make(map[string]*influxql.Call)
	var ok bool
	for k, c := range p.calls {
		clone.calls[k], ok = influxql.CloneExpr(c).(*influxql.Call)
		if !ok {
			logger.GetLogger().Warn("LogicalHashAgg clone: type isn't *influxql.Call")
		}
	}
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalHashAgg) Schema() hybridqp.Catalog {
	return p.LogicalPlanBase.schema
}

func (p *LogicalHashAgg) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalHashAgg) Type() string {
	return GetType(p)
}

func (p *LogicalHashAgg) Digest() string {
	// format fmt.Sprintf("%s(%d)[%d](%s)(%s)", GetTypeName(p), p.aggType, p.inputs[0].ID(), p.schema.Fields(), buffer.String())
	if !p.digest {
		p.digest = true
	} else {
		return p.digestBuff.String()
	}
	// format fmt.Sprintf("%s(%d)[%d](%s)(%s)", GetTypeName(p), p.aggType, p.inputs[0].ID(), p.schema.Fields(), buffer.String())
	p.digestBuff.Reset()
	p.digestBuff.WriteString(p.String())
	p.digestBuff.WriteString("(")
	p.digestBuff.WriteString(strconv.Itoa(p.aggType))
	p.digestBuff.WriteString(")")
	p.digestBuff.WriteString("[")
	p.digestBuff.WriteString(strconv.FormatUint(p.inputs[0].ID(), 10))
	p.digestBuff.WriteString("]")
	p.digestBuff.WriteString("(")
	p.digestBuff.WriteString(p.schema.Fields().String())
	p.digestBuff.WriteString(")")
	p.digestBuff.WriteString("(")

	firstCall := true
	for _, order := range p.callsOrder {
		call := p.calls[order]
		if firstCall {
			call.WriteString(&p.digestBuff)
			firstCall = false
			continue
		}
		p.digestBuff.WriteString(",")
		call.WriteString(&p.digestBuff)
	}
	p.digestBuff.WriteString(")")
	return p.digestBuff.String()
}

func (p *LogicalHashAgg) DeriveOperations() {
	p.init()
}

func (p *LogicalHashAgg) ForwardCallArgs() {
	for k, call := range p.calls {
		if len(call.Args) > 0 {
			ref := p.schema.Mapping()[p.schema.Calls()[k]]
			p.digest = false
			call.Args[0] = &ref
		}
	}
}

func (p *LogicalHashAgg) CountToSum() {
	for _, call := range p.calls {
		if call.Name == "count" {
			call.Name = "sum"
			p.digest = false
		}
	}
}

func (p *LogicalHashAgg) AddTrait(trait interface{}) {
	p.eTraits = append(p.eTraits, trait)
}

type LogicalSort struct {
	LogicalPlanSingle
	sortFields influxql.SortFields
}

func NewLogicalSort(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSort {
	sort := &LogicalSort{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
		sortFields:        schema.GetSortFields(),
	}
	sort.init()
	return sort
}

// impl me
func (p *LogicalSort) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalSort) DeriveOperations() {
	p.init()
}

func (p *LogicalSort) init() {
	p.ForwardInit(p.inputs[0])
}

func (p *LogicalSort) Clone() hybridqp.QueryNode {
	clone := &LogicalSort{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	clone.digest = false
	return clone
}

func (p *LogicalSort) ExplainIterms(writer LogicalPlanWriter) {
	p.LogicalPlanBase.ExplainIterms(writer)
}

func (p *LogicalSort) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	p.LogicalPlanBase.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSort) Type() string {
	return GetType(p)
}

func (p *LogicalSort) Digest() string {
	return string(p.digestName)
}

type LogicalExchangeBase struct {
	eType   ExchangeType
	eRole   ExchangeRole
	eTraits []hybridqp.Trait
}

func NewLogicalExchangeBase(eType ExchangeType, eRole ExchangeRole, eTraits []hybridqp.Trait) *LogicalExchangeBase {
	return &LogicalExchangeBase{eType: eType, eRole: eRole, eTraits: eTraits}
}

func (p *LogicalExchangeBase) ToProducer() {
	p.eRole = PRODUCER_ROLE
}

func (p *LogicalExchangeBase) EType() ExchangeType {
	return p.eType
}

func (p *LogicalExchangeBase) ERole() ExchangeRole {
	return p.eRole
}

func (p *LogicalExchangeBase) ETraits() []hybridqp.Trait {
	return p.eTraits
}

type LogicalHashMerge struct {
	LogicalPlanSingle
	LogicalExchangeBase
}

func NewLogicalHashMerge(input hybridqp.QueryNode, schema hybridqp.Catalog, eType ExchangeType, eTraits []hybridqp.Trait) *LogicalHashMerge {
	merge := &LogicalHashMerge{
		LogicalPlanSingle:   *NewLogicalPlanSingle(input, schema),
		LogicalExchangeBase: *NewLogicalExchangeBase(eType, CONSUMER_ROLE, eTraits),
	}
	merge.init()
	return merge
}

func (p *LogicalHashMerge) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	var merge *LogicalHashMerge
	if p.eType == NODE_EXCHANGE {
		merge = NewLogicalHashMerge(inputs[0], schema, p.eType, eTrait)
	} else {
		merge = NewLogicalHashMerge(inputs[0], schema, p.eType, []hybridqp.Trait{})
	}
	merge.eRole = p.eRole
	return merge
}

func (p *LogicalHashMerge) DeriveOperations() {
	p.init()
}

func (p *LogicalHashMerge) init() {
	p.ForwardInit(p.inputs[0])
}

func (p *LogicalHashMerge) Clone() hybridqp.QueryNode {
	clone := &LogicalHashMerge{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	clone.digest = false
	return clone
}

func (p *LogicalHashMerge) ExplainIterms(writer LogicalPlanWriter) {
	p.LogicalPlanBase.ExplainIterms(writer)
}

func (p *LogicalHashMerge) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	p.LogicalPlanBase.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalHashMerge) Type() string {
	return GetType(p)
}

func (p *LogicalHashMerge) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.ID())
	return string(p.digestName)
}

func (p *LogicalHashMerge) AddTrait(trait interface{}) {
	p.eTraits = append(p.eTraits, trait)
}

func NewPlanBySchemaAndSrcPlan(schema hybridqp.Catalog, srcPlan []hybridqp.QueryNode, eTrait []hybridqp.Trait) (hybridqp.QueryNode, error) {
	var inputs []hybridqp.QueryNode
	hasScanReader := false
	for _, srcNode := range srcPlan {
		newNode := srcNode.New(inputs, schema, eTrait)
		if newNode == nil {
			return nil, fmt.Errorf("newPlan error")
		}
		if _, ok := newNode.(*LogicalReader); ok {
			hasScanReader = true
		}
		if hasScanReader {
			if agg, ok := newNode.(*LogicalAggregate); ok {
				agg.ForwardCallArgs()
				agg.CountToSum()
			}
			newNode.DeriveOperations()
		}
		inputs = inputs[:0]
		inputs = append(inputs, newNode)
	}
	return inputs[0], nil
}

type LogicalSparseIndexScan struct {
	LogicalPlanSingle
}

func NewLogicalSparseIndexScan(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalSparseIndexScan {
	project := &LogicalSparseIndexScan{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	project.init()

	return project
}

// impl me
func (p *LogicalSparseIndexScan) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalSparseIndexScan) DeriveOperations() {
	p.init()
}

func (p *LogicalSparseIndexScan) init() {
	p.ForwardInit(p.inputs[0])
}

func (p *LogicalSparseIndexScan) Clone() hybridqp.QueryNode {
	clone := &LogicalSparseIndexScan{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	return clone
}

func (p *LogicalSparseIndexScan) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalSparseIndexScan) Type() string {
	return GetType(p)
}

func (p *LogicalSparseIndexScan) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.inputs[0].ID())
	return string(p.digestName)
}

type LogicalColumnStoreReader struct {
	mstName string
	LogicalPlanSingle
}

func NewLogicalColumnStoreReader(input hybridqp.QueryNode, schema hybridqp.Catalog) *LogicalColumnStoreReader {
	reader := &LogicalColumnStoreReader{
		LogicalPlanSingle: *NewLogicalPlanSingle(input, schema),
	}

	if schema.Options().(*query.ProcessorOptions).Sources != nil {
		reader.mstName = schema.Options().(*query.ProcessorOptions).Sources[0].String()
	}

	reader.init()

	return reader
}

// impl me
func (p *LogicalColumnStoreReader) New(inputs []hybridqp.QueryNode, schema hybridqp.Catalog, eTrait []hybridqp.Trait) hybridqp.QueryNode {
	return nil
}

func (p *LogicalColumnStoreReader) MstName() string {
	return p.mstName
}

func (p *LogicalColumnStoreReader) DeriveOperations() {
	p.init()
}

func (p *LogicalColumnStoreReader) init() {
	p.initForSink()
}

func (p *LogicalColumnStoreReader) Clone() hybridqp.QueryNode {
	clone := &LogicalColumnStoreReader{}
	*clone = *p
	clone.id = hybridqp.GenerateNodeId()
	clone.digest = false
	return clone
}

func (p *LogicalColumnStoreReader) ExplainIterms(writer LogicalPlanWriter) {
	p.LogicalPlanBase.ExplainIterms(writer)
	writer.Item("mstName", p.mstName)
	writer.Item("ID", p.id)
}

func (p *LogicalColumnStoreReader) Explain(writer LogicalPlanWriter) {
	p.ExplainIterms(writer)
	p.LogicalPlanBase.ExplainIterms(writer)
	writer.Explain(p)
}

func (p *LogicalColumnStoreReader) Type() string {
	return GetType(p)
}

func (p *LogicalColumnStoreReader) Digest() string {
	if p.digest {
		return string(p.digestName)
	}
	p.digest = true
	p.digestName = p.digestName[:0]
	p.digestName = encoding.MarshalUint32(p.digestName, uint32(p.LogicPlanType()))
	p.digestName = encoding.MarshalUint64(p.digestName, p.ID())
	return string(p.digestName)
}
