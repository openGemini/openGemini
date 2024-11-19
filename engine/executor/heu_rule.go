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

package executor

import (
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

var (
	_ OptRule = &AggPushdownToExchangeRule{}
	_ OptRule = &AggPushdownToReaderRule{}
	_ OptRule = &AggPushdownToSeriesRule{}
	_ OptRule = &AggSpreadToExchangeRule{}
	_ OptRule = &AggSpreadToReaderRule{}
	_ OptRule = &AggPushDownToSubQueryRule{}
	_ OptRule = &ReaderUpdateInSubQueryRule{}
	_ OptRule = &AggToProjectInSubQueryRule{}
)

type LimitPushdownToExchangeRule struct {
	OptRuleBase
}

func NewLimitPushdownToExchangeRule(description string) *LimitPushdownToExchangeRule {
	mr := &LimitPushdownToExchangeRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalExchange{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *LimitPushdownToExchangeRule) Catagory() OptRuleCatagory {
	return RULE_PUSHDOWN_LIMIT
}

func (r *LimitPushdownToExchangeRule) ToString() string {
	return GetTypeName(r)
}

func (r *LimitPushdownToExchangeRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*LimitPushdownToExchangeRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *LimitPushdownToExchangeRule) OnMatch(call *OptRuleCall) {
	exchange, ok := call.Node(0).(*LogicalExchange)
	if !ok {
		logger.GetLogger().Warn("LimitPushdownToSeriesRule OnMatch failed, call Node 0 isn't *LogicalExchange")
		return
	}

	if !exchange.Schema().HasLimit() {
		return
	}

	if exchange.Schema().HasCall() {
		return
	}

	if exchange.Schema().Options().HaveOnlyCSStore() {
		return
	}

	exchangeType := exchange.EType()
	if exchangeType == NODE_EXCHANGE || exchangeType == SERIES_EXCHANGE {
		return
	}

	if vertex, ok := call.planner.Vertex(exchange); ok {
		builder := NewLogicalPlanBuilderImpl(exchange.Schema())
		node, err := builder.CreateLimit(vertex)
		if err != nil {
			panic(err.Error())
		}
		if _, ok := call.planner.Vertex(node); ok {
			return
		}
		call.TransformTo(node)
	}
}

type LimitPushdownToReaderRule struct {
	OptRuleBase
}

func NewLimitPushdownToReaderRule(description string) *LimitPushdownToReaderRule {
	mr := &LimitPushdownToReaderRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalReader{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *LimitPushdownToReaderRule) Catagory() OptRuleCatagory {
	return RULE_PUSHDOWN_LIMIT
}

func (r *LimitPushdownToReaderRule) ToString() string {
	return GetTypeName(r)
}

func (r *LimitPushdownToReaderRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*LimitPushdownToReaderRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *LimitPushdownToReaderRule) OnMatch(call *OptRuleCall) {
	reader, ok := call.Node(0).(*LogicalReader)
	if !ok {
		logger.GetLogger().Warn("LimitPushdownToSeriesRule OnMatch failed, call Node 0 isn't *LogicalReader")
		return
	}

	if !reader.Schema().HasLimit() {
		return
	}

	if reader.Schema().HasCall() {
		return
	}

	if vertex, ok := call.planner.Vertex(reader); ok {
		builder := NewLogicalPlanBuilderImpl(reader.Schema())
		node, err := builder.CreateLimit(vertex)
		if err != nil {
			panic(err.Error())
		}
		if _, ok := call.planner.Vertex(node); ok {
			return
		}
		call.TransformTo(node)
	}
}

type LimitPushdownToSeriesRule struct {
	OptRuleBase
}

func NewLimitPushdownToSeriesRule(description string) *LimitPushdownToSeriesRule {
	mr := &LimitPushdownToSeriesRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalSeries{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *LimitPushdownToSeriesRule) Catagory() OptRuleCatagory {
	return RULE_PUSHDOWN_LIMIT
}

func (r *LimitPushdownToSeriesRule) ToString() string {
	return GetTypeName(r)
}

func (r *LimitPushdownToSeriesRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*LimitPushdownToSeriesRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *LimitPushdownToSeriesRule) OnMatch(call *OptRuleCall) {
	series, ok := call.Node(0).(*LogicalSeries)
	if !ok {
		logger.GetLogger().Warn("LimitPushdownToSeriesRule OnMatch failed, call Node 0 isn't *LogicalSeries")
		return
	}

	if enableFileCursor && series.Schema().HasOptimizeAgg() {
		return
	}

	if !series.Schema().HasLimit() {
		return
	}

	if vertex, ok := call.planner.Vertex(series); ok {
		builder := NewLogicalPlanBuilderImpl(series.Schema())
		node, err := builder.CreateLimit(vertex)
		if err != nil {
			panic(err.Error())
		}
		if _, ok := call.planner.Vertex(node); ok {
			return
		}
		call.TransformTo(node)
	}
}

type AggPushdownToExchangeRule struct {
	OptRuleBase
}

func NewAggPushdownToExchangeRule(description string) *AggPushdownToExchangeRule {
	mr := &AggPushdownToExchangeRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalExchange{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *AggPushdownToExchangeRule) Catagory() OptRuleCatagory {
	return RULE_PUSHDOWN_AGG
}

func (r *AggPushdownToExchangeRule) ToString() string {
	return GetTypeName(r)
}

func (r *AggPushdownToExchangeRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*AggPushdownToExchangeRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *AggPushdownToExchangeRule) OnMatch(call *OptRuleCall) {
	exchange, ok := call.Node(0).(*LogicalExchange)
	if !ok {
		logger.GetLogger().Warn("AggPushdownToSeriesRule OnMatch failed, OptRuleCall Node 0 isn't *LogicalExchange")
		return
	}

	if !exchange.Schema().HasCall() {
		return
	}

	if !exchange.Schema().CanCallsPushdown() {
		return
	}

	// the calculation of prom function with range vector selector is only pushed down to series
	// however the agg in the prom nested call needs to be pushed down to the exchange.
	if exchange.schema.Options().IsRangeVectorSelector() && !exchange.Schema().HasPromNestedCall() {
		return
	}

	switch exchange.EType() {
	case NODE_EXCHANGE, SERIES_EXCHANGE:
		return
	default:
		break
	}

	if exchange.Schema().HasCastorCall() && !exchange.Schema().Options().IsGroupByAllDims() {
		return
	}

	if vertex, ok := call.planner.Vertex(exchange); ok {
		builder := NewLogicalPlanBuilderImpl(exchange.Schema())
		node, err := builder.CreateAggregate(vertex)
		if err != nil {
			panic(err.Error())
		}
		if _, ok := call.planner.Vertex(node); ok {
			return
		}
		call.TransformTo(node)
	}
}

type AggPushdownToReaderRule struct {
	OptRuleBase
}

func NewAggPushdownToReaderRule(description string) *AggPushdownToReaderRule {
	mr := &AggPushdownToReaderRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalReader{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *AggPushdownToReaderRule) Catagory() OptRuleCatagory {
	return RULE_PUSHDOWN_AGG
}

func (r *AggPushdownToReaderRule) ToString() string {
	return GetTypeName(r)
}

func (r *AggPushdownToReaderRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*AggPushdownToReaderRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *AggPushdownToReaderRule) OnMatch(call *OptRuleCall) {
	reader, ok := call.Node(0).(*LogicalReader)
	if !ok {
		logger.GetLogger().Warn("AggPushdownToSeriesRule OnMatch failed, OptRuleCall Node 0 isn't *LogicalReader")
		return
	}

	if !reader.Schema().HasCall() {
		return
	}

	canSlidingWindowPushDown := reader.Schema().HasSlidingWindowCall() && sysconfig.GetEnableSlidingWindowPushUp() != sysconfig.OnSlidingWindowPushUp
	if !reader.Schema().CanCallsPushdown() || (!reader.Schema().HasPercentileOGSketch() && !canSlidingWindowPushDown) {
		return
	}

	// the calculation of prom function with range vector selector is only pushed down to series
	// however the agg in the prom nested call needs to be pushed down to the reader.
	if reader.schema.Options().IsRangeVectorSelector() && !reader.Schema().HasPromNestedCall() {
		return
	}

	if reader.Schema().MatchPreAgg() {
		return
	}

	if reader.schema.HasCastorCall() && !reader.Schema().Options().IsGroupByAllDims() {
		return
	}

	if vertex, ok := call.planner.Vertex(reader); ok {
		builder := NewLogicalPlanBuilderImpl(reader.Schema())
		var node hybridqp.QueryNode
		var err error
		if canSlidingWindowPushDown {
			node, err = builder.CreateSlideWindow(vertex)
		} else {
			node, err = builder.CreateAggregate(vertex)
		}
		if err != nil {
			panic(err.Error())
		}
		if _, ok := call.planner.Vertex(node); ok {
			return
		}
		call.TransformTo(node)
	}
}

type AggPushDownToColumnStoreReaderRule struct {
	OptRuleBase
}

func NewAggPushDownToColumnStoreReaderRule(description string) *AggPushDownToColumnStoreReaderRule {
	mr := &AggPushDownToColumnStoreReaderRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalColumnStoreReader{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *AggPushDownToColumnStoreReaderRule) Catagory() OptRuleCatagory {
	return RULE_PUSHDOWN_AGG
}

func (r *AggPushDownToColumnStoreReaderRule) ToString() string {
	return GetTypeName(r)
}

func (r *AggPushDownToColumnStoreReaderRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*AggPushDownToColumnStoreReaderRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *AggPushDownToColumnStoreReaderRule) OnMatch(call *OptRuleCall) {
	series, ok := call.Node(0).(*LogicalColumnStoreReader)
	if !ok {
		logger.GetLogger().Warn("AggPushdownToSeriesRule OnMatch failed, OptRuleCall Node 0 isn't *LogicalSeries")
		return
	}

	if !series.Schema().HasCall() {
		return
	}

	if !series.Schema().CanCallsPushdown() {
		return
	}

	if series.Schema().ContainSeriesIgnoreCall() {
		return
	}

	if vertex, ok := call.planner.Vertex(series); ok {
		builder := NewLogicalPlanBuilderImpl(series.Schema())
		node, err := builder.CreateAggregate(vertex)
		if err != nil {
			panic(err.Error())
		}
		if _, ok := call.planner.Vertex(node); ok {
			return
		}
		call.TransformTo(node)
	}
}

type AggPushdownToSeriesRule struct {
	OptRuleBase
}

func NewAggPushdownToSeriesRule(description string) *AggPushdownToSeriesRule {
	mr := &AggPushdownToSeriesRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalSeries{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *AggPushdownToSeriesRule) Catagory() OptRuleCatagory {
	return RULE_PUSHDOWN_AGG
}

func (r *AggPushdownToSeriesRule) ToString() string {
	return GetTypeName(r)
}

func (r *AggPushdownToSeriesRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*AggPushdownToSeriesRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *AggPushdownToSeriesRule) OnMatch(call *OptRuleCall) {
	series, ok := call.Node(0).(*LogicalSeries)
	if !ok {
		logger.GetLogger().Warn("AggPushdownToSeriesRule OnMatch failed, OptRuleCall Node 0 isn't *LogicalSeries")
		return
	}

	if !series.Schema().HasCall() {
		return
	}

	// the calculation of prom function with range vector selector is only pushed down to series.
	if !series.Schema().CanCallsPushdown() && !(series.Schema().Options().IsRangeVectorSelector()) {
		return
	}

	if series.Schema().ContainSeriesIgnoreCall() {
		return
	}

	if vertex, ok := call.planner.Vertex(series); ok {
		builder := NewLogicalPlanBuilderImpl(series.Schema())
		node, err := builder.CreateAggregate(vertex)
		if err != nil {
			panic(err.Error())
		}
		if _, ok := call.planner.Vertex(node); ok {
			return
		}
		call.TransformTo(node)
	}
}

type AggSpreadToExchangeRule struct {
	OptRuleBase
}

func NewAggSpreadToExchangeRule(description string) *AggSpreadToExchangeRule {
	mr := &AggSpreadToExchangeRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalExchange{}).Type())
	input := builder.Operand()
	builder.OneInput((&LogicalAggregate{}).Type(), input)

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *AggSpreadToExchangeRule) Catagory() OptRuleCatagory {
	return RULE_SPREAD_AGG
}

func (r *AggSpreadToExchangeRule) ToString() string {
	return GetTypeName(r)
}

func (r *AggSpreadToExchangeRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*AggSpreadToExchangeRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *AggSpreadToExchangeRule) OnMatch(call *OptRuleCall) {
	agg, ok := call.Node(0).(*LogicalAggregate)
	if !ok {
		logger.GetLogger().Warn("AggSpreadToReaderRule OnMatch failed, OptRuleCall Node 0 isn't *LogicalAggregate")
		return
	}

	if !agg.Schema().HasCall() {
		return
	}

	if !agg.Schema().CanCallsPushdown() {
		return
	}

	if agg.schema.HasCastorCall() {
		return
	}

	clone, ok := agg.Clone().(*LogicalAggregate)
	if !ok {
		logger.GetLogger().Warn("AggSpreadToReaderRule OnMatch failed, after clone isn't *LogicalAggregate")
		return
	}
	clone.ForwardCallArgs()
	clone.CountToSum()
	call.TransformTo(clone)
}

type AggSpreadToSortAppendRule struct {
	OptRuleBase
}

func NewAggSpreadToSortAppendRule(description string) *AggSpreadToSortAppendRule {
	mr := &AggSpreadToSortAppendRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalSortAppend{}).Type())
	input := builder.Operand()
	builder.OneInput((&LogicalAggregate{}).Type(), input)

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *AggSpreadToSortAppendRule) Catagory() OptRuleCatagory {
	return RULE_SPREAD_AGG
}

func (r *AggSpreadToSortAppendRule) ToString() string {
	return GetTypeName(r)
}

func (r *AggSpreadToSortAppendRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*AggSpreadToSortAppendRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *AggSpreadToSortAppendRule) OnMatch(call *OptRuleCall) {
	agg, ok := call.Node(0).(*LogicalAggregate)
	if !ok {
		logger.GetLogger().Warn("AggSpreadToReaderRule OnMatch failed, OptRuleCall Node 0 isn't *LogicalAggregate")
		return
	}

	if !agg.Schema().HasCall() {
		return
	}

	if !agg.Schema().CanCallsPushdown() {
		return
	}

	if !agg.Schema().IsMultiMeasurements() {
		return
	}

	if agg.schema.HasCastorCall() && !agg.Schema().Options().IsGroupByAllDims() {
		return
	}

	clone, ok := agg.Clone().(*LogicalAggregate)
	if !ok {
		logger.GetLogger().Warn("AggSpreadToReaderRule OnMatch failed, after clone isn't *LogicalAggregate")
		return
	}
	clone.ForwardCallArgs()
	clone.CountToSum()
	call.TransformTo(clone)
}

type AggSpreadToReaderRule struct {
	OptRuleBase
}

func NewAggSpreadToReaderRule(description string) *AggSpreadToReaderRule {
	mr := &AggSpreadToReaderRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalExchange{}).Type())
	input := builder.Operand()
	builder.OneInput((&LogicalReader{}).Type(), input)
	input = builder.Operand()
	builder.OneInput((&LogicalAggregate{}).Type(), input)

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *AggSpreadToReaderRule) Catagory() OptRuleCatagory {
	return RULE_SPREAD_AGG
}

func (r *AggSpreadToReaderRule) ToString() string {
	return GetTypeName(r)
}

func (r *AggSpreadToReaderRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*AggSpreadToReaderRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *AggSpreadToReaderRule) OnMatch(call *OptRuleCall) {
	agg, ok := call.Node(0).(*LogicalAggregate)
	if !ok {
		logger.GetLogger().Warn("AggSpreadToReaderRule OnMatch failed, OptRuleCall Node 0 isn't *LogicalAggregate")
		return
	}

	if !agg.Schema().HasCall() {
		return
	}

	if !agg.Schema().CanCallsPushdown() {
		return
	}

	if agg.schema.HasCastorCall() && !agg.Schema().Options().IsGroupByAllDims() {
		return
	}

	if agg.Schema().MatchPreAgg() {
		return
	}

	clone, ok := agg.Clone().(*LogicalAggregate)
	if !ok {
		logger.GetLogger().Warn("AggSpreadToReaderRule OnMatch failed, after clone isn't *LogicalAggregate")
		return
	}
	clone.ForwardCallArgs()
	clone.CountToSum()
	call.TransformTo(clone)
}

type AggPushDownToSubQueryRule struct {
	OptRuleBase
}

func NewAggPushDownToSubQueryRule(description string) *AggPushDownToSubQueryRule {
	mr := &AggPushDownToSubQueryRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalProject{}).Type())
	input := builder.Operand()
	builder.WildCardInput((&LogicalSubQuery{}).Type(), input)
	input = builder.Operand()
	builder.WildCardInput((&LogicalAggregate{}).Type(), input)

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *AggPushDownToSubQueryRule) Catagory() OptRuleCatagory {
	return RULE_SUBQUERY
}

func (r *AggPushDownToSubQueryRule) ToString() string {
	return GetTypeName(r)
}

func (r *AggPushDownToSubQueryRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*AggPushDownToSubQueryRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *AggPushDownToSubQueryRule) canPush(agg *LogicalAggregate, project *LogicalProject) bool {
	if project.Schema().HasGroupBy() {
		return false
	}

	if len(project.Schema().Sources()) > 1 || len(agg.Schema().Sources()) > 1 {
		return false
	}

	if !agg.Schema().HasCall() || project.Schema().HasCall() {
		return false
	}

	if agg.Schema().HasSlidingWindowCall() {
		return false
	}

	if project.Schema().HasMath() || project.Schema().HasString() {
		return false
	}

	if !agg.Schema().CanCallsPushdown() {
		return false
	}

	if agg.Schema().CountDistinct() != nil {
		return false
	}

	if len(project.Schema().(*QuerySchema).binarys) > 0 {
		return false
	}

	if agg.Schema().(*QuerySchema).opt.HasInterval() {
		return false
	}

	if project.Schema().HasLimit() {
		return false
	}
	if !config.GetCommon().PreAggEnabled || project.schema.Options().GetHintType() == hybridqp.ExactStatisticQuery {
		return false
	}
	if project.Schema().Options().IsRangeVectorSelector() {
		return false
	}
	return true
}

func getCallField(pField *influxql.Field, f *influxql.Field) *influxql.Field {
	aggArg := f.Expr.(*influxql.Call).Args[0].(*influxql.VarRef)
	if pField.Alias == aggArg.Val {
		midCall := &influxql.Call{
			Name: f.Expr.(*influxql.Call).Name,
			Args: []influxql.Expr{
				&influxql.VarRef{Val: pField.Expr.(*influxql.VarRef).Val, Type: aggArg.Type},
			},
		}
		return &influxql.Field{Expr: midCall, Alias: pField.Alias}
	} else if pField.Expr.(*influxql.VarRef).Val == aggArg.Val {
		return &influxql.Field{Expr: influxql.CloneExpr(f.Expr), Alias: ""}
	}
	return nil
}

func ExprWalk(v influxql.Fields, node influxql.Node) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *influxql.BinaryExpr:
		ExprWalk(v, n.LHS)
		ExprWalk(v, n.RHS)

	case *influxql.Call:
		for _, expr := range n.Args {
			for _, f := range v {
				if expr.(*influxql.VarRef).Val == f.Alias {
					expr.(*influxql.VarRef).Val = f.Expr.(*influxql.VarRef).Val
				}
			}
		}
	}
}

func getVarRefField(pField *influxql.Field, f *influxql.Field) *influxql.Field {
	aggVar := f.Expr.(*influxql.VarRef)
	if pField.Alias == aggVar.Val {
		midCall := &influxql.VarRef{Val: pField.Expr.(*influxql.VarRef).Val, Type: aggVar.Type}
		return &influxql.Field{Expr: midCall, Alias: pField.Alias}
	} else if pField.Expr.(*influxql.VarRef).Val == aggVar.Val {
		return &influxql.Field{Expr: influxql.CloneExpr(f.Expr), Alias: ""}
	}
	return nil
}

func getBinaryfField(pFields influxql.Fields, f *influxql.Field) *influxql.Field {
	ExprWalk(pFields, f.Expr)
	return f
}

func getField(pField *influxql.Field, f *influxql.Field, pFields influxql.Fields) *influxql.Field {
	switch f.Expr.(type) {
	case *influxql.BinaryExpr:
		goal := getBinaryfField(pFields, f)
		return goal
	case *influxql.VarRef:
		goal := getVarRefField(pField, f)
		return goal
	case *influxql.Call:
		goal := getCallField(pField, f)
		return goal
	default:
		return nil
	}
}

func (r *AggPushDownToSubQueryRule) OnMatch(call *OptRuleCall) {
	agg, ok := call.Node(0).(*LogicalAggregate)
	if !ok {
		logger.GetLogger().Warn("AggPushDownToSubQueryRule OnMatch failed, call Node 0 isn't LogicalAggregate")
		return
	}
	project, ok := call.Node(2).(*LogicalProject)
	if !ok {
		logger.GetLogger().Warn("AggPushDownToSubQueryRule OnMatch failed, call Node 2 isn't LogicalProject")
		return
	}

	if !r.canPush(agg, project) {
		return
	}

	aggVertex, ok := call.planner.Vertex(agg)
	if !ok {
		return
	}

	aggFromVertex := aggVertex.GetParentVertex(aggVertex)
	if aggFromVertex == nil {
		return
	}

	aggFieldsCopy := make(influxql.Fields, 0, len(agg.Schema().GetQueryFields()))
	aggColumnCopy := make([]string, 0, len(agg.Schema().GetColumnNames()))
	for _, f := range agg.Schema().GetQueryFields() {
		for _, pField := range project.Schema().GetQueryFields() {
			goal := getField(pField, f, project.Schema().GetQueryFields())
			if goal != nil {
				aggFieldsCopy = append(aggFieldsCopy, goal)
				if _, ok := f.Expr.(*influxql.BinaryExpr); ok {
					break
				}
			} else {
				continue
			}
		}
	}

	aggColumnCopy = append(aggColumnCopy, agg.Schema().GetColumnNames()...)

	project.Schema().(*QuerySchema).reset(aggFieldsCopy, aggColumnCopy)

	fieldsCopy := make(influxql.Fields, 0, len(agg.Schema().GetQueryFields()))
	columnCopy := make([]string, 0, len(agg.Schema().GetColumnNames()))
	for i, f := range agg.Schema().GetQueryFields() {
		varRefType, err := agg.schema.(*QuerySchema).deriveType(f.Expr)
		if err != nil {
			return
		}
		if f.Alias == "" {
			fieldsCopy = append(fieldsCopy,
				&influxql.Field{Expr: &influxql.VarRef{Val: agg.schema.(*QuerySchema).columnNames[i], Type: varRefType},
					Alias: f.Alias})
		} else {
			fieldsCopy = append(fieldsCopy,
				&influxql.Field{Expr: &influxql.VarRef{Val: f.Alias, Type: varRefType},
					Alias: f.Alias})
		}
	}

	columnCopy = append(columnCopy, agg.Schema().GetColumnNames()...)

	agg.Schema().(*QuerySchema).reset(fieldsCopy, columnCopy)
	call.planner.(*HeuPlannerImpl).dag.RemoveEdge(agg.Children()[0].(*HeuVertex), aggVertex)
	aggSonChild := agg.Children()[0].(*HeuVertex).Node()
	aggVertex = nil
	call.TransformTo(aggSonChild)
}

type AggToProjectInSubQueryRule struct {
	OptRuleBase
}

func NewAggToProjectInSubQueryRule(description string) *AggToProjectInSubQueryRule {
	mr := &AggToProjectInSubQueryRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AfterInput((&LogicalProject{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *AggToProjectInSubQueryRule) Catagory() OptRuleCatagory {
	return RULE_SUBQUERY
}

func (r *AggToProjectInSubQueryRule) ToString() string {
	return GetTypeName(r)
}

func (r *AggToProjectInSubQueryRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*AggToProjectInSubQueryRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *AggToProjectInSubQueryRule) OnMatch(call *OptRuleCall) {
	node := call.Node(0)

	if _, ok := node.(*LogicalAggregate); ok {
		return
	}

	if _, ok := node.(*LogicalInterval); ok {
		return
	}

	if _, ok := node.(*LogicalSlidingWindow); ok {
		return
	}

	if _, ok := node.(*LogicalHashAgg); ok {
		if node.Schema().Options().IsPromQuery() {
			return
		}
	}

	if !node.Schema().HasCall() {
		return
	}

	nodeVertex, ok := call.planner.Vertex(node)
	if !ok {
		return
	}

	nodeFromVertex := nodeVertex.GetParentVertex(nodeVertex)
	if nodeFromVertex == nil {
		return
	}

	if node.Schema().Options().IsRangeVectorSelector() {
		return
	}

	if vertex, ok := call.planner.Vertex(node); ok {
		builder := NewLogicalPlanBuilderImpl(node.Schema())
		nodeA, err := builder.CreateAggregate(vertex)
		if err != nil {
			panic(err.Error())
		}
		if _, ok := call.planner.Vertex(nodeA); ok {
			return
		}
		call.TransformTo(nodeA)
	}
}

type ReaderUpdateInSubQueryRule struct {
	OptRuleBase
}

func NewReaderUpdateInSubQueryRule(description string) *ReaderUpdateInSubQueryRule {
	mr := &ReaderUpdateInSubQueryRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalSeries{}).Type())
	input := builder.Operand()
	builder.WildCardInput((&LogicalReader{}).Type(), input)
	input = builder.Operand()
	builder.OneInput((&LogicalExchange{}).Type(), input)

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *ReaderUpdateInSubQueryRule) Catagory() OptRuleCatagory {
	return RULE_SUBQUERY
}

func (r *ReaderUpdateInSubQueryRule) ToString() string {
	return GetTypeName(r)
}

func (r *ReaderUpdateInSubQueryRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*ReaderUpdateInSubQueryRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *ReaderUpdateInSubQueryRule) OnMatch(call *OptRuleCall) {
	node := call.Node(0)

	if !node.Schema().MatchPreAgg() {
		return
	}

	if node.Schema().HasInterval() {
		return
	}

	if node.Schema().Options().GetHintType() != 0 {
		return
	}

	if !node.Schema().MatchPreAgg() {
		return
	}

	if _, ok := call.planner.Vertex(node); ok {
		builder := NewLogicalPlanBuilderImpl(node.Schema())
		var plan hybridqp.QueryNode
		var pErr error
		plan, pErr = builder.CreateSeriesPlan()
		if pErr != nil {
			return
		}

		plan, pErr = builder.CreateMeasurementPlan(plan)
		if pErr != nil {
			return
		}
		if IsSubTreeEqual(plan, node) {
			return
		}

		if _, ok := call.planner.Vertex(plan); ok {
			return
		}

		call.planner.(*HeuPlannerImpl).addNodeToDag(plan)
		call.TransformTo(plan)
	}
}

type IntervalToProjectInSubQueryRule struct {
	OptRuleBase
}

func NewIntervalToProjectInSubQueryRule(description string) *IntervalToProjectInSubQueryRule {
	mr := &IntervalToProjectInSubQueryRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AfterInput((&LogicalProject{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *IntervalToProjectInSubQueryRule) Catagory() OptRuleCatagory {
	return RULE_SUBQUERY
}

func (r *IntervalToProjectInSubQueryRule) ToString() string {
	return GetTypeName(r)
}

func (r *IntervalToProjectInSubQueryRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*IntervalToProjectInSubQueryRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *IntervalToProjectInSubQueryRule) OnMatch(call *OptRuleCall) {
	node := call.Node(0)
	if _, ok := node.(*LogicalAggregate); !ok {
		return
	}

	if _, ok := node.(*LogicalInterval); ok {
		return
	}

	nodeVertex, ok := call.planner.Vertex(node)
	if !ok {
		return
	}

	nodeFromVertex := nodeVertex.GetParentVertex(nodeVertex)
	if nodeFromVertex == nil {
		return
	}

	if vertex, ok := call.planner.Vertex(node); ok {
		builder := NewLogicalPlanBuilderImpl(node.Schema())
		nodeA, err := builder.CreateInterval(vertex)
		if err != nil {
			panic(err.Error())
		}
		if _, ok := call.planner.Vertex(nodeA); ok {
			return
		}
		call.TransformTo(nodeA)
	}
}

type SlideWindowSpreadRule struct {
	OptRuleBase
}

func NewSlideWindowSpreadRule(description string) *SlideWindowSpreadRule {
	mr := &SlideWindowSpreadRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalSlidingWindow{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *SlideWindowSpreadRule) Catagory() OptRuleCatagory {
	return RULE_SPREAD_AGG
}

func (r *SlideWindowSpreadRule) ToString() string {
	return GetTypeName(r)
}

func (r *SlideWindowSpreadRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*SlideWindowSpreadRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *SlideWindowSpreadRule) OnMatch(call *OptRuleCall) {
	slidewindow, ok := call.Node(0).(*LogicalSlidingWindow)
	if !ok {
		logger.GetLogger().Warn("SlideWindowSpreadRule OnMatch failed, OptRuleCall Node 0 isn't *slidewindow")
		return
	}

	if !slidewindow.Schema().CanCallsPushdown() {
		return
	}

	if slidewindow.schema.HasSubQuery() {
		return
	}

	clone, ok := slidewindow.Clone().(*LogicalSlidingWindow)
	if !ok {
		logger.GetLogger().Warn("SlideWindowSpreadRule OnMatch failed, after clone isn't *slidewindow")
		return
	}
	clone.ForwardCallArgs()
	clone.CountToSum()
	call.TransformTo(clone)
}

type CastorAggCutRule struct {
	OptRuleBase
}

func NewCastorAggCutRule(description string) *CastorAggCutRule {
	mr := &CastorAggCutRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalAggregate{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *CastorAggCutRule) Catagory() OptRuleCatagory {
	return RULE_HEIMADLL_PUSHDOWN
}

func (r *CastorAggCutRule) ToString() string {
	return GetTypeName(r)
}

func (r *CastorAggCutRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*CastorAggCutRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *CastorAggCutRule) OnMatch(call *OptRuleCall) {
	agg, ok := call.Node(0).(*LogicalAggregate)
	if !ok {
		logger.GetLogger().Warn("LogicalAggregate of Heimdal OnMatch failed, OptRuleCall Node 0 isn't *LogicalAggregate")
		return
	}

	if !agg.Schema().HasCastorCall() {
		return
	}

	if !agg.Schema().Options().IsGroupByAllDims() {
		return
	}

	aggSonChild := agg.Children()[0].(*HeuVertex).Node()

	exchange, ok := aggSonChild.(*LogicalExchange)
	if ok && exchange.eType == SHARD_EXCHANGE {
		return
	}

	aggVertex, ok := call.planner.Vertex(agg)
	if !ok {
		return
	}

	call.planner.(*HeuPlannerImpl).dag.RemoveEdge(agg.Children()[0].(*HeuVertex), aggVertex)
	call.TransformTo(aggSonChild)
}

type IncAggRule struct {
	OptRuleBase
}

func NewIncAggRule(description string) *IncAggRule {
	mr := &IncAggRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalIncAgg{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *IncAggRule) Catagory() OptRuleCatagory {
	return RULE_SPREAD_AGG
}

func (r *IncAggRule) ToString() string {
	return GetTypeName(r)
}

func (r *IncAggRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*IncAggRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *IncAggRule) OnMatch(call *OptRuleCall) {
	incAgg, ok := call.Node(0).(*LogicalIncAgg)
	if !ok {
		logger.GetLogger().Warn("IncAggRule OnMatch failed, OptRuleCall Node 0 isn't *IncAgg")
		return
	}

	if !incAgg.Schema().CanCallsPushdown() {
		return
	}

	if incAgg.schema.HasSubQuery() {
		return
	}

	clone, ok := incAgg.Clone().(*LogicalIncAgg)
	if !ok {
		logger.GetLogger().Warn("IncAggRule OnMatch failed, after clone isn't *IncAgg")
		return
	}
	clone.ForwardCallArgs()
	clone.CountToSum()
	call.TransformTo(clone)
}

type IncHashAggRule struct {
	OptRuleBase
}

func NewIncHashAggRule(description string) *IncHashAggRule {
	mr := &IncHashAggRule{}
	if description == "" {
		description = GetType(mr)
	}

	builder := NewOptRuleOperandBuilderBase()
	builder.AnyInput((&LogicalIncHashAgg{}).Type())

	mr.Initialize(mr, builder.Operand(), description)
	return mr
}

func (r *IncHashAggRule) Catagory() OptRuleCatagory {
	return RULE_SPREAD_AGG
}

func (r *IncHashAggRule) ToString() string {
	return GetTypeName(r)
}

func (r *IncHashAggRule) Equals(rhs OptRule) bool {
	rr, ok := rhs.(*IncHashAggRule)

	if !ok {
		return false
	}

	if r == rr {
		return true
	}

	if r.Catagory() == rr.Catagory() && r.OptRuleBase.Equals(&(rr.OptRuleBase)) {
		return true
	}

	return false
}

func (r *IncHashAggRule) OnMatch(call *OptRuleCall) {
	incAgg, ok := call.Node(0).(*LogicalIncHashAgg)
	if !ok {
		logger.GetLogger().Warn("IncHashAggRule OnMatch failed, OptRuleCall Node 0 isn't *IncAgg")
		return
	}

	if !incAgg.Schema().CanCallsPushdown() {
		return
	}

	if incAgg.schema.HasSubQuery() {
		return
	}

	clone, ok := incAgg.Clone().(*LogicalIncHashAgg)
	if !ok {
		logger.GetLogger().Warn("IncHashAggRule OnMatch failed, after clone isn't *IncAgg")
		return
	}
	clone.ForwardCallArgs()
	clone.CountToSum()
	call.TransformTo(clone)
}
