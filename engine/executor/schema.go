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

package executor

import (
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/op"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var NotAggOnSeries = make(map[string]bool)

func init() {
	initAggInfo()
	addUDAFNotAggOnSeries()
}

func initAggInfo() {
	factory := query.GetFunctionFactoryInstance()
	aggOps := factory.GetAggregateOp()
	for opName, op := range aggOps {
		if !op.CanPushDownSeries() {
			NotAggOnSeries[opName] = true
		}
	}
}

func addUDAFNotAggOnSeries() {
	// UDAF can not sink into the series
	udafRes := op.GetOpFactory().GetUDAFOpNames()
	for _, udafName := range udafRes {
		NotAggOnSeries[udafName] = true
	}
}

var DefaultTypeMapper = influxql.MultiTypeMapper(
	op.TypeMapper{},
	query.MathTypeMapper{},
	query.FunctionTypeMapper{},
	query.StringFunctionTypeMapper{},
	query.LabelFunctionTypeMapper{},
	query.PromTimeFunctionTypeMapper{},
)

type QueryTable struct {
	m  *influxql.Measurement
	rt hybridqp.RowDataType
}

func NewQueryTable(m *influxql.Measurement, refs []influxql.VarRef) *QueryTable {
	table := &QueryTable{
		m:  m,
		rt: hybridqp.NewRowDataTypeImpl(refs...),
	}

	return table
}

func (b *QueryTable) RowDataType() hybridqp.RowDataType {
	return b.rt
}

func (b *QueryTable) Name() string {
	return b.m.Name
}

type PreAggregateCallMapping struct {
	mapCalls map[string]struct{}
}

func NewPreAggregateCallMapping() *PreAggregateCallMapping {
	mapping := &PreAggregateCallMapping{
		mapCalls: make(map[string]struct{}),
	}

	mapping.mapCalls["count"] = struct{}{}
	mapping.mapCalls["sum"] = struct{}{}
	mapping.mapCalls["max"] = struct{}{}
	mapping.mapCalls["min"] = struct{}{}
	mapping.mapCalls["first"] = struct{}{}
	mapping.mapCalls["last"] = struct{}{}
	mapping.mapCalls["mean"] = struct{}{}
	return mapping
}

func (mapping *PreAggregateCallMapping) Contains(name string) bool {
	_, ok := mapping.mapCalls[name]
	return ok
}

var preAggregateCallMappingInstance *PreAggregateCallMapping
var preAggregateCallMappingOnce sync.Once

func GetPreAggregateCallMapping() *PreAggregateCallMapping {
	preAggregateCallMappingOnce.Do(func() {
		preAggregateCallMappingInstance = NewPreAggregateCallMapping()
	})

	return preAggregateCallMappingInstance
}

type ConditionExprVisitor struct {
	hasField bool
}

func NewConditionExprVisitor() *ConditionExprVisitor {
	return &ConditionExprVisitor{
		hasField: false,
	}
}

func (c *ConditionExprVisitor) Visit(n influxql.Node) influxql.Visitor {
	if _, ok := n.(influxql.Expr); !ok {
		return c
	}

	if ref, ok := n.(*influxql.VarRef); ok && ref.Type != influxql.Tag {
		c.hasField = true
		return nil
	}

	return c
}

type EquivalenceExprRewriter struct {
}

func (w *EquivalenceExprRewriter) Visit(_ influxql.Node) influxql.Visitor {
	return w
}

type QuerySchemaCreator struct {
}

func (c *QuerySchemaCreator) Create(fields influxql.Fields, columnNames []string, opt hybridqp.Options) hybridqp.Catalog {
	s := NewQuerySchema(fields, columnNames, opt, nil)
	return s
}

var _ = hybridqp.RegistryCatalogCreator(&QuerySchemaCreator{})

type QuerySchema struct {
	tables        map[string]*QueryTable
	queryFields   influxql.Fields
	columnNames   []string
	fields        influxql.Fields
	fieldsRef     influxql.VarRefs
	mapDeriveType map[influxql.Expr]influxql.DataType
	mapping       map[influxql.Expr]influxql.VarRef
	symbols       map[string]influxql.VarRef
	countDistinct *influxql.Call
	calls         map[string]*influxql.Call
	origCalls     map[string]*influxql.Call
	refs          map[string]*influxql.VarRef
	binarys       map[string]*influxql.BinaryExpr
	maths         map[string]*influxql.Call
	strings       map[string]*influxql.Call
	labelCalls    map[string]*influxql.Call
	promTimeCalls map[string]*influxql.Call
	slidingWindow map[string]*influxql.Call
	holtWinters   []*influxql.Field
	compositeCall map[string]*hybridqp.OGSketchCompositeOperator
	i             int
	notIncI       bool
	sources       influxql.Sources
	// Options is interface now, it must be cloned in internal
	opt hybridqp.Options

	joinCases         []*influxql.Join
	unnestCases       []*influxql.Unnest
	hasFieldCondition bool
	planType          PlanType
}

func NewQuerySchema(fields influxql.Fields, columnNames []string, opt hybridqp.Options, sortFields influxql.SortFields) *QuerySchema {
	schema := &QuerySchema{
		tables:        make(map[string]*QueryTable),
		queryFields:   fields,
		columnNames:   columnNames,
		fields:        make(influxql.Fields, 0, len(fields)),
		fieldsRef:     make(influxql.VarRefs, 0, len(fields)),
		mapDeriveType: make(map[influxql.Expr]influxql.DataType),
		mapping:       make(map[influxql.Expr]influxql.VarRef),
		symbols:       make(map[string]influxql.VarRef),
		calls:         make(map[string]*influxql.Call),
		origCalls:     make(map[string]*influxql.Call),
		refs:          make(map[string]*influxql.VarRef),
		binarys:       make(map[string]*influxql.BinaryExpr),
		maths:         make(map[string]*influxql.Call),
		strings:       make(map[string]*influxql.Call),
		labelCalls:    make(map[string]*influxql.Call),
		promTimeCalls: make(map[string]*influxql.Call),
		slidingWindow: make(map[string]*influxql.Call),
		holtWinters:   make([]*influxql.Field, 0),
		compositeCall: make(map[string]*hybridqp.OGSketchCompositeOperator),
		i:             0,
		notIncI:       false,
		opt:           opt,
		sources:       nil,
		planType:      UNKNOWN,
	}
	if len(sortFields) > 0 && len(opt.GetSortFields()) == 0 {
		schema.opt.SetSortFields(sortFields)
	}

	schema.init()

	if schema.HasPercentileOGSketch() {
		schema.rewritePercentileOGSketchCompositeCall()
	}

	return schema
}

func NewQuerySchemaWithJoinCase(fields influxql.Fields, sources influxql.Sources, columnNames []string, opt hybridqp.Options,
	joinCases []*influxql.Join, unnest []*influxql.Unnest, sortFields influxql.SortFields) *QuerySchema {
	q := NewQuerySchemaWithSources(fields, sources, columnNames, opt, sortFields)
	q.joinCases = joinCases
	q.SetUnnests(unnest)
	return q
}

func NewQuerySchemaWithSources(fields influxql.Fields, sources influxql.Sources, columnNames []string, opt hybridqp.Options, sortFields influxql.SortFields) *QuerySchema {
	if len(sortFields) > 0 && sources.HaveOnlyTSStore() {
		sortFields = nil
	}
	schema := NewQuerySchema(fields, columnNames, opt, sortFields)
	schema.sources = sources
	if !schema.Options().IsAscending() && schema.MatchPreAgg() && len(schema.opt.GetGroupBy()) == 0 {
		schema.Options().SetAscending(true)
	}

	return schema
}

func (qs *QuerySchema) reset(fields influxql.Fields, column []string) {
	qs.queryFields = fields
	qs.columnNames = column
	qs.fields = make(influxql.Fields, 0, len(fields))
	qs.fieldsRef = make(influxql.VarRefs, 0, len(fields))
	qs.mapDeriveType = make(map[influxql.Expr]influxql.DataType)
	qs.mapping = make(map[influxql.Expr]influxql.VarRef)
	qs.symbols = make(map[string]influxql.VarRef)
	qs.calls = make(map[string]*influxql.Call)
	qs.origCalls = make(map[string]*influxql.Call)
	qs.refs = make(map[string]*influxql.VarRef)
	qs.binarys = make(map[string]*influxql.BinaryExpr)
	qs.maths = make(map[string]*influxql.Call)
	qs.strings = make(map[string]*influxql.Call)
	qs.labelCalls = make(map[string]*influxql.Call)
	qs.promTimeCalls = make(map[string]*influxql.Call)
	qs.slidingWindow = make(map[string]*influxql.Call)
	qs.holtWinters = qs.holtWinters[0:0]
	qs.unnestCases = qs.unnestCases[:0]
	qs.i = 0
	qs.init()
}

func (qs *QuerySchema) init() {
	for _, f := range qs.queryFields {
		clone := qs.CloneField(f)
		if call, ok := clone.Expr.(*influxql.Call); ok {
			if call.Name == "sliding_window" {
				qs.AddSlidingWindow(call.String(), call)
				clone.Expr = call.Args[0]
			} else if call.Name == "holt_winters" || call.Name == "holt_winters_with_fit" {
				qs.AddHoltWinters(call, f.Alias)
				clone.Expr = call.Args[0]
			}
		}
		clone.Expr = qs.rewriteBaseCallTransformExprCall(clone.Expr)
		influxql.Walk(qs, clone.Expr)
		clone.Expr = influxql.RewriteExpr(clone.Expr, qs.rewriteExpr)
		qs.fields = append(qs.fields, clone)
	}

	for i, f := range qs.fields {
		f.Alias = qs.columnNames[i]
		typ, err := qs.deriveType(f.Expr)
		if err != nil {
			panic(fmt.Sprintf("derive type from %v failed, %v", f.Expr, err.Error()))
		}
		qs.fieldsRef = append(qs.fieldsRef, influxql.VarRef{Val: f.Name(), Type: typ})
	}
	qs.InitFieldCondition()
}

func (qs *QuerySchema) GetColumnNames() []string {
	return qs.columnNames
}

func (qs *QuerySchema) GetQueryFields() influxql.Fields {
	return qs.queryFields
}

func (qs *QuerySchema) GetOptions() hybridqp.Options {
	return qs.opt
}

func (qs *QuerySchema) CloneField(f *influxql.Field) *influxql.Field {
	clone := &influxql.Field{}
	*clone = *f
	clone.Expr = influxql.CloneExpr(f.Expr)
	return clone
}

func (qs *QuerySchema) rewriteBaseCallTransformExprCall(expr influxql.Expr) influxql.Expr {
	if expr == nil {
		return nil
	}
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		return &influxql.BinaryExpr{Op: expr.Op, LHS: qs.rewriteBaseCallTransformExprCall(expr.LHS), RHS: qs.rewriteBaseCallTransformExprCall(expr.RHS), ReturnBool: expr.ReturnBool}
	case *influxql.ParenExpr:
		return &influxql.ParenExpr{Expr: qs.rewriteBaseCallTransformExprCall(expr.Expr)}
	case *influxql.Call:
		if expr.Name == "mean" {
			if qs.opt.IsRangeVectorSelector() {
				return expr
			}
			replacement := qs.meanToSumDivCount(expr)
			typ, err := qs.deriveType(expr)
			if err != nil {
				panic(err.Error())
			}
			qs.mapDeriveType[replacement] = typ
			return replacement
		} else if expr.Name == "spread" {
			replacement := qs.spreadToMaxSubMin(expr)
			typ, err := qs.deriveType(expr)
			if err != nil {
				panic(err.Error())
			}
			qs.mapDeriveType[replacement] = typ
			return replacement
		} else {
			for i, arg := range expr.Args {
				expr.Args[i] = qs.rewriteBaseCallTransformExprCall(arg)
			}
		}
		return influxql.CloneExpr(expr)
	default:
		return influxql.CloneExpr(expr)
	}
}

func (qs *QuerySchema) meanToSumDivCount(call *influxql.Call) influxql.Expr {
	lhs := &influxql.Call{Name: "sum", Args: nil}
	lhs.Args = append(lhs.Args, influxql.CloneExpr(call.Args[0]))
	rhs := &influxql.Call{Args: nil}
	if qs.opt.IsPromQuery() {
		rhs.Name = "count_prom"
	} else {
		rhs.Name = "count"
	}
	rhs.Args = append(rhs.Args, influxql.CloneExpr(call.Args[0]))
	be := &influxql.BinaryExpr{Op: influxql.DIV, LHS: lhs, RHS: rhs}
	return be
}

func (qs *QuerySchema) spreadToMaxSubMin(call *influxql.Call) influxql.Expr {
	lhs := &influxql.Call{Name: "max", Args: nil}
	lhs.Args = append(lhs.Args, influxql.CloneExpr(call.Args[0]))
	rhs := &influxql.Call{Name: "min", Args: nil}
	rhs.Args = append(rhs.Args, influxql.CloneExpr(call.Args[0]))
	be := &influxql.BinaryExpr{Op: influxql.SUB, LHS: lhs, RHS: rhs}
	return be
}

func (qs *QuerySchema) HasCall() bool {
	return len(qs.calls) > 0
}

// HasRowCount check whether all data is queried to use mst-level pre-aggregation.
func (qs *QuerySchema) HasRowCount() bool {
	// pre-aggregation is not used for exact statistic aggregation.
	if qs.Options().GetHintType() == hybridqp.ExactStatisticQuery {
		return false
	}

	if len(qs.origCalls) != 1 {
		return false
	}

	for _, c := range qs.origCalls {
		if c.Name != "count" {
			return false
		}
	}

	if qs.Options().GetCondition() != nil {
		return false
	}

	if qs.Options().HasInterval() {
		return false
	}

	if len(qs.Options().GetOptDimension()) > 0 {
		return false
	}
	return true
}

func (qs *QuerySchema) HasOptimizeAgg() bool {
	if qs.MatchPreAgg() {
		return true
	}
	if len(qs.Calls()) <= 0 {
		return false
	}
	return qs.HasOptimizeCall()
}

// CanSeqAggPushDown determines whether the csstore engine performs seqAgg optimization.
func (qs *QuerySchema) CanSeqAggPushDown() bool {
	// TODO: Open it after seqAgg is added to HybridStoreReader
	return false && qs.HasOptimizeCall() && len(qs.opt.GetDimensions()) == 0 && qs.opt.IsTimeSorted()
}

func (qs *QuerySchema) HasOptimizeCall() bool {
	for _, call := range qs.calls {
		if aggFunc := query.GetAggregateOperator(call.Name); aggFunc != nil && !aggFunc.OptimizeAgg() {
			return false
		}
	}
	return true
}

func (qs *QuerySchema) HasAuxTag() bool {
	for _, ref := range qs.Refs() {
		if ref.Type == influxql.Tag {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) HasNotInSeriesAgg() bool {
	for _, call := range qs.calls {
		if NotAggOnSeries[call.Name] {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) HasInSeriesAgg() bool {
	if qs.MatchPreAgg() || (!qs.HasNotInSeriesAgg() && len(qs.Calls()) > 0) {
		return true
	}

	return false
}

func (qs *QuerySchema) HasMath() bool {
	return len(qs.maths) > 0
}

func (qs *QuerySchema) HasString() bool {
	return len(qs.strings) > 0
}

func (qs *QuerySchema) HasLabelCalls() bool {
	return len(qs.labelCalls) > 0
}

func (qs *QuerySchema) HasPromTimeCalls() bool {
	return len(qs.promTimeCalls) > 0
}

func (qs *QuerySchema) HasGroupBy() bool {
	return len(qs.opt.GetGroupBy()) > 0
}

func (qs *QuerySchema) HasSubQuery() bool {
	src := qs.Sources()
	if src == nil {
		return false
	}

	if _, ok := src[0].(*influxql.SubQuery); ok {
		return true
	}
	return false
}

func (qs *QuerySchema) Sources() influxql.Sources {
	return qs.sources
}

func (qs *QuerySchema) HasNonPreCall() bool {
	for _, call := range qs.calls {
		if !GetPreAggregateCallMapping().Contains(call.Name) {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) CanLimitCut() bool {
	return qs.HasLimit() && !qs.HasCall() && !qs.HasFieldCondition() && qs.Options().FieldWildcard()
}

func (qs *QuerySchema) CountField() map[int]bool {
	fieldColIdx := make(map[int]bool)
	for i, f := range qs.queryFields {
		if c, ok := f.Expr.(*influxql.Call); ok && c.Name == "count" {
			fieldColIdx[i] = true
		}
	}
	return fieldColIdx
}

func (qs *QuerySchema) HasMeanCall() bool {
	for _, f := range qs.queryFields {
		if c, ok := f.Expr.(*influxql.Call); ok && c.Name == "mean" {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) HasBlankRowCall() bool {
	for _, f := range qs.queryFields {
		if c, ok := f.Expr.(*influxql.Call); ok &&
			(c.Name == "non_negative_difference" || c.Name == "non_negative_derivative") {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) HasPercentileOGSketch() bool {
	for _, f := range qs.queryFields {
		if c, ok := f.Expr.(*influxql.Call); ok && c.Name == PercentileOGSketch {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) CompositeCall() map[string]*hybridqp.OGSketchCompositeOperator {
	return qs.compositeCall
}

func (qs *QuerySchema) isPercentileOGSketch(call *influxql.Call) bool {
	if len(call.Args) == 0 {
		return false
	}

	return call.Name == PercentileOGSketch
}

func (qs *QuerySchema) rewritePercentileOGSketchCompositeCall() {
	for key, call := range qs.calls {
		if call.Name == PercentileOGSketch {
			cc := qs.compositeCall[key]

			io := cc.GetInsertOp()
			ioInRef, ok := io.Args[0].(*influxql.VarRef)
			if !ok {
				panic(fmt.Sprintf("the type of the %s should be *influxql.VarRef", io.Args[0].String()))
			}
			oriKey := fmt.Sprintf("%s::%s", ioInRef.Val, ioInRef.Type)
			inSymbolRef := qs.symbols[oriKey]
			ioInRef.Val = inSymbolRef.Val
			ioInRef.Type = inSymbolRef.Type

			qo := cc.GetQueryPerOp()
			qoOutRef := qs.mapping[qo]
			outSymbolRef := qs.symbols[key]
			qoOutRef.Val = outSymbolRef.Val
			qoOutRef.Type = outSymbolRef.Type
			qs.mapping[qo] = qoOutRef
		}
	}
}

func (qs *QuerySchema) genOGSketchOperator(call *influxql.Call) {
	// add the percentile_ogsketch call
	qs.addCall(call.String(), call)
	qs.mapSymbol(call.String(), call)

	// ogsketch insert
	OGSketchInsertCall, ok := influxql.CloneExpr(call).(*influxql.Call)
	if !ok {
		panic(fmt.Sprintf("the type of the %s should be a *influxql.Call", call.String()))
	}
	OGSketchInsertCall.Name = OGSketchInsert
	qs.notIncI = true
	qs.mapSymbol(OGSketchInsertCall.String(), OGSketchInsertCall)
	qs.notIncI = false

	// ogsketch merge
	OGSketchMergeCall, ok := influxql.CloneExpr(call).(*influxql.Call)
	if !ok {
		panic(fmt.Sprintf("the type of the %s should be a *influxql.Call", call.String()))
	}
	insetRef := qs.mapping[OGSketchInsertCall]
	OGSketchMergeCall.Args[0].(*influxql.VarRef).Val = insetRef.Val
	OGSketchMergeCall.Args[0].(*influxql.VarRef).Type = insetRef.Type
	OGSketchMergeCall.Name = OGSketchMerge
	qs.mapSymbol(OGSketchMergeCall.String(), OGSketchMergeCall)

	// ogsketch query
	OGSketchPercentileCall, ok := influxql.CloneExpr(call).(*influxql.Call)
	if !ok {
		panic(fmt.Sprintf("the type of the %s should be a *influxql.Call", call.String()))
	}
	mergeRef := qs.mapping[OGSketchMergeCall]
	OGSketchPercentileCall.Args[0].(*influxql.VarRef).Val = mergeRef.Val
	OGSketchPercentileCall.Args[0].(*influxql.VarRef).Type = mergeRef.Type
	OGSketchPercentileCall.Name = OGSketchPercentile
	qs.mapSymbol(OGSketchPercentileCall.String(), OGSketchPercentileCall)

	operator := hybridqp.NewOGSketchCompositeOperator(OGSketchInsertCall, OGSketchMergeCall, OGSketchPercentileCall)
	qs.addCompositeCall(call.String(), operator)
}

func (qs *QuerySchema) HasInterval() bool {
	return qs.opt.HasInterval()
}

func (qs *QuerySchema) InitFieldCondition() {
	if qs.opt.GetCondition() == nil {
		qs.hasFieldCondition = false
		return
	}

	v := NewConditionExprVisitor()
	influxql.Walk(v, qs.opt.GetCondition())

	qs.hasFieldCondition = v.hasField
}

func (qs *QuerySchema) HasFieldCondition() bool {
	return qs.hasFieldCondition
}

func (qs *QuerySchema) IsMultiMeasurements() bool {
	if qs.sources == nil {
		return false
	}

	if len(qs.sources) < 1 {
		return false
	}

	if _, ok := qs.sources[0].(*influxql.Measurement); !ok {
		return false
	}

	return true
}

func (qs *QuerySchema) AddTable(m *influxql.Measurement, refs []influxql.VarRef) {
	if _, ok := qs.tables[m.Name]; !ok {
		qs.tables[m.Name] = NewQueryTable(m, refs)
	}
}

func (qs *QuerySchema) Options() hybridqp.Options {
	return qs.opt
}

// PromResetTime is used to determine whether to set the time of result to the end time of the query,
// according to the semantics of the prom instant query,
func (qs *QuerySchema) PromResetTime() bool {
	return qs.opt.IsPromInstantQuery() && (qs.opt.GetPromRange() == 0 || (qs.opt.GetPromRange() > 0 && len(qs.Calls()) > 0))
}

func (qs *QuerySchema) Table(name string) *QueryTable {
	return qs.tables[name]
}

func (qs *QuerySchema) Symbols() map[string]influxql.VarRef {
	return qs.symbols
}

func (qs *QuerySchema) Mapping() map[influxql.Expr]influxql.VarRef {
	return qs.mapping
}

func (qs *QuerySchema) Refs() map[string]*influxql.VarRef {
	return qs.refs
}

func (qs *QuerySchema) DerivedOrigCall(call *influxql.Call) influxql.VarRef {
	c := qs.calls[call.String()]
	return qs.mapping[c]
}

func (qs *QuerySchema) DerivedRef(ref *influxql.VarRef) influxql.VarRef {
	r := qs.refs[ref.String()]
	return qs.mapping[r]
}

func (qs *QuerySchema) MakeRefs() []influxql.VarRef {
	refs := make([]influxql.VarRef, 0, len(qs.refs))
	for _, ref := range qs.refs {
		refs = append(refs, *ref)
	}
	return refs
}

func (qs *QuerySchema) OrigCalls() map[string]*influxql.Call {
	return qs.origCalls
}

func (qs *QuerySchema) SetOpt(opt hybridqp.Options) {
	qs.opt = opt
}

func (qs *QuerySchema) Calls() map[string]*influxql.Call {
	return qs.calls
}

func (qs *QuerySchema) SlidingWindow() map[string]*influxql.Call {
	return qs.slidingWindow
}

func (qs *QuerySchema) HoltWinters() []*influxql.Field {
	return qs.holtWinters
}

func (qs *QuerySchema) SetHoltWinters(calls []*influxql.Call) {
	for _, call := range calls {
		f := &influxql.Field{
			Expr:  call,
			Alias: "",
		}
		qs.holtWinters = append(qs.holtWinters, f)
	}
}

func (qs *QuerySchema) Binarys() map[string]*influxql.BinaryExpr {
	return qs.binarys
}

func (qs *QuerySchema) Fields() influxql.Fields {
	return qs.fields
}

func (qs *QuerySchema) FieldsRef() influxql.VarRefs {
	return qs.fieldsRef
}

func (qs *QuerySchema) IsHoltWinters(val string) bool {
	for _, hw := range qs.HoltWinters() {
		if hw.Alias == val {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) isCountDistinct(call *influxql.Call) bool {
	if len(call.Args) == 0 {
		return false
	}

	if c, ok := call.Args[0].(*influxql.Call); ok && c.Name == "distinct" {
		return true
	}

	return false
}

func (qs *QuerySchema) CountDistinct() *influxql.Call {
	return qs.countDistinct
}

func (qs *QuerySchema) HasCastorCall() bool {
	for _, v := range qs.calls {
		if v.Name == "castor" {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) isMathFunction(call *influxql.Call) bool {
	if mathFunc := query.GetMaterializeFunction(call.Name, query.MATH); mathFunc != nil {
		return true
	}
	return false
}

func (qs *QuerySchema) isStringFunction(call *influxql.Call) bool {
	if stringFunc := query.GetMaterializeFunction(call.Name, query.STRING); stringFunc != nil {
		return true
	}
	return false
}

func (qs *QuerySchema) isLabelFunction(call *influxql.Call) bool {
	if labelFunc := query.GetLabelFunction(call.Name); labelFunc != nil {
		return true
	}
	return false
}

func (qs *QuerySchema) isPromTimeFunction(call *influxql.Call) bool {
	if promTimeFunc := query.GetPromTimeFunction(call.Name); promTimeFunc != nil {
		return true
	}
	return false
}

func (qs *QuerySchema) AddString(key string, str *influxql.Call) {
	_, ok := qs.strings[key]

	if !ok {
		qs.strings[key] = str
	}
}

func (qs *QuerySchema) isSlidingWindow(call *influxql.Call) bool {
	switch call.Name {
	case "sliding_window":
		return true
	}
	return false
}

func (qs *QuerySchema) AddSlidingWindow(key string, str *influxql.Call) {
	_, ok := qs.slidingWindow[key]

	if !ok {
		qs.slidingWindow[key] = str
	}
}

func (qs *QuerySchema) AddHoltWinters(call *influxql.Call, alias string) {
	f := &influxql.Field{
		Expr:  call,
		Alias: alias,
	}
	qs.holtWinters = append(qs.holtWinters, f)
}

func (qs *QuerySchema) Visit(n influxql.Node) influxql.Visitor {
	expr, ok := n.(influxql.Expr)
	if !ok {
		return qs
	}

	key := expr.String()

	switch n := n.(type) {
	case *influxql.BinaryExpr:
		qs.addBinary(key, n)
		return qs
	case *influxql.Call:
		if qs.isSlidingWindow(n) {
			qs.AddSlidingWindow(key, n)
			qs.mapSymbol(key, expr)
			return qs
		}
		if qs.isMathFunction(n) || op.IsProjectOp(n) {
			qs.AddMath(key, n)
			return qs
		}
		if qs.isStringFunction(n) {
			qs.AddString(key, n)
			return qs
		}
		if qs.isLabelFunction(n) {
			qs.AddLabelCalls(key, n)
			return qs
		}

		if qs.isPromTimeFunction(n) {
			qs.AddPromTimeCalls(key, n)
			return qs
		}

		if qs.isCountDistinct(n) {
			qs.countDistinct = n
			qs.mapSymbol(key, expr)
			return qs
		}

		if qs.isPercentileOGSketch(n) {
			qs.genOGSketchOperator(n)
			return qs
		}

		qs.addCall(key, n)
		qs.mapSymbol(key, expr)
		return qs
	case *influxql.VarRef:
		if n.Val == promql2influxql.ArgNameOfTimeFunc {
			return nil
		}
		qs.addRef(key, n)
		qs.mapSymbol(key, expr)
		return nil
	default:
		return qs
	}
}

func (qs *QuerySchema) addRef(key string, ref *influxql.VarRef) {
	_, ok := qs.refs[key]

	if !ok {
		qs.refs[key] = ref
	}
}

func (qs *QuerySchema) addCall(key string, call *influxql.Call) {
	_, ok := qs.calls[key]

	if !ok {
		qs.origCalls[key], ok = influxql.CloneExpr(call).(*influxql.Call)
		if !ok {
			panic("QuerySchema addCall call isn't *influxql.Call")
		}
		qs.calls[key] = call
	}
}

func (qs *QuerySchema) addCompositeCall(key string, operator *hybridqp.OGSketchCompositeOperator) {
	_, ok := qs.compositeCall[key]

	if !ok {
		qs.compositeCall[key] = operator
	}
}

func (qs *QuerySchema) AddMath(key string, math *influxql.Call) {
	_, ok := qs.maths[key]

	if !ok {
		qs.maths[key] = math
	}
}

func (qs *QuerySchema) AddLabelCalls(key string, labelCalls *influxql.Call) {
	_, ok := qs.labelCalls[key]

	if !ok {
		qs.labelCalls[key] = labelCalls
	}
}

func (qs *QuerySchema) AddPromTimeCalls(key string, promTimeCalls *influxql.Call) {
	_, ok := qs.promTimeCalls[key]

	if !ok {
		qs.promTimeCalls[key] = promTimeCalls
	}
}

func (qs *QuerySchema) addBinary(key string, binary *influxql.BinaryExpr) {
	_, ok := qs.binarys[key]

	if !ok {
		qs.binarys[key] = binary
	}
}

func (qs *QuerySchema) mapSymbol(key string, expr influxql.Expr) {
	symbol, ok := qs.symbols[key]

	if !ok {
		symbolName := fmt.Sprintf("val%d", qs.i)
		typ, err := qs.deriveType(expr)
		if err != nil {
			if errno.Equal(err, errno.DtypeNotSupport) {
				panic(err)
			}
			panic(fmt.Errorf("QuerySchema mapSymbol get derive type failed, %v", err.Error()))
		}
		if qs.opt.IsPromQuery() {
			typ = influxql.Float
		}
		symbol = influxql.VarRef{
			Val:  symbolName,
			Type: typ,
		}

		// Assign this symbol to the symbol table if it is not presently there
		// and increment the value index number.
		qs.symbols[key] = symbol
		if !qs.notIncI {
			qs.i++
		}
	}

	qs.mapping[expr] = symbol
}

func (qs *QuerySchema) GetFieldType(i int) (int64, error) {
	t, e := qs.deriveType(qs.queryFields[i].Expr)
	return int64(t), e
}

func (qs *QuerySchema) deriveType(expr influxql.Expr) (influxql.DataType, error) {
	if typ, ok := qs.mapDeriveType[expr]; ok {
		return typ, nil
	}

	valuer := influxql.TypeValuerEval{
		TypeMapper: DefaultTypeMapper,
	}

	return valuer.EvalType(expr, false)
}

func (qs *QuerySchema) rewriteExpr(expr influxql.Expr) influxql.Expr {
	symbol, ok := qs.mapping[expr]
	if !ok {
		return expr
	}
	return &symbol
}

func (qs *QuerySchema) OnlyOneCallRef() *influxql.VarRef {
	if len(qs.Calls()) == 1 && len(qs.Fields()) > 1 {
		if qs.MatchPreAgg() {
			for i := range qs.queryFields {
				f := qs.queryFields[i]
				if call, ok := f.Expr.(*influxql.Call); ok {
					return call.Args[0].(*influxql.VarRef)
				}
			}
		}
		for _, c := range qs.calls {
			return c.Args[0].(*influxql.VarRef)
		}
	}
	return nil
}

func (qs *QuerySchema) LimitType() hybridqp.LimitType {
	for _, call := range qs.calls {
		switch call.Name {
		case "top", "bottom":
			return hybridqp.MultipleRowsIgnoreTagLimit
		default:
			continue
		}
	}

	return hybridqp.SingleRowIgnoreTagLimit
}

func (qs *QuerySchema) HasLimit() bool {
	return qs.opt.GetLimit()+qs.opt.GetOffset() > 0
}

func (qs *QuerySchema) LimitAndOffset() (int, int) {
	if qs.opt.GetLimit()+qs.opt.GetOffset() == 0 {
		return qs.opt.GetLimit(), qs.opt.GetOffset()
	}

	if qs.opt.GetLimit() == 0 {
		return 1, qs.opt.GetOffset()
	}

	return qs.opt.GetLimit(), qs.opt.GetOffset()
}

func (qs *QuerySchema) MatchPreAgg() bool {
	if !config.GetCommon().PreAggEnabled {
		return false
	}

	if !qs.HasCall() {
		return false
	}

	if qs.HasNonPreCall() {
		return false
	}

	if qs.HasInterval() {
		return false
	}

	if qs.HasFieldCondition() {
		return false
	}

	if qs.Options().IsPromQuery() {
		return false
	}
	return true
}

func (qs *QuerySchema) CanCallsPushdown() bool {
	for _, call := range qs.calls {
		if aggFunc := query.GetAggregateOperator(call.Name); aggFunc != nil && !aggFunc.CanPushDown() {
			return false
		}
	}
	return true
}

func (qs *QuerySchema) CanAggPushDown() bool {
	return !(qs.HasMath() || qs.HasString() || qs.HasLabelCalls() || qs.HasPromTimeCalls() || !qs.CanCallsPushdown())
}

// CanAggTagSet indicates that aggregation is performed among multiple TagSets. File traversal and SeqAgg optimization are used.
func (qs *QuerySchema) CanAggTagSet() bool {
	return qs.HasCall() && qs.CanCallsPushdown() && !qs.ContainSeriesIgnoreCall() && !qs.Options().IsRangeVectorSelector()
}

func (qs *QuerySchema) ContainSeriesIgnoreCall() bool {
	for _, call := range qs.calls {
		// UDAF can not sink into the series
		if op.IsUDAFOp(call) {
			return true
		}
		if call.Name == PercentileOGSketch {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) isRefInBinaryExpr(be *influxql.BinaryExpr, ref *influxql.VarRef) bool {
	if ok := qs.matchExpr(be.LHS, ref); ok {
		return true
	}

	if ok := qs.matchExpr(be.RHS, ref); ok {
		return true
	}

	return false
}

func (qs *QuerySchema) isRefInRef(fref *influxql.VarRef, ref *influxql.VarRef) bool {
	return fref.Val == ref.Val
}

func (qs *QuerySchema) IsRefInQueryFields(ref *influxql.VarRef) bool {
	for _, field := range qs.queryFields {
		if ok := qs.matchExpr(field.Expr, ref); ok {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) IsRefInSymbolFields(ref *influxql.VarRef) bool {
	for _, field := range qs.fields {
		if ok := qs.matchExpr(field.Expr, ref); ok {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) matchExpr(expr influxql.Expr, ref *influxql.VarRef) bool {
	switch n := expr.(type) {
	case *influxql.BinaryExpr:
		return qs.isRefInBinaryExpr(n, ref)
	case *influxql.VarRef:
		return qs.isRefInRef(n, ref)
	default:
		return false
	}
}

func (qs *QuerySchema) IsTimeZero() bool {
	var zero bool
	if qs.opt.HasInterval() {
		return zero
	}
	if len(qs.calls) == 1 {
		for i := range qs.calls {
			if qs.calls[i].Name == "count" || qs.calls[i].Name == "sum" || qs.calls[i].Name == "mean" {
				zero = true
				return zero
			}
		}
		return zero
	} else if len(qs.calls) == 0 {
		return zero
	}
	zero = true
	return zero
}

func (qs *QuerySchema) HasStreamCall() bool {
	for _, call := range qs.calls {
		if aggFunc := query.GetAggregateOperator(call.Name); aggFunc != nil && aggFunc.SortedMergeCall() {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) HasSlidingWindowCall() bool {
	for _, call := range qs.slidingWindow {
		if call.Name == "sliding_window" {
			return true
		}
	}
	return false
}

func (qs *QuerySchema) HasHoltWintersCall() bool {
	return len(qs.holtWinters) > 0
}

func (qs *QuerySchema) BuildDownSampleSchema(addPrefix bool) record.Schemas {
	var outSchema record.Schemas
	for _, f := range qs.origCalls {
		c, ok := f.Args[0].(*influxql.VarRef)
		if !ok {
			continue
		}
		var field record.Field
		if addPrefix {
			field = record.Field{Name: f.Name + "_" + c.Val}
		} else {
			field = record.Field{Name: c.Val}
		}
		switch f.Name {
		case "min", "first", "last", "max", "sum":
			field.Type = record.ToModelTypes(c.Type)
		case "count":
			field.Type = influx.Field_Type_Int
		default:
			panic("wrong call")
		}
		outSchema = append(outSchema, field)
	}
	outSchema = append(outSchema, record.Field{
		Name: "time",
		Type: influx.Field_Type_Int,
	})
	return outSchema
}

func (qs *QuerySchema) HasExcatLimit() bool {
	return (!config.GetCommon().PreAggEnabled || qs.Options().GetHintType() == hybridqp.ExactStatisticQuery) &&
		qs.HasLimit() && !qs.HasOptimizeAgg()
}

func (qs *QuerySchema) GetSourcesNames() []string {
	return qs.opt.GetSourcesNames()
}

func (qs *QuerySchema) GetJoinCaseCount() int {
	return len(qs.joinCases)
}

func (qs *QuerySchema) GetJoinCases() []*influxql.Join {
	return qs.joinCases
}

func (qs *QuerySchema) HasSort() bool {
	return qs.opt != nil && len(qs.opt.GetSortFields()) > 0
}

func (qs *QuerySchema) GetSortFields() influxql.SortFields {
	if qs.opt == nil {
		return nil
	}
	return qs.opt.GetSortFields()
}

func (qs *QuerySchema) SetFill(fill influxql.FillOption) {
	qs.opt.SetFill(fill)
}

func (qs *QuerySchema) SetPlanType(planType PlanType) {
	qs.planType = planType
}

func (qs *QuerySchema) GetPlanType() PlanType {
	return qs.planType
}

func (qs *QuerySchema) SetUnnests(unnests []*influxql.Unnest) {
	qs.unnestCases = unnests
}

func (qs *QuerySchema) GetUnnests() influxql.Unnests {
	return qs.unnestCases
}

func (qs *QuerySchema) HasUnnests() bool {
	return len(qs.unnestCases) > 0
}

func (qs *QuerySchema) GetTimeRangeByTC() util.TimeRange {
	startTime, endTime := qs.Options().GetStartTime(), qs.Options().GetEndTime()
	var interval int64
	if indexR := qs.Options().GetMeasurements()[0].IndexRelation; indexR != nil {
		interval = indexR.GetTimeClusterDuration()
	}
	return util.TimeRange{Min: window(startTime, interval), Max: window(endTime, interval)}
}

// window used to calculate the time point that belongs to a specific time window.
func window(t, window int64) int64 {
	if t == influxql.MinTime || t == influxql.MaxTime || window == 0 {
		return t
	}
	return t - t%window
}
