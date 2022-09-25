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
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

var NotAggOnSeries = map[string]bool{
	"percentile": true,
	"difference": true, "non_negative_difference": true,
	"derivative": true, "non_negative_derivative": true,
	"rate": true, "irate": true, "absent": true, "stddev": true, "mode": true, "median": true,
	"elapsed": true, "moving_average": true, "cumulative_sum": true, "integral": true, "sample": true,
	"sliding_window": true,
}

var DefaultTypeMapper = influxql.MultiTypeMapper(
	op.TypeMapper{},
	query.MathTypeMapper{},
	query.FunctionTypeMapper{},
	query.StringFunctionTypeMapper{},
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
	s := NewQuerySchema(fields, columnNames, opt)
	return s
}

var _ = hybridqp.RegistryCatalogCreator(&QuerySchemaCreator{})

type QuerySchema struct {
	tables        map[string]*QueryTable
	queryFields   influxql.Fields
	columnNames   []string
	fields        influxql.Fields
	fieldsMap     map[string]*influxql.Field
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
	slidingWindow map[string]*influxql.Call
	i             int
	sources       influxql.Sources
	// Options is interface now, it must be cloned in internal
	opt hybridqp.Options
}

func NewQuerySchema(fields influxql.Fields, columnNames []string, opt hybridqp.Options) *QuerySchema {
	schema := &QuerySchema{
		tables:        make(map[string]*QueryTable),
		queryFields:   fields,
		columnNames:   columnNames,
		fields:        make(influxql.Fields, 0, len(fields)),
		fieldsMap:     make(map[string]*influxql.Field),
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
		slidingWindow: make(map[string]*influxql.Call),
		i:             0,
		opt:           opt,
		sources:       nil,
	}

	schema.init()

	return schema
}

func NewQuerySchemaWithSources(fields influxql.Fields, sources influxql.Sources, columnNames []string, opt hybridqp.Options) *QuerySchema {
	schema := &QuerySchema{
		tables:        make(map[string]*QueryTable),
		queryFields:   fields,
		columnNames:   columnNames,
		fields:        make(influxql.Fields, 0, len(fields)),
		fieldsMap:     make(map[string]*influxql.Field),
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
		slidingWindow: make(map[string]*influxql.Call),
		i:             0,
		opt:           opt,
		sources:       sources,
	}

	schema.init()
	if !schema.Options().IsAscending() && schema.MatchPreAgg() && len(schema.opt.GetGroupBy()) == 0 {
		schema.Options().SetAscending(true)
	}

	return schema
}

func (qs *QuerySchema) reset(fields influxql.Fields, column []string) {
	qs.queryFields = fields
	qs.columnNames = column
	qs.fields = make(influxql.Fields, 0, len(fields))
	qs.fieldsMap = make(map[string]*influxql.Field)
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
	qs.slidingWindow = make(map[string]*influxql.Call)
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
			}
		}
		clone.Expr = qs.rewriteBaseCallTransformExprCall(clone.Expr)
		influxql.Walk(qs, clone.Expr)
		clone.Expr = influxql.RewriteExpr(clone.Expr, qs.rewriteExpr)
		qs.fields = append(qs.fields, clone)
		qs.fieldsMap[clone.String()] = clone
	}

	for i, f := range qs.fields {
		f.Alias = qs.columnNames[i]
		typ, err := qs.deriveType(f.Expr)
		if err != nil {
			panic(fmt.Sprintf("derive type from %v failed, %v", f.Expr, err.Error()))
		}
		qs.fieldsRef = append(qs.fieldsRef, influxql.VarRef{Val: f.Name(), Type: typ})
	}
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
		return &influxql.BinaryExpr{Op: expr.Op, LHS: qs.rewriteBaseCallTransformExprCall(expr.LHS), RHS: qs.rewriteBaseCallTransformExprCall(expr.RHS)}
	case *influxql.Call:
		if expr.Name == "mean" {
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
		}
		return influxql.CloneExpr(expr)
	default:
		return influxql.CloneExpr(expr)
	}
}

func (qs *QuerySchema) meanToSumDivCount(call *influxql.Call) influxql.Expr {
	lhs := &influxql.Call{Name: "sum", Args: nil}
	lhs.Args = append(lhs.Args, influxql.CloneExpr(call.Args[0]))
	rhs := &influxql.Call{Name: "count", Args: nil}
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
	return qs.HasLimit() && !qs.HasCall() && !qs.HasFieldCondition()
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

func (qs *QuerySchema) HasInterval() bool {
	return qs.opt.HasInterval()
}

func (qs *QuerySchema) HasFieldCondition() bool {
	if qs.opt.GetCondition() == nil {
		return false
	}

	v := NewConditionExprVisitor()
	influxql.Walk(v, qs.opt.GetCondition())

	return v.hasField
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

func (qs *QuerySchema) Binarys() map[string]*influxql.BinaryExpr {
	return qs.binarys
}

func (qs *QuerySchema) Fields() influxql.Fields {
	return qs.fields
}

func (qs *QuerySchema) FieldsMap() map[string]*influxql.Field {
	return qs.fieldsMap
}

func (qs *QuerySchema) FieldsRef() influxql.VarRefs {
	return qs.fieldsRef
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

func (qs *QuerySchema) isMathFunction(call *influxql.Call) bool {
	switch call.Name {
	case "abs", "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "exp", "log", "ln", "log2", "log10", "sqrt", "pow", "floor", "ceil", "round":
		return true
	}
	return false
}

func (qs *QuerySchema) isStringFunction(call *influxql.Call) bool {
	switch call.Name {
	case "str", "strlen", "substr":
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

		if qs.isCountDistinct(n) {
			qs.countDistinct = n
			qs.mapSymbol(key, expr)
			return qs
		}

		qs.addCall(key, n)
		qs.mapSymbol(key, expr)
		return qs
	case *influxql.VarRef:
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

func (qs *QuerySchema) AddMath(key string, math *influxql.Call) {
	_, ok := qs.maths[key]

	if !ok {
		qs.maths[key] = math
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
			panic(fmt.Errorf("QuerySchema mapSymbol get derive type failed, %v", err.Error()))
		}

		symbol = influxql.VarRef{
			Val:  symbolName,
			Type: typ,
		}

		// Assign this symbol to the symbol table if it is not presently there
		// and increment the value index number.
		qs.symbols[key] = symbol
		qs.i++
	}

	qs.mapping[expr] = symbol
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

	return true
}

func (qs *QuerySchema) CanCallsPushdown() bool {
	for _, call := range qs.calls {
		if mergeCall[call.Name] || sortedMergeCall[call.Name] {
			return false
		}
	}
	return true
}

func (qs *QuerySchema) CanAggPushDown() bool {
	return !(qs.HasMath() || qs.HasString() || !qs.CanCallsPushdown())
}

func (qs *QuerySchema) ContainSeriesIgnoreCall() bool {
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
		if sortedMergeCall[call.Name] {
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
