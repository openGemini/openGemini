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

package hybridqp

import (
	"context"
	"sync"

	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type LimitType int

const (
	//SingleRowLimit means single row represents one Point and return number of limit points Per Tag.
	SingleRowLimit LimitType = 1
	//MultipleRowsLimit means single row represents one Point and return number of limit points Per Tag.
	MultipleRowsLimit LimitType = 2
	//SingleRowIgnoreTagLimit means single row represents one Point and return only number of limit points.
	SingleRowIgnoreTagLimit LimitType = 3
	//MultipleRowsIgnoreTagLimit means single row represents one Point and return only number of limit points.
	MultipleRowsIgnoreTagLimit LimitType = 4
)

type RowDataType interface {
	Aux() []int
	Equal(RowDataType) bool
	Fields() influxql.Fields
	MakeRefs() []influxql.VarRef
	Field(int) *influxql.Field
	FieldIndex(string) int
	NumColumn() int
	SlimFields(ridIdx []int) RowDataType
	CopyTo(dst *RowDataTypeImpl)

	IndexByName() map[string]int
	UpdateByDownSampleFields(map[string]string)
	SetDataType(i int, t influxql.DataType)
}

type RowDataTypeImpl struct {
	aux         []int
	fields      influxql.Fields
	indexByName map[string]int
}

func NewRowDataTypeImpl(refs ...influxql.VarRef) *RowDataTypeImpl {
	rowDataType := &RowDataTypeImpl{
		fields:      make(influxql.Fields, len(refs)),
		indexByName: make(map[string]int),
	}

	for i := range refs {
		ref := refs[i]
		rowDataType.fields[i] = &influxql.Field{Expr: &ref}
	}

	for i, f := range rowDataType.fields {
		rowDataType.indexByName[f.Name()] = i
	}

	return rowDataType
}

func (s *RowDataTypeImpl) SetDataType(i int, t influxql.DataType) {
	if i >= len(s.fields) {
		return
	}
	if val, ok := s.fields[i].Expr.(*influxql.VarRef); ok {
		val.SetDataType(t)
	}
}

func (s *RowDataTypeImpl) UpdateByDownSampleFields(m map[string]string) {
	tempMap := make(map[string]int)
	for k, v := range s.Fields() {
		currVal := v.Expr.(*influxql.VarRef).Val
		originVal := s.Fields()[k].Expr.(*influxql.VarRef).Val
		s.Fields()[k].Expr.(*influxql.VarRef).Val = m[currVal]
		s.NumColumn()
		tempMap[m[currVal]] = s.indexByName[originVal]
	}
	s.indexByName = tempMap
}

func (s *RowDataTypeImpl) DeepEqual(to *RowDataTypeImpl) bool {
	if to == nil {
		return false
	}

	for i := range s.fields {
		if !s.fields[i].Equal(to.fields[i]) {
			return false
		}
	}

	return true
}

func (s *RowDataTypeImpl) Aux() []int {
	return s.aux
}

func (s *RowDataTypeImpl) Equal(to RowDataType) bool {
	if t, ok := to.(*RowDataTypeImpl); !ok {
		return false
	} else {
		return s.DeepEqual(t)
	}
}

func (s *RowDataTypeImpl) Fields() influxql.Fields {
	return s.fields
}

func (s *RowDataTypeImpl) MakeRefs() []influxql.VarRef {
	refs := make([]influxql.VarRef, 0, len(s.fields))
	for _, f := range s.fields {
		refs = append(refs, *(f.Expr.(*influxql.VarRef)))
	}
	return refs
}

func (s *RowDataTypeImpl) Field(i int) *influxql.Field {
	return s.fields[i]
}

func (s *RowDataTypeImpl) SetIndexByName(m map[string]int) {
	s.indexByName = m
}

func (s *RowDataTypeImpl) IndexByName() map[string]int {
	return s.indexByName
}

func (s *RowDataTypeImpl) FieldIndex(name string) int {
	if index, ok := s.indexByName[name]; ok {
		return index
	}
	return -1
}

func (s *RowDataTypeImpl) NumColumn() int {
	return len(s.fields)
}

func (s *RowDataTypeImpl) SlimFields(ridIdx []int) RowDataType {
	for i, idx := range ridIdx {
		s.fields = append(s.fields[0:idx-i], s.fields[idx-i+1:]...)
	}
	return s
}

func (s *RowDataTypeImpl) CopyTo(dst *RowDataTypeImpl) {
	if cap(dst.aux) < len(s.aux) {
		dst.aux = make([]int, len(s.aux))
	} else {
		dst.aux = dst.aux[:0]
	}
	copy(dst.aux, s.aux)

	if cap(dst.fields) < len(s.fields) {
		dst.fields = make(influxql.Fields, len(s.fields))
	} else {
		dst.fields = dst.fields[:len(s.fields)]
	}
	for i, field := range s.Fields() {
		dst.fields[i] = &influxql.Field{
			Expr:  influxql.CloneExpr(field.Expr),
			Alias: field.Alias,
		}
	}
	if dst.indexByName == nil {
		dst.indexByName = make(map[string]int, len(s.indexByName))
	}
	for k, v := range s.indexByName {
		dst.indexByName[k] = v
	}
}

type Catalog interface {
	GetColumnNames() []string
	GetQueryFields() influxql.Fields
	CloneField(f *influxql.Field) *influxql.Field
	HasCall() bool
	HasRowCount() bool
	HasMath() bool
	HasString() bool
	HasNonPreCall() bool
	CountField() map[int]bool
	HasMeanCall() bool
	HasBlankRowCall() bool
	HasInterval() bool
	HasFieldCondition() bool
	HasAuxTag() bool
	IsPromNestedCall(call *influxql.Call) bool
	HasPercentileOGSketch() bool
	HasPromNestedCall() bool
	Options() Options
	PromResetTime() bool
	Symbols() map[string]influxql.VarRef
	Mapping() map[influxql.Expr]influxql.VarRef
	Refs() map[string]*influxql.VarRef
	DerivedOrigCall(call *influxql.Call) influxql.VarRef
	DerivedRef(ref *influxql.VarRef) influxql.VarRef
	MakeRefs() []influxql.VarRef
	OrigCalls() map[string]*influxql.Call
	SetOpt(opt Options)
	Calls() map[string]*influxql.Call
	SlidingWindow() map[string]*influxql.Call
	HoltWinters() []*influxql.Field
	CompositeCall() map[string]*OGSketchCompositeOperator
	PromNestedCall() map[string]*PromNestedCall
	Binarys() map[string]*influxql.BinaryExpr
	Fields() influxql.Fields
	FieldsRef() influxql.VarRefs
	CountDistinct() *influxql.Call
	OnlyOneCallRef() *influxql.VarRef
	LimitType() LimitType
	HasLimit() bool
	HasCastorCall() bool
	LimitAndOffset() (int, int)
	MatchPreAgg() bool
	HasInSeriesAgg() bool
	CanSeqAggPushDown() bool
	CanCallsPushdown() bool
	CanAggPushDown() bool
	CanAggTagSet() bool
	ContainSeriesIgnoreCall() bool
	IsRefInQueryFields(ref *influxql.VarRef) bool
	IsRefInSymbolFields(ref *influxql.VarRef) bool
	IsTimeZero() bool
	HasStreamCall() bool
	HasSlidingWindowCall() bool
	HasHoltWintersCall() bool
	IsMultiMeasurements() bool
	HasGroupBy() bool
	Sources() influxql.Sources
	HasSubQuery() bool
	HasOptimizeAgg() bool
	HasOptimizeCall() bool
	GetSourcesNames() []string
	GetFieldType(i int) (int64, error)
	GetJoinCaseCount() int
	GetJoinCases() []*influxql.Join
	IsHoltWinters(val string) bool
	GetSortFields() influxql.SortFields
	SetUnnests(unnests []*influxql.Unnest)
	GetUnnests() influxql.Unnests
	HasUnnests() bool
	GetTimeRangeByTC() util.TimeRange
	GetPromCalls() []*influxql.PromSubCall
}

type CatalogCreator interface {
	Create(fields influxql.Fields, columnNames []string, opt Options) Catalog
}

func RegistryCatalogCreator(creator CatalogCreator) bool {
	factory := GetCatalogFactoryInstance()

	factory.Attach(creator)

	return true
}

type CatalogCreatorFactory struct {
	creator CatalogCreator
}

func NewCatalogCreatorFactory() *CatalogCreatorFactory {
	return &CatalogCreatorFactory{
		creator: nil,
	}
}

func (r *CatalogCreatorFactory) Attach(creator CatalogCreator) {
	r.creator = creator
}

func (r *CatalogCreatorFactory) Create(fields influxql.Fields, columnNames []string, opt Options) Catalog {
	return r.creator.Create(fields, columnNames, opt)
}

var instanceCatalog *CatalogCreatorFactory
var onceCatalog sync.Once

func GetCatalogFactoryInstance() *CatalogCreatorFactory {
	onceCatalog.Do(func() {
		instanceCatalog = NewCatalogCreatorFactory()
	})

	return instanceCatalog
}

type StoreEngine interface {
	ReportLoad()
	CreateLogicPlan(ctx context.Context, db string, ptId uint32, shardID uint64, sources influxql.Sources, schema Catalog) (QueryNode, error)
	ScanWithSparseIndex(ctx context.Context, db string, ptId uint32, shardIDS []uint64, schema Catalog) (IShardsFragments, error)
	GetIndexInfo(db string, ptId uint32, shardID uint64, schema Catalog) (interface{}, error)
	RowCount(db string, ptId uint32, shardIDS []uint64, schema Catalog) (int64, error)
	UnrefEngineDbPt(db string, ptId uint32)
	GetShardDownSampleLevel(db string, ptId uint32, shardID uint64) int
}

type IShardsFragments interface{}

type OGSketchCompositeOperator struct {
	insertOp   *influxql.Call
	mergeOp    *influxql.Call
	queryPerOp *influxql.Call
}

func NewOGSketchCompositeOperator(i, m, qp *influxql.Call) *OGSketchCompositeOperator {
	return &OGSketchCompositeOperator{insertOp: i, mergeOp: m, queryPerOp: qp}
}

func (o *OGSketchCompositeOperator) GetInsertOp() *influxql.Call {
	return o.insertOp
}

func (o *OGSketchCompositeOperator) GetMergeOp() *influxql.Call {
	return o.mergeOp
}

func (o *OGSketchCompositeOperator) GetQueryPerOp() *influxql.Call {
	return o.queryPerOp
}

type PromNestedCall struct {
	aggCall  *influxql.Call
	funcCall *influxql.Call
}

func NewPromNestedCall(fc, ac *influxql.Call) *PromNestedCall {
	return &PromNestedCall{funcCall: fc, aggCall: ac}
}

func (c *PromNestedCall) GetAggCall() *influxql.Call {
	return c.aggCall
}

func (c *PromNestedCall) GetFuncCall() *influxql.Call {
	return c.funcCall
}
