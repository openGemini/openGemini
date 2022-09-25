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

package hybridqp

import (
	"context"
	"sync"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
	internal "github.com/openGemini/openGemini/open_src/influx/query/proto"
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

	Marshal() *internal.RowDataType
	Unmarshal(*internal.RowDataType) error
}

type RowDataTypeImpl struct {
	aux         []int
	fields      influxql.Fields
	indexByName map[string]int
}

func NewRowDataTypeImpl(refs ...influxql.VarRef) *RowDataTypeImpl {
	rowDataType := &RowDataTypeImpl{
		fields:      make(influxql.Fields, 0, len(refs)),
		indexByName: make(map[string]int),
	}

	for _, ref := range refs {
		r := ref
		rowDataType.fields = append(rowDataType.fields, &influxql.Field{Expr: &r})
	}

	for i, f := range rowDataType.fields {
		rowDataType.indexByName[f.Name()] = i
	}

	return rowDataType
}

func (s *RowDataTypeImpl) DeepEqual(to *RowDataTypeImpl) bool {
	if to == nil {
		return false
	}

	for i := range s.fields {
		if s.fields[i].String() != to.fields[i].String() {
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
			Expr:  field.Expr,
			Alias: field.Alias,
		}
	}

	dst.indexByName = nil
}

func (s *RowDataTypeImpl) Marshal() *internal.RowDataType {
	mc := MapConvert{}

	ret := &internal.RowDataType{
		Aux:         make([]int64, len(s.aux)),
		Fields:      s.fields.String(),
		IndexByName: mc.IntToInt64(s.indexByName),
	}

	for _, v := range s.aux {
		ret.Aux = append(ret.Aux, int64(v))
	}

	return ret
}

func (s *RowDataTypeImpl) Unmarshal(rt *internal.RowDataType) error {
	mc := MapConvert{}

	var err error
	s.fields, err = ParseFields(rt.Fields)
	if err != nil {
		return err
	}

	for _, v := range rt.Aux {
		s.aux = append(s.aux, int(v))
	}

	s.indexByName = mc.Int64ToInt(rt.IndexByName)

	return nil
}

type Catalog interface {
	GetColumnNames() []string
	GetQueryFields() influxql.Fields
	GetOptions() Options

	CloneField(f *influxql.Field) *influxql.Field
	HasCall() bool
	HasMath() bool
	HasString() bool
	HasNonPreCall() bool
	CountField() map[int]bool
	HasMeanCall() bool
	HasBlankRowCall() bool
	HasInterval() bool
	HasFieldCondition() bool
	Options() Options
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
	Binarys() map[string]*influxql.BinaryExpr
	Fields() influxql.Fields
	FieldsMap() map[string]*influxql.Field
	FieldsRef() influxql.VarRefs
	CountDistinct() *influxql.Call
	OnlyOneCallRef() *influxql.VarRef
	LimitType() LimitType
	HasLimit() bool
	LimitAndOffset() (int, int)
	MatchPreAgg() bool
	HasInSeriesAgg() bool
	CanCallsPushdown() bool
	CanAggPushDown() bool
	ContainSeriesIgnoreCall() bool
	IsRefInQueryFields(ref *influxql.VarRef) bool
	IsRefInSymbolFields(ref *influxql.VarRef) bool
	IsTimeZero() bool
	HasStreamCall() bool
	HasSlidingWindowCall() bool
	IsMultiMeasurements() bool
	HasGroupBy() bool
	Sources() influxql.Sources
	HasSubQuery() bool
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
	CreateLogicPlanV2(ctx context.Context, db string, ptId uint32, shardID uint64, sources influxql.Sources, schema Catalog) (QueryNode, error)
	UnrefEngineDbPt(db string, ptId uint32)
}
