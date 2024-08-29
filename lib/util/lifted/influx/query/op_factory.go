// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package query

import (
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type FuncType int

var instance *FunctionFactory = NewFunctionFactory()

// functions/operator type
// The agg operator is classified based on the implementation process.
const (
	STRING FuncType = iota
	MATH
	LABEL
	AGG_NORMAL  // Traverse the data in the current time window to complete the calculation.The traversal process does not depend on other data in the time window.eg: max,min
	AGG_SLICE   // All data in the time window needs to be cached during calculation. eg: median,percentile
	AGG_HEAP    // Same as AGG_SLICE but requires heap sorting. eg: top,bottom
	AGG_TRANS   // Calculation depends on part data in the time window. eg: derivative
	AGG_SPECIAL // Special categories,Need to implement custom iterator.
	PROMTIME    // use for promTime funcs
)

type BaseFunc interface {
	CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error)
	GetFuncType() FuncType
}

type BaseInfo struct {
	FuncType FuncType
}

func (b *BaseInfo) GetFuncType() FuncType {
	return b.FuncType
}

type BaseAgg struct {
	canPushDown       bool // Operators can be pushed down to the bottom layer for calculation.
	sortedMergeCall   bool // Sorting data within a group.(can't push down)
	mergeCall         bool // Data is not sorted within a group.(can't push down)
	canPushDownSeries bool // canPushDownSeries is a subset of canPushDown.Operators can be pushed down in series_agg.
	optimizeAgg       bool // optimizeAgg is a subset of canPushDown.But aggregation result of optimizeAgg is a single value.
}

func (b *BaseAgg) CanPushDown() bool {
	return b.canPushDown
}

func (b *BaseAgg) SortedMergeCall() bool {
	return b.sortedMergeCall
}

func (b *BaseAgg) MergeCall() bool {
	return b.mergeCall
}

func (b *BaseAgg) CanPushDownSeries() bool {
	return b.canPushDownSeries
}

func (b *BaseAgg) OptimizeAgg() bool {
	return b.optimizeAgg
}

type LabelFunc interface {
	BaseFunc
	CompileFunc(expr *influxql.Call, c *compiledField) error
}

type PromTimeFunc interface {
	BaseFunc
	CompileFunc(expr *influxql.Call, c *compiledField) error
}

type MaterializeFunc interface {
	BaseFunc
	CompileFunc(expr *influxql.Call, c *compiledField) error
	CallFunc(name string, args []interface{}) (interface{}, bool)
}

type AggregateFunc interface {
	BaseFunc
	CompileFunc(expr *influxql.Call, c *compiledField) error
	CanPushDown() bool
	SortedMergeCall() bool
	MergeCall() bool
	CanPushDownSeries() bool
	OptimizeAgg() bool
}

type FunctionFactory struct {
	materialize map[string]MaterializeFunc
	aggregate   map[string]AggregateFunc
	label       map[string]LabelFunc
	promTime    map[string]PromTimeFunc // use for prom
}

func NewFunctionFactory() *FunctionFactory {
	return &FunctionFactory{
		materialize: make(map[string]MaterializeFunc),
		aggregate:   make(map[string]AggregateFunc),
		label:       make(map[string]LabelFunc),
		promTime:    make(map[string]PromTimeFunc),
	}
}

func RegistryPromTimeFunction(name string, function PromTimeFunc) {
	factory := GetFunctionFactoryInstance()
	_, ok := factory.promTime[name]
	if ok {
		return
	}

	factory.promTime[name] = function
}

func RegistryLabelFunction(name string, function LabelFunc) {
	factory := GetFunctionFactoryInstance()
	_, ok := factory.label[name]
	if ok {
		return
	}

	factory.label[name] = function
}

func RegistryMaterializeFunction(name string, function MaterializeFunc) bool {
	factory := GetFunctionFactoryInstance()
	_, ok := factory.FindMaterFunc(name)

	if ok {
		return ok
	}

	factory.AddMaterFunc(name, function)
	return ok
}

func (r *FunctionFactory) AddMaterFunc(name string, function MaterializeFunc) {
	r.materialize[name] = function
}

func (r *FunctionFactory) FindMaterFunc(name string) (MaterializeFunc, bool) {
	function, ok := r.materialize[name]
	return function, ok
}

func RegistryAggregateFunction(name string, function AggregateFunc) bool {
	factory := GetFunctionFactoryInstance()
	_, ok := factory.FindAggFunc(name)

	if ok {
		return ok
	}

	factory.AddAggFunc(name, function)
	return ok
}

func (r *FunctionFactory) AddAggFunc(name string, function AggregateFunc) {
	r.aggregate[name] = function
}

func (r *FunctionFactory) FindAggFunc(name string) (AggregateFunc, bool) {
	function, ok := r.aggregate[name]
	return function, ok
}

func (r *FunctionFactory) GetAggregateOp() map[string]AggregateFunc {
	return r.aggregate
}

func GetFunctionFactoryInstance() *FunctionFactory {
	return instance
}

func GetMaterializeFunction(name string, t FuncType) MaterializeFunc {
	materialize, ok := GetFunctionFactoryInstance().FindMaterFunc(name)
	if ok && materialize.GetFuncType() == t {
		return materialize
	}
	return nil
}
