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
	"fmt"
	"strconv"

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
	COMPARE     // Year-over-year and month-over-month compare functions
)

type BaseFunc interface {
	GetRules(name string) []CheckRule
	CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error)
	GetFuncType() FuncType
	CompileFunc(expr *influxql.Call, c *compiledField) error
}

type BaseInfo struct {
	FuncType FuncType
}

func (b *BaseInfo) GetFuncType() FuncType {
	return b.FuncType
}

func (b *BaseInfo) GetRules(name string) []CheckRule {
	return []CheckRule{}
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
}

type PromTimeFunc interface {
	BaseFunc
}

type MaterializeFunc interface {
	BaseFunc
	CallFunc(name string, args []interface{}) (interface{}, bool)
}

type AggregateFunc interface {
	BaseFunc
	CanPushDown() bool
	SortedMergeCall() bool
	MergeCall() bool
	CanPushDownSeries() bool
	OptimizeAgg() bool
}

type CompareFunc interface {
	BaseFunc
	CompileFunc(expr *influxql.Call, c *compiledField) error
}

type FunctionFactory struct {
	materialize map[string]MaterializeFunc
	aggregate   map[string]AggregateFunc
	label       map[string]LabelFunc
	promTime    map[string]PromTimeFunc // use for prom
	compare     map[string]CompareFunc
}

func NewFunctionFactory() *FunctionFactory {
	return &FunctionFactory{
		materialize: make(map[string]MaterializeFunc),
		aggregate:   make(map[string]AggregateFunc),
		label:       make(map[string]LabelFunc),
		promTime:    make(map[string]PromTimeFunc),
		compare:     make(map[string]CompareFunc),
	}
}

func RegistryCompareFunction(name string, function CompareFunc) {
	factory := GetFunctionFactoryInstance()
	_, ok := factory.compare[name]
	if ok {
		return
	}
	factory.compare[name] = function
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

func RegisterAggregateFunction(name string, function AggregateFunc) bool {
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

func (r *FunctionFactory) FindCompareFunc(name string) (CompareFunc, bool) {
	function, ok := r.compare[name]
	return function, ok
}

func (r *FunctionFactory) GetAggregateOp() map[string]AggregateFunc {
	return r.aggregate
}

func (r *FunctionFactory) GetMaterializeOp() map[string]MaterializeFunc {
	return r.materialize
}

func (r *FunctionFactory) GetLabelOp() map[string]LabelFunc {
	return r.label
}

func (r *FunctionFactory) GetPromTimeOp() map[string]PromTimeFunc {
	return r.promTime
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

type CheckRule interface {
	Check(expr *influxql.Call) error
}

type ArgNumberCheckRule struct {
	Name string
	Max  int
	Min  int
}

func (a *ArgNumberCheckRule) Check(expr *influxql.Call) error {
	got := len(expr.Args)
	if got < a.Min || got > a.Max {
		if a.Min == a.Max {
			return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, a.Min, got)
		}
		return fmt.Errorf("invalid number of arguments for %s, expected %d-%d, got %d", expr.Name, a.Min, a.Max, got)
	}
	return nil
}

type TypeCheckRule struct {
	Name    string
	Index   int
	Asserts []func(interface{}) bool
}

func (t *TypeCheckRule) Check(expr *influxql.Call) error {
	callLen := len(expr.Args)
	if t.Index >= callLen {
		return nil
	}
	for _, assert := range t.Asserts {
		if assert(expr.Args[t.Index]) {
			return nil
		}
	}
	return fmt.Errorf("invalid argument type for the %s argument in %s(): %s", convToOrdinal(t.Index+1), expr.Name, expr.Args[t.Index])
}

func convToOrdinal(n int) string {
	suffix := "th"
	switch n % 10 {
	case 1:
		if n%100 != 11 {
			suffix = "st"
		}
	case 2:
		if n%100 != 12 {
			suffix = "nd"
		}
	case 3:
		if n%100 != 13 {
			suffix = "rd"
		}
	}
	return strconv.Itoa(n) + suffix
}

func AssertStringLiteral(arg interface{}) bool {
	_, ok := arg.(*influxql.StringLiteral)
	return ok
}

func AssertIntegerLiteral(arg interface{}) bool {
	_, ok := arg.(*influxql.IntegerLiteral)
	return ok
}

func AssertNumberLiteral(arg interface{}) bool {
	_, ok := arg.(*influxql.NumberLiteral)
	return ok
}
