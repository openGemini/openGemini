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
	"errors"
	"fmt"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

var (
	_ = RegistryAggregateFunction("mean", &MeanFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown: true,
			optimizeAgg: true,
		},
	})
	_ = RegistryAggregateFunction("count", &CountFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
			optimizeAgg:       true,
		},
	})
	_ = RegistryAggregateFunction("last", &LastFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
			optimizeAgg:       true,
		},
	})
	_ = RegistryAggregateFunction("first", &FirstFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
			optimizeAgg:       true,
		},
	})
	_ = RegistryAggregateFunction("min", &MinFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
			optimizeAgg:       true,
		},
	})
	_ = RegistryAggregateFunction("max", &MaxFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
			optimizeAgg:       true,
		},
	})
	_ = RegistryAggregateFunction("sum", &SumFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
			optimizeAgg:       true,
		},
	})
	_ = RegistryAggregateFunction("percentile", &PercentileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("percentile_ogsketch", &PercentileOGSketchFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			canPushDown: true,
		},
	})
	_ = RegistryAggregateFunction("percentile_approx", &PercentileApproxFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("histogram", &HistogramFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg:  BaseAgg{},
	})
	_ = RegistryAggregateFunction("sample", &SampleFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("distinct", &DistinctFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
		},
	})
	_ = RegistryAggregateFunction("top", &TopFunc{
		BaseInfo: BaseInfo{FuncType: AGG_HEAP},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
		},
	})
	_ = RegistryAggregateFunction("bottom", &BottomFunc{
		BaseInfo: BaseInfo{FuncType: AGG_HEAP},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
		},
	})
	_ = RegistryAggregateFunction("derivative", &DerivativeFunc{
		BaseInfo: BaseInfo{FuncType: AGG_TRANS},
		BaseAgg: BaseAgg{
			sortedMergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("non_negative_derivative", &NonNegativeDerivativeFunc{
		BaseInfo: BaseInfo{FuncType: AGG_TRANS},
		BaseAgg: BaseAgg{
			sortedMergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("difference", &DifferenceFunc{
		BaseInfo: BaseInfo{FuncType: AGG_TRANS},
		BaseAgg: BaseAgg{
			sortedMergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("non_negative_difference", &NonNegativeDifferenceFunc{
		BaseInfo: BaseInfo{FuncType: AGG_TRANS},
		BaseAgg: BaseAgg{
			sortedMergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("cumulative_sum", &CumulativeSumFunc{
		BaseInfo: BaseInfo{FuncType: AGG_TRANS},
		BaseAgg: BaseAgg{
			sortedMergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("moving_average", &MovingAverageFunc{
		BaseInfo: BaseInfo{FuncType: AGG_TRANS},
		BaseAgg: BaseAgg{
			sortedMergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("elapsed", &ElapsedFunc{
		BaseInfo: BaseInfo{FuncType: AGG_TRANS},
		BaseAgg: BaseAgg{
			sortedMergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("integral", &IntegralFunc{
		BaseInfo: BaseInfo{FuncType: AGG_TRANS},
		BaseAgg: BaseAgg{
			sortedMergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("mode", &ModeFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("stddev", &StddevFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("spread", &SpreadFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			canPushDown: true,
		},
	})
	_ = RegistryAggregateFunction("rate", &RateFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("irate", &IRateFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("absent", &AbsentFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("median", &MedianFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("sliding_window", &SlidingWindowFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			canPushDown: true,
		},
	})
	_ = RegistryAggregateFunction("sum_over_time", &SumFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("avg_over_time", &MeanFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("max_over_time", &MaxFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("min_over_time", &MinFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("count_over_time", &CountPromFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("stddev_over_time", &StddevFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("present_over_time_prom", &PresentFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("quantile_over_time", &PercentileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("rate_prom", &RateFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("irate_prom", &IRateFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("increase", &IncreaseFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("deriv", &DerivFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("predict_linear", &PredictLinearFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("min_prom", &MinPromFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
			optimizeAgg:       true,
		},
	})
	_ = RegistryAggregateFunction("max_prom", &MaxPromFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
			optimizeAgg:       true,
		},
	})
	_ = RegistryAggregateFunction("count_prom", &FloatCountPromFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown:       true,
			canPushDownSeries: true,
			optimizeAgg:       true,
		},
	})
	_ = RegistryAggregateFunction("histogram_quantile", &HistogramQuantileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("count_values_prom", &CountValuesFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			// TODO support push down
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("delta_prom", &PromDeltaFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("idelta_prom", &PromIDeltaFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SPECIAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("stdvar_prom", &OneParamCompileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("stddev_prom", &OneParamCompileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("group_prom", &PromGroupFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("scalar_prom", &PromScalarFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("quantile_prom", &PercentileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("stdvar_over_time_prom", &OneParamCompileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("stddev_over_time_prom", &OneParamCompileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("holt_winters_prom", &HoltWintersFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("changes_prom", &PromIDeltaFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("quantile_over_time_prom", &PercentileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("last_over_time_prom", &OneParamCompileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("resets_prom", &OneParamCompileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("absent_over_time_prom", &OneParamCompileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("absent_prom", &OneParamCompileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("mad_over_time_prom", &OneParamCompileFunc{
		BaseInfo: BaseInfo{FuncType: AGG_SLICE},
		BaseAgg: BaseAgg{
			mergeCall: true,
		},
	})
	_ = RegistryAggregateFunction("topn_ddcm", &TopNDDCMFunc{
		BaseInfo: BaseInfo{FuncType: AGG_NORMAL},
		BaseAgg: BaseAgg{
			canPushDown: false,
		},
	})
)

func GetAggregateOperator(name string) AggregateFunc {
	agg, ok := GetFunctionFactoryInstance().FindAggFunc(name)
	if ok {
		return agg
	}
	return nil
}

type MeanFunc struct {
	BaseInfo
	BaseAgg
}

func (f *MeanFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *MeanFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	c.global.OnlySelectors = false
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *MeanFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type CountFunc struct {
	BaseInfo
	BaseAgg
}

func (f *CountFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	c.global.OnlySelectors = false
	if exp, got := 1, len(expr.Args); exp != got {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
	}
	// If we have count(), the argument may be a distinct() call.
	if arg0, ok := expr.Args[0].(*influxql.Call); ok && arg0.Name == "distinct" {
		return c.compileDistinct(arg0.Args, true)
	} else if arg0, ok := expr.Args[0].(*influxql.Distinct); ok {
		call := arg0.NewCall()
		return c.compileDistinct(call.Args, true)
	}
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *CountFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Integer, nil
}

type CountPromFunc struct {
	BaseInfo
	BaseAgg
}

func (f *CountPromFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *CountPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	c.global.OnlySelectors = false
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *CountPromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type LastFunc struct {
	BaseInfo
	BaseAgg
}

func (f *LastFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *LastFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *LastFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type FirstFunc struct {
	BaseInfo
	BaseAgg
}

func (f *FirstFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *FirstFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *FirstFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type MinFunc struct {
	BaseInfo
	BaseAgg
}

func (f *MinFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *MinFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *MinFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type MaxFunc struct {
	BaseInfo
	BaseAgg
}

func (f *MaxFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *MaxFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *MaxFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type SumFunc struct {
	BaseInfo
	BaseAgg
}

func (f *SumFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *SumFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	c.global.OnlySelectors = false
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *SumFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type PercentileFunc struct {
	BaseInfo
	BaseAgg
}

func (f *PercentileFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 2, Max: 2},
		&TypeCheckRule{Name: name, Index: 1, Asserts: []func(interface{}) bool{AssertIntegerLiteral, AssertNumberLiteral}},
	}
}

func (f *PercentileFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *PercentileFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type PercentileOGSketchFunc struct {
	BaseInfo
	BaseAgg
}

func (f *PercentileOGSketchFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 2, Max: 3},
		&TypeCheckRule{Name: name, Index: 1, Asserts: []func(interface{}) bool{AssertIntegerLiteral, AssertNumberLiteral}},
		&TypeCheckRule{Name: name, Index: 2, Asserts: []func(interface{}) bool{AssertIntegerLiteral}},
	}
}

func (f *PercentileOGSketchFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	name := expr.Name
	c.global.OnlySelectors = false
	c.global.PercentileOGSketchFunction = name
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *PercentileOGSketchFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type PercentileApproxFunc struct {
	BaseInfo
	BaseAgg
}

func (f *PercentileApproxFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 2, Max: 3},
		&TypeCheckRule{Name: name, Index: 1, Asserts: []func(interface{}) bool{AssertIntegerLiteral, AssertNumberLiteral}},
		&TypeCheckRule{Name: name, Index: 2, Asserts: []func(interface{}) bool{AssertIntegerLiteral}},
	}
}

func (f *PercentileApproxFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	c.global.OnlySelectors = false
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *PercentileApproxFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type HistogramFunc struct {
	BaseInfo
	BaseAgg
}

func (f *HistogramFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 2, Max: 2},
		&TypeCheckRule{Name: name, Index: 1, Asserts: []func(interface{}) bool{AssertIntegerLiteral, AssertNumberLiteral, AssertStringLiteral}},
	}
}

func (f *HistogramFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *HistogramFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type SampleFunc struct {
	BaseInfo
	BaseAgg
}

func (f *SampleFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args := expr.Args
	if exp, got := 2, len(args); got != exp {
		return fmt.Errorf("invalid number of arguments for sample, expected %d, got %d", exp, got)
	}

	switch arg1 := args[1].(type) {
	case *influxql.IntegerLiteral:
		if arg1.Val <= 0 {
			return fmt.Errorf("sample window must be greater than 1, got %d", arg1.Val)
		}
	default:
		return fmt.Errorf("expected integer argument in sample()")
	}
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *SampleFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type DistinctFunc struct {
	BaseInfo
	BaseAgg
}

func (f *DistinctFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileDistinct(expr.Args, false)
}

func (f *DistinctFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type TopFunc struct {
	BaseInfo
	BaseAgg
}

func (f *TopFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileTopBottom(expr, c)
}

func (f *TopFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

func compileTopBottom(call *influxql.Call, c *compiledField) error {
	if c.global.TopBottomFunction != "" {
		return fmt.Errorf("selector function %s() cannot be combined with other functions", c.global.TopBottomFunction)
	}

	if exp, got := 2, len(call.Args); got < exp {
		return fmt.Errorf("invalid number of arguments for %s, expected at least %d, got %d", call.Name, exp, got)
	}

	limit, ok := call.Args[len(call.Args)-1].(*influxql.IntegerLiteral)
	if !ok {
		return fmt.Errorf("expected integer as last argument in %s(), found %s", call.Name, call.Args[len(call.Args)-1])
	} else if limit.Val <= 0 {
		return fmt.Errorf("limit (%d) in %s function must be at least 1", limit.Val, call.Name)
	} else if c.global.Limit > 0 && int(limit.Val) > c.global.Limit {
		return fmt.Errorf("limit (%d) in %s function can not be larger than the LIMIT (%d) in the select statement", limit.Val, call.Name, c.global.Limit)
	}

	if _, ok := call.Args[0].(*influxql.VarRef); !ok {
		return fmt.Errorf("expected first argument to be a field in %s(), found %s", call.Name, call.Args[0])
	}

	if len(call.Args) > 2 {
		for _, v := range call.Args[1 : len(call.Args)-1] {
			ref, ok := v.(*influxql.VarRef)
			if !ok {
				return fmt.Errorf("only fields or tags are allowed in %s(), found %s", call.Name, v)
			}

			// Add a field for each of the listed dimensions when not writing the results.
			if !c.global.HasTarget {
				field := &compiledField{
					global: c.global,
					Field:  &influxql.Field{Expr: ref},
				}
				c.global.Fields = append(c.global.Fields, field)
				if err := field.compileExpr(ref); err != nil {
					return err
				}
			}
		}
	}
	c.global.TopBottomFunction = call.Name
	return nil
}

type BottomFunc struct {
	BaseInfo
	BaseAgg
}

func (f *BottomFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileTopBottom(expr, c)
}

func (f *BottomFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type DerivativeFunc struct {
	BaseInfo
	BaseAgg
}

func (f *DerivativeFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileDerivative(expr, c)
}

func (f *DerivativeFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type NonNegativeDerivativeFunc struct {
	BaseInfo
	BaseAgg
}

func (f *NonNegativeDerivativeFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileDerivative(expr, c)
}

func (f *NonNegativeDerivativeFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

func compileDerivative(expr *influxql.Call, c *compiledField) error {
	name, args := expr.Name, expr.Args

	if min, max, got := 1, 2, len(args); got > max || got < min {
		return fmt.Errorf("invalid number of arguments for %s, expected at least %d but no more than %d, got %d", name, min, max, got)
	}

	// Retrieve the duration from the derivative() call, if specified.
	if len(args) == 2 {
		switch arg1 := args[1].(type) {
		case *influxql.DurationLiteral:
			if arg1.Val <= 0 {
				return fmt.Errorf("duration argument must be positive, got %s", influxql.FormatDuration(arg1.Val))
			}
		default:
			return fmt.Errorf("second argument to %s must be a duration, got %T", name, args[1])
		}
	}
	c.global.OnlySelectors = false
	if c.global.ExtraIntervals < 1 {
		c.global.ExtraIntervals = 1
	}

	// Must be a variable reference, function, wildcard, or regexp.
	switch arg0 := args[0].(type) {
	case *influxql.Call:
		if c.global.Interval.IsZero() {
			return fmt.Errorf("%s aggregate requires a GROUP BY interval", name)
		}
		return c.compileNestedExpr(arg0)
	default:
		if !c.global.Interval.IsZero() && !c.global.InheritedInterval {
			return fmt.Errorf("aggregate function required inside the call to %s", name)
		}
		return c.compileSymbol(name, arg0)
	}
}

type DifferenceFunc struct {
	BaseInfo
	BaseAgg
}

func (f *DifferenceFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileDifference(expr, c)
}

func (f *DifferenceFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type NonNegativeDifferenceFunc struct {
	BaseInfo
	BaseAgg
}

func (f *NonNegativeDifferenceFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileDifference(expr, c)
}

func (f *NonNegativeDifferenceFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

func compileDifference(expr *influxql.Call, c *compiledField) error {
	name, args := expr.Name, expr.Args
	if min, max, got := 1, 2, len(args); got > max || got < min {
		return fmt.Errorf("invalid number of arguments for %s, expected at least %d but no more than %d, got %d", name, min, max, got)
	}
	// Retrieve the duration from the difference() call, if specified.
	if len(args) == 2 {
		switch arg1 := args[1].(type) {
		case *influxql.StringLiteral:
			if !(arg1.Val == "front" || arg1.Val == "behind" || arg1.Val == "absolute") {
				return fmt.Errorf("the second argument must be front, behind or absolute, got %s", arg1.Val)
			}
		default:
			return fmt.Errorf("second argument to %s must be a string, got %T", name, args[1])
		}
	}
	c.global.OnlySelectors = false
	if c.global.ExtraIntervals < 1 {
		c.global.ExtraIntervals = 1
	}

	// Must be a variable reference, function, wildcard, or regexp.
	switch arg0 := args[0].(type) {
	case *influxql.Call:
		if c.global.Interval.IsZero() {
			return fmt.Errorf("%s aggregate requires a GROUP BY interval", name)
		}
		return c.compileNestedExpr(arg0)
	default:
		if !c.global.Interval.IsZero() && !c.global.InheritedInterval {
			return fmt.Errorf("aggregate function required inside the call to %s", name)
		}
		return c.compileSymbol(name, arg0)
	}
}

type CumulativeSumFunc struct {
	BaseInfo
	BaseAgg
}

func (f *CumulativeSumFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	if got := len(args); got != 1 {
		return fmt.Errorf("invalid number of arguments for cumulative_sum, expected 1, got %d", got)
	}
	c.global.OnlySelectors = false
	if c.global.ExtraIntervals < 1 {
		c.global.ExtraIntervals = 1
	}

	// Must be a variable reference, function, wildcard, or regexp.
	switch arg0 := args[0].(type) {
	case *influxql.Call:
		if c.global.Interval.IsZero() {
			return fmt.Errorf("cumulative_sum aggregate requires a GROUP BY interval")
		}
		return c.compileNestedExpr(arg0)
	default:
		if !c.global.Interval.IsZero() && !c.global.InheritedInterval {
			return fmt.Errorf("aggregate function required inside the call to cumulative_sum")
		}
		return c.compileSymbol(name, arg0)
	}
}

func (f *CumulativeSumFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type MovingAverageFunc struct {
	BaseInfo
	BaseAgg
}

func (f *MovingAverageFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	if got := len(args); got != 2 {
		return fmt.Errorf("invalid number of arguments for moving_average, expected 2, got %d", got)
	}

	arg1, ok := args[1].(*influxql.IntegerLiteral)
	if !ok {
		return fmt.Errorf("second argument for moving_average must be an integer, got %T", args[1])
	} else if arg1.Val <= 1 {
		return fmt.Errorf("moving_average window must be greater than 1, got %d", arg1.Val)
	}
	c.global.OnlySelectors = false
	if c.global.ExtraIntervals < int(arg1.Val) {
		c.global.ExtraIntervals = int(arg1.Val)
	}

	// Must be a variable reference, function, wildcard, or regexp.
	switch arg0 := args[0].(type) {
	case *influxql.Call:
		if c.global.Interval.IsZero() {
			return fmt.Errorf("moving_average aggregate requires a GROUP BY interval")
		}
		return c.compileNestedExpr(arg0)
	default:
		if !c.global.Interval.IsZero() && !c.global.InheritedInterval {
			return fmt.Errorf("aggregate function required inside the call to moving_average")
		}
		return c.compileSymbol(name, arg0)
	}
}

func (f *MovingAverageFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type ElapsedFunc struct {
	BaseInfo
	BaseAgg
}

func (f *ElapsedFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	if min, max, got := 1, 2, len(args); got > max || got < min {
		return fmt.Errorf("invalid number of arguments for elapsed, expected at least %d but no more than %d, got %d", min, max, got)
	}

	// Retrieve the duration from the elapsed() call, if specified.
	if len(args) == 2 {
		switch arg1 := args[1].(type) {
		case *influxql.DurationLiteral:
			if arg1.Val <= 0 {
				return fmt.Errorf("duration argument must be positive, got %s", influxql.FormatDuration(arg1.Val))
			}
		default:
			return fmt.Errorf("second argument to elapsed must be a duration, got %T", args[1])
		}
	}
	c.global.OnlySelectors = false
	if c.global.ExtraIntervals < 1 {
		c.global.ExtraIntervals = 1
	}

	// Must be a variable reference, function, wildcard, or regexp.
	switch arg0 := args[0].(type) {
	case *influxql.Call:
		if c.global.Interval.IsZero() {
			return fmt.Errorf("elapsed aggregate requires a GROUP BY interval")
		}
		return c.compileNestedExpr(arg0)
	default:
		if !c.global.Interval.IsZero() && !c.global.InheritedInterval {
			return fmt.Errorf("aggregate function required inside the call to elapsed")
		}
		return c.compileSymbol(name, arg0)
	}
}

func (f *ElapsedFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Integer, nil
}

type IntegralFunc struct {
	BaseInfo
	BaseAgg
}

func (f *IntegralFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	if min, max, got := 1, 2, len(args); got > max || got < min {
		return fmt.Errorf("invalid number of arguments for integral, expected at least %d but no more than %d, got %d", min, max, got)
	}

	if len(args) == 2 {
		switch arg1 := args[1].(type) {
		case *influxql.DurationLiteral:
			if arg1.Val <= 0 {
				return fmt.Errorf("duration argument must be positive, got %s", influxql.FormatDuration(arg1.Val))
			}
		default:
			return errors.New("second argument must be a duration")
		}
	}
	c.global.OnlySelectors = false

	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *IntegralFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type ModeFunc struct {
	BaseInfo
	BaseAgg
}

func (f *ModeFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *ModeFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *ModeFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type HoltWintersFunc struct {
	BaseInfo
	BaseAgg
}

func (f *HoltWintersFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	c.global.OnlySelectors = false
	if exp, got := 3, len(expr.Args); exp != got {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
	}
	args := expr.Args

	var sf float64
	switch arg := args[1].(type) {
	case *influxql.NumberLiteral:
		sf = arg.Val
	case *influxql.IntegerLiteral:
		sf = float64(arg.Val)
	default:
		return fmt.Errorf("expected float argument in holt_winters()")
	}
	if sf <= 0 || sf >= 1 {
		return fmt.Errorf("invalid smoothing factor. Expected: 0 < sf < 1, got: %f", sf)
	}
	var tf float64
	switch arg := args[2].(type) {
	case *influxql.NumberLiteral:
		tf = arg.Val
	case *influxql.IntegerLiteral:
		tf = float64(arg.Val)
	default:
		return fmt.Errorf("expected float argument in holt_winters()")
	}
	if tf <= 0 || tf >= 1 {
		return fmt.Errorf("invalid trend factor. Expected: 0 < tf < 1, got: %f", tf)
	}
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *HoltWintersFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type OneParamCompileFunc struct {
	BaseInfo
	BaseAgg
}

func (f *OneParamCompileFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *OneParamCompileFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *OneParamCompileFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type StddevFunc struct {
	BaseInfo
	BaseAgg
}

func (f *StddevFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *StddevFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *StddevFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type StdvarFunc struct {
	BaseInfo
	BaseAgg
}

func (f *StdvarFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *StdvarFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *StdvarFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type PresentFunc struct {
	BaseInfo
	BaseAgg
}

func (f *PresentFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *PresentFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *PresentFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type SpreadFunc struct {
	BaseInfo
	BaseAgg
}

func (f *SpreadFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *SpreadFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *SpreadFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type RateFunc struct {
	BaseInfo
	BaseAgg
}

func (f *RateFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *RateFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *RateFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type IRateFunc struct {
	BaseInfo
	BaseAgg
}

func (f *IRateFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *IRateFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *IRateFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type AbsentFunc struct {
	BaseInfo
	BaseAgg
}

func (f *AbsentFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *AbsentFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *AbsentFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Integer, nil
}

type MedianFunc struct {
	BaseInfo
	BaseAgg
}

func (f *MedianFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *MedianFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *MedianFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type SlidingWindowFunc struct {
	BaseInfo
	BaseAgg
}

func (f *SlidingWindowFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args := expr.Args
	if got := len(args); got != 2 {
		return fmt.Errorf("invalid number of arguments for sliding_window, expected 2, got %d", got)
	}

	arg1, ok := args[1].(*influxql.IntegerLiteral)
	if !ok {
		return fmt.Errorf("second argument for sliding_window must be an integer, got %T", args[1])
	} else if arg1.Val <= 1 {
		return fmt.Errorf("sliding_window window must be greater than 1, got %d", arg1.Val)
	}
	c.global.OnlySelectors = false
	if c.global.ExtraIntervals < int(arg1.Val) {
		c.global.ExtraIntervals = int(arg1.Val)
	}

	// Must be a variable reference, function, wildcard, or regexp.
	switch arg0 := args[0].(type) {
	case *influxql.Call:
		if c.global.Interval.IsZero() {
			return fmt.Errorf("sliding_window aggregate requires a GROUP BY interval")
		}
		return c.compileNestedExpr(arg0)
	default:
		return fmt.Errorf("aggregate function required inside the call to sliding_window")
	}
}

func (f *SlidingWindowFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type DerivFunc struct {
	BaseInfo
	BaseAgg
}

func (f *DerivFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *DerivFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *DerivFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type IncreaseFunc struct {
	BaseInfo
	BaseAgg
}

func (f *IncreaseFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *IncreaseFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *IncreaseFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type PredictLinearFunc struct {
	BaseInfo
	BaseAgg
}

func (f *PredictLinearFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 2, Max: 2},
		&TypeCheckRule{Name: name, Index: 1, Asserts: []func(interface{}) bool{AssertIntegerLiteral, AssertNumberLiteral}},
	}
}

func (f *PredictLinearFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *PredictLinearFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type MinPromFunc struct {
	BaseInfo
	BaseAgg
}

func (f *MinPromFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *MinPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *MinPromFunc) CallTypeFunc(_ string, _ []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type MaxPromFunc struct {
	BaseInfo
	BaseAgg
}

func (f *MaxPromFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *MaxPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *MaxPromFunc) CallTypeFunc(_ string, _ []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type FloatCountPromFunc struct {
	BaseInfo
	BaseAgg
}

func (f *FloatCountPromFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *FloatCountPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *FloatCountPromFunc) CallTypeFunc(_ string, _ []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type HistogramQuantileFunc struct {
	BaseInfo
	BaseAgg
}

func (f *HistogramQuantileFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 2, Max: 2},
		&TypeCheckRule{Name: name, Index: 1, Asserts: []func(interface{}) bool{AssertIntegerLiteral, AssertNumberLiteral}},
	}
}

func (f *HistogramQuantileFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *HistogramQuantileFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return args[0], nil
}

type CountValuesFunc struct {
	BaseInfo
	BaseAgg
}

func (f *CountValuesFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 2, Max: 2},
		&TypeCheckRule{Name: name, Index: 1, Asserts: []func(interface{}) bool{AssertStringLiteral}},
	}
}

func (f *CountValuesFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *CountValuesFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type PromDeltaFunc struct {
	BaseInfo
	BaseAgg
}

func (f *PromDeltaFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *PromDeltaFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *PromDeltaFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type PromIDeltaFunc struct {
	BaseInfo
	BaseAgg
}

func (f *PromIDeltaFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *PromIDeltaFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *PromIDeltaFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type PromGroupFunc struct {
	BaseInfo
	BaseAgg
}

func (f *PromGroupFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *PromGroupFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *PromGroupFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type PromScalarFunc struct {
	BaseInfo
	BaseAgg
}

func (f *PromScalarFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 1, Max: 1},
	}
}

func (f *PromScalarFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	c.global.OnlySelectors = false
	// Must be a variable reference, wildcard, or regexp.
	return c.compileSymbol(name, args[0])
}

func (f *PromScalarFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type TopNDDCMFunc struct {
	BaseInfo
	BaseAgg
}

// topn_ddcm(field, countLowerBound, topNumber), countLowerBound use defaultNumber by set to zero
func (f *TopNDDCMFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args := expr.Args
	name := "topN_ddcm"
	if exp, got := 4, len(args); got != exp {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", name, exp, got)
	}

	_, ok := args[1].(*influxql.NumberLiteral)
	if !ok {
		return fmt.Errorf("expected integer argument as second arg in %s", name)
	}

	s, ok := args[2].(*influxql.IntegerLiteral)
	if !ok {
		return fmt.Errorf("expected integer argument as third arg in %s", name)
	} else if s.Val <= 0 {
		return fmt.Errorf("third arg to %s cannot be negative or zero, got %d", name, s.Val)
	}
	c.global.OnlySelectors = false

	fn, ok := args[3].(*influxql.StringLiteral)
	if !ok {
		return fmt.Errorf("expected string argument as fourth arg in %s", name)
	} else if fn.Val == "count" {
		expr.Args = append(expr.Args, &influxql.IntegerLiteral{Val: 0})
	} else if fn.Val == "sum" {
		expr.Args = append(expr.Args, &influxql.NumberLiteral{Val: 0})
	} else {
		return fmt.Errorf("fourth arg to %s can only be count or sum, got %d", name, s.Val)
	}

	_, ok = args[0].(*influxql.VarRef)
	if !ok {
		return fmt.Errorf("must use field name with %s", name)
	} else if len(c.global.stmt.Dimensions) == 0 {
		return fmt.Errorf("%s aggregate requires a GROUP BY dims", name)
	}
	c.global.TopBottomFunction = name
	return nil
}

func (f *TopNDDCMFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[len(args)-1] == influxql.Integer {
		return influxql.Integer, nil
	} else if args[len(args)-1] == influxql.Float && (args[0] == influxql.Integer || args[0] == influxql.Float) {
		return args[0], nil
	} else {
		return influxql.Integer, fmt.Errorf("topn_ddcm call type func err")
	}
}
