/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"math"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func assertEqual(t *testing.T, expected, actual interface{}) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

func TestFloatRate(t *testing.T) {
	// Test the case where the number of points is less than 2
	result, ok := rate(true, true)([]int64{}, []int64{}, []float64{}, []float64{}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the case where lastTime is equal to prevTime
	result, ok = rate(true, true)([]int64{1000000000}, []int64{1000000000}, []float64{10}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the case where call.Range.Nanoseconds() is equal to 0
	result, ok = rate(true, true)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(0)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the normal case
	result, ok = rate(true, true)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(5), result)
	assertEqual(t, true, ok)

	// Test the case where lastValue is less than prevValue
	result, ok = rate(true, true)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{5}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(2.5), result)
	assertEqual(t, true, ok)

	// Test the case where lastValue is greater than prevValue
	result, ok = rate(true, true)([]int64{1000000000}, []int64{3000000000}, []float64{5}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(6.875), result)
	assertEqual(t, true, ok)
}

func TestFloatDelta(t *testing.T) {
	// Test the case where the number of points is less than 2
	result, ok := rate(false, false)([]int64{}, []int64{}, []float64{}, []float64{}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the case where lastTime is equal to prevTime
	result, ok = rate(false, false)([]int64{1000000000}, []int64{1000000000}, []float64{10}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the case where call.Range.Nanoseconds() is equal to 0
	result, ok = rate(false, false)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(0)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the normal case
	result, ok = rate(false, false)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(20), result)
	assertEqual(t, true, ok)

	// Test the case where lastValue is less than prevValue
	result, ok = rate(false, false)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{5}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(-10), result)
	assertEqual(t, true, ok)

	// Test the case where lastValue is greater than prevValue
	result, ok = rate(false, false)([]int64{1000000000}, []int64{3000000000}, []float64{5}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(27.5), result)
	assertEqual(t, true, ok)
}

func TestFloatIncrease(t *testing.T) {
	// Test the case where the number of points is less than 2
	result, ok := rate(false, true)([]int64{}, []int64{}, []float64{}, []float64{}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the case where lastTime is equal to prevTime
	result, ok = rate(false, true)([]int64{1000000000}, []int64{1000000000}, []float64{10}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the case where call.Range.Nanoseconds() is equal to 0
	result, ok = rate(false, true)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(0)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the normal case
	result, ok = rate(false, true)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(20), result)
	assertEqual(t, true, ok)

	// Test the case where lastValue is less than prevValue
	result, ok = rate(false, true)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{5}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(10), result)
	assertEqual(t, true, ok)

	// Test the case where lastValue is greater than prevValue
	result, ok = rate(false, true)([]int64{1000000000}, []int64{3000000000}, []float64{5}, []float64{20}, 4000000000, &influxql.PromSubCall{Range: time.Duration(4000000000)})
	assertEqual(t, float64(27.5), result)
	assertEqual(t, true, ok)
}

func TestFloatIrate_True(t *testing.T) {
	// Test the case where the number of points is less than 2
	result, ok := irate(true)([]int64{}, []int64{}, []float64{}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the case where lastTime is equal to prevTime
	result, ok = irate(true)([]int64{1000000000}, []int64{1000000000}, []float64{10}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the case where call.Range.Nanoseconds() is equal to 0
	result, ok = irate(true)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(0)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the normal case
	result, ok = irate(true)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)})
	assertEqual(t, float64(5), result)
	assertEqual(t, true, ok)

	// Test the case where lastValue is less than prevValue
	result, ok = irate(true)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{5}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)})
	assertEqual(t, float64(2.5), result)
	assertEqual(t, true, ok)

	// Test the case where lastValue is greater than prevValue
	result, ok = irate(true)([]int64{1000000000}, []int64{3000000000}, []float64{5}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)})
	assertEqual(t, float64(7.5), result)
	assertEqual(t, true, ok)
}

func TestFloatIrate_False(t *testing.T) {
	// Test the case where the number of points is less than 2
	result, ok := irate(false)([]int64{}, []int64{}, []float64{}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the case where lastTime is equal to prevTime
	result, ok = irate(false)([]int64{1000000000}, []int64{1000000000}, []float64{10}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the case where call.Range.Nanoseconds() is equal to 0
	result, ok = irate(false)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(0)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Test the normal case
	result, ok = irate(false)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)})
	assertEqual(t, float64(10), result)
	assertEqual(t, true, ok)

	// Test the case where lastValue is less than prevValue
	result, ok = irate(false)([]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{5}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)})
	assertEqual(t, float64(-5), result)
	assertEqual(t, true, ok)

	// Test the case where lastValue is greater than prevValue
	result, ok = irate(false)([]int64{1000000000}, []int64{3000000000}, []float64{5}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)})
	assertEqual(t, float64(15), result)
	assertEqual(t, true, ok)
}

func TestSubQueryStdVarOverTime(t *testing.T) {
	// Case: Current values only.
	result, ok := stdVarOverTime(false)([]int64{}, []int64{1e9, 2e9, 3e9}, []float64{}, []float64{7, 14, 25}, 3e9, &influxql.PromSubCall{Range: time.Duration(1e9)})
	assertEqual(t, 54.88888888888889, result)
	assertEqual(t, true, ok)

	// Case: Previous values only.
	result, ok = stdVarOverTime(false)([]int64{1e9, 2e9, 3e9}, []int64{}, []float64{3, 7, 14}, []float64{}, 3e9, &influxql.PromSubCall{Range: time.Duration(1e9)})
	assertEqual(t, 20.666666666666668, result)
	assertEqual(t, true, ok)

	// Case: Both previous and current values, different timestamps.
	result, ok = stdVarOverTime(false)([]int64{1e9, 3e9}, []int64{5e9, 7e9}, []float64{1, 3}, []float64{7, 14}, 7e9, &influxql.PromSubCall{Range: time.Duration(2e9)})
	assertEqual(t, 24.6875, result)
	assertEqual(t, true, ok)

	// Case: Count of preValues and curValues are all zeros
	result, ok = stdVarOverTime(false)([]int64{}, []int64{}, []float64{}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1e9)})
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	// Case: Large data difference
	result, ok = stdVarOverTime(false)([]int64{1e9, 3e9}, []int64{5e9, 7e9}, []float64{1e6, 2}, []float64{2e8, 3}, 7e9, &influxql.PromSubCall{Range: time.Duration(2e9)})
	assertEqual(t, float64(7475187374375002), result)
	assertEqual(t, true, ok)

	// Case: Parameter IsStdDev is true
	result, ok = stdVarOverTime(true)([]int64{2e9, 4e9}, []int64{6e9, 8e9}, []float64{1, 3}, []float64{7, 14}, 8e9, &influxql.PromSubCall{Range: time.Duration(2e9)})
	assertEqual(t, 4.968651728587948, result)
	assertEqual(t, true, ok)
}

func TestFloatMinOverTime(t *testing.T) {
	prevTimes := []int64{1, 2, 3, 4}
	currTimes := []int64{5, 6, 7, 8}
	prevValues := []float64{1.0, 2.0, 4.0, 8.0}
	currValues := []float64{9.0, 8.0, 7.0, 6.0}

	result, ok := minOverTime(prevTimes, currTimes, prevValues, currValues, 0, nil)
	assertEqual(t, float64(1.0), result)
	assertEqual(t, true, ok)
}

func TestFloatMaxOverTime(t *testing.T) {
	prevTimes := []int64{1, 2, 3, 4}
	currTimes := []int64{5, 6, 7, 8}
	prevValues := []float64{1.0, 2.0, 4.0, 8.0}
	currValues := []float64{9.0, 8.0, 7.0, 6.0}

	result, ok := maxOverTime(prevTimes, currTimes, prevValues, currValues, 0, nil)
	assertEqual(t, float64(9.0), result)
	assertEqual(t, true, ok)

}

func TestFloatSumOverTime(t *testing.T) {
	prevTimes := []int64{1, 2, 3, 4}
	currTimes := []int64{5, 6, 7, 8}
	prevValues := []float64{1.0, 2.0, 4.0, 8.0}
	currValues := []float64{9.0, 8.0, 7.0, 6.0}

	result, ok := sumOverTime(prevTimes, currTimes, prevValues, currValues, 0, nil)
	assertEqual(t, float64(45.0), result)
	assertEqual(t, true, ok)
}

func TestFloatCountOverTime(t *testing.T) {
	prevTimes := []int64{1, 2, 3, 4}
	currTimes := []int64{5, 6, 7, 8}
	prevValues := []float64{1.0, 2.0, 4.0, 8.0}
	currValues := []float64{9.0, 8.0, 7.0, 6.0}

	result, ok := countOverTime(prevTimes, currTimes, prevValues, currValues, 0, nil)
	assertEqual(t, float64(8), result)
	assertEqual(t, true, ok)
}

func TestFloatAvgOverTime(t *testing.T) {
	prevTimes := []int64{1, 2, 3, 4}
	currTimes := []int64{5, 6, 7, 8}
	prevValues := []float64{1.0, 2.0, 4.0, 8.0}
	currValues := []float64{9.0, 8.0, 7.0, 6.0}

	result, ok := avgOverTime(prevTimes, currTimes, prevValues, currValues, 0, nil)
	assertEqual(t, float64(5.625), result)
	assertEqual(t, true, ok)
}

func TestSubQueryPredictLinear(t *testing.T) {
	prevTimes := []int64{1e9, 2e9, 3e9, 4e9}
	currTimes := []int64{5e9, 6e9, 7e9, 8e9}
	prevValues := []float64{2, 4, 7, 13}
	currValues := []float64{17, 25, 25, 25}
	call := &influxql.PromSubCall{InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 10}}}

	result, ok := predictLinear(false)(prevTimes, currTimes, prevValues, currValues, 8e9, call)
	assertEqual(t, 66.82142857142857, result)
	assertEqual(t, true, ok)

	prevTimes = []int64{}
	currTimes = []int64{}
	prevValues = []float64{}
	currValues = []float64{}

	result, ok = predictLinear(false)(prevTimes, currTimes, prevValues, currValues, 8e9, call)
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)

	prevTimes = []int64{1, 2, 3}
	currTimes = []int64{}
	prevValues = []float64{37, 37, 37}
	currValues = []float64{}

	result, ok = predictLinear(false)(prevTimes, currTimes, prevValues, currValues, 3, call)
	assertEqual(t, float64(37), result)
	assertEqual(t, true, ok)
}

func TestSubQueryDeriv(t *testing.T) {
	prevTimes := []int64{1e9, 2e9, 3e9, 4e9}
	currTimes := []int64{5e9, 6e9, 7e9, 8e9}
	prevValues := []float64{2, 4, 7, 13}
	currValues := []float64{17, 25, 25, 25}

	result, ok := predictLinear(true)(prevTimes, currTimes, prevValues, currValues, 8e9, nil)
	assertEqual(t, 3.857142857142857, result)
	assertEqual(t, true, ok)

	prevTimes = []int64{1, 2, 3}
	currTimes = []int64{}
	prevValues = []float64{37, 37, 37}
	currValues = []float64{}

	result, ok = predictLinear(true)(prevTimes, currTimes, prevValues, currValues, 3, nil)
	assertEqual(t, float64(0), result)
	assertEqual(t, true, ok)
}

func TestCalcQuantile(t *testing.T) {
	testCases := []struct {
		values   []float64
		q        float64
		expected float64
	}{
		{[]float64{}, 0.5, math.NaN()},                 // "empty input"
		{[]float64{1, 2, 3, 4, 5}, -0.1, math.Inf(-1)}, // "q < 0"
		{[]float64{1, 2, 3, 4, 5}, 1.1, math.Inf(1)},   // "q > 1"
		{[]float64{1, 2, 3, 4, 5}, 0.5, 3.0},           // "normal case" - "q =0.5"
		{[]float64{1, 2, 3, 4, 5}, 0.2, 1.8},           // "normal case" - "q =0.2"
	}

	for _, tc := range testCases {
		result := CalcQuantile(tc.q, tc.values)
		equal := Float64Equal(result, tc.expected)
		if !equal {
			t.Errorf("CalcQuantile failed: expected %v but got %v", tc.expected, result)
		}
		assertEqual(t, true, equal)
	}
}

func TestCalcMad(t *testing.T) {
	testCases := []struct {
		values   []float64
		expected float64
	}{
		{[]float64{}, math.NaN()},
		{[]float64{1, 2, 3, 4, 5}, 1.0},
		{[]float64{4, 6, 2, 1, 999, 1, 2}, 1.0},
	}

	for _, tc := range testCases {
		result := CalcMad(tc.values)
		equal := Float64Equal(result, tc.expected)
		if !equal {
			t.Errorf("CalcMad failed: expected %v but got %v", tc.expected, result)
		}
		assertEqual(t, true, equal)
	}
}

func TestMadOverTime(t *testing.T) {
	testCases := []struct {
		preTimes   []int64
		currTimes  []int64
		preValues  []float64
		currValues []float64
		ts         int64
		call       *influxql.PromSubCall
		expected   float64
		ok         bool
	}{
		{[]int64{}, []int64{}, []float64{}, []float64{}, 7, &influxql.PromSubCall{}, math.NaN(), false},
		{[]int64{}, []int64{4, 5, 6}, []float64{}, []float64{4, 5, 6}, 7, &influxql.PromSubCall{}, 1.0, true},
		{[]int64{1, 2, 3}, []int64{4, 5, 6}, []float64{1, 2, 3}, []float64{4, 5, 6}, 7, &influxql.PromSubCall{}, 1.5, true},
	}

	for _, tc := range testCases {
		result, ok := madOverTime(tc.preTimes, tc.currTimes, tc.preValues, tc.currValues, tc.ts, tc.call)
		equal := Float64Equal(result, tc.expected)
		if !equal {
			t.Errorf("TestMadOverTime failed: expected %v but got %v", tc.expected, result)
		}
		assertEqual(t, true, equal)
		assertEqual(t, tc.ok, ok)
	}
}

func TestSubQueryAbsentPresentOverTime(t *testing.T) {
	prevTimes := []int64{1e9, 2e9, 3e9, 4e9}
	currTimes := []int64{5e9, 6e9, 7e9, 8e9}
	prevValues := []float64{1, 2, 3, 4}
	currValues := []float64{5, 6, 7, 8}

	result, ok := intervalExistMark(prevTimes, currTimes, prevValues, currValues, 8e9, nil)
	assertEqual(t, float64(1), result)
	assertEqual(t, true, ok)

	prevTimes = []int64{}
	currTimes = []int64{}
	prevValues = []float64{}
	currValues = []float64{}

	result, ok = intervalExistMark(prevTimes, currTimes, prevValues, currValues, 8e9, nil)
	assertEqual(t, float64(0), result)
	assertEqual(t, false, ok)
}
func TestFloatLastOverTime(t *testing.T) {
	testCases := []struct {
		preTimes   []int64
		currTimes  []int64
		preValues  []float64
		currValues []float64
		ts         int64
		call       *influxql.PromSubCall
		expected   float64
		ok         bool
	}{
		{[]int64{}, []int64{}, []float64{}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 0, false},                         //empty
		{[]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 20, true}, // normal
		{[]int64{1000000000}, []int64{}, []float64{10}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 10, true},             // one point
		{[]int64{}, []int64{3000000000}, []float64{}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 20, true},
	}

	for _, tc := range testCases {
		result, ok := lastOverTime(tc.preTimes, tc.currTimes, tc.preValues, tc.currValues, tc.ts, tc.call)
		equal := Float64Equal(result, tc.expected)
		if !equal {
			t.Errorf("TestLastOverTime failed: expected %v but got %v", tc.expected, result)
		}
		assertEqual(t, true, equal)
		assertEqual(t, tc.ok, ok)
	}
}

func TestFloatQuantileOverTime(t *testing.T) {
	testCases := []struct {
		preTimes   []int64
		currTimes  []int64
		preValues  []float64
		currValues []float64
		ts         int64
		call       *influxql.PromSubCall
		expected   float64
		ok         bool
	}{
		{[]int64{}, []int64{}, []float64{}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 0.8}}}, 0, false},                                  // empty
		{[]int64{}, []int64{1000000000}, []float64{}, []float64{10}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 0.8}}}, 10, true},                      // one point
		{[]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 1.1}}}, math.Inf(1), true}, //INF
		{[]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: -1.1}}}, math.Inf(-1), true},
		{[]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 0.5}}}, 15, true}, //normal
		{[]int64{1000000000}, []int64{3000000000}, []float64{10, 15}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 0.3}}}, 13, true},
	}
	for _, tc := range testCases {
		result, ok := quantileOverTime(tc.preTimes, tc.currTimes, tc.preValues, tc.currValues, tc.ts, tc.call)
		equal := Float64Equal(result, tc.expected)
		if !equal {
			t.Errorf("TestQuantileOverTime failed: expected %v but got %v", tc.expected, result)
		}
		assertEqual(t, true, equal)
		assertEqual(t, tc.ok, ok)
	}

}

func TestFloatChanges(t *testing.T) {

	testCases := []struct {
		preTimes   []int64
		currTimes  []int64
		preValues  []float64
		currValues []float64
		ts         int64
		call       *influxql.PromSubCall
		expected   float64
		ok         bool
	}{
		{[]int64{}, []int64{}, []float64{}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 0, false},                        //empty
		{[]int64{1000000000}, []int64{}, []float64{10}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 0, true},             // one point
		{[]int64{1000000000}, []int64{3000000000}, []float64{10}, []float64{10}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 0, true}, // two points
		{[]int64{1000000000}, []int64{3000000000}, []float64{5}, []float64{20}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 1, true},
		{[]int64{1000000000, 2000000000}, []int64{300000000, 4000000000}, []float64{10, 20}, []float64{20, 10}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 2, true},
		{[]int64{1000000000, 2000000000}, []int64{300000000, 4000000000}, []float64{10, math.NaN()}, []float64{20, 10}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 3, true},  //NAN
		{[]int64{1000000000, 2000000000}, []int64{300000000, 4000000000}, []float64{10, math.Inf(1)}, []float64{20, 10}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 3, true}, //INF

	}

	for _, tc := range testCases {
		result, ok := changes(tc.preTimes, tc.currTimes, tc.preValues, tc.currValues, tc.ts, tc.call)
		equal := Float64Equal(result, tc.expected)
		if !equal {
			t.Errorf("TestChanges failed: expected %v but got %v", tc.expected, result)
		}
		assertEqual(t, true, equal)
		assertEqual(t, ok, tc.ok)
	}

}

func TestFloatResets(t *testing.T) {

	testCases := []struct {
		preTimes   []int64
		currTimes  []int64
		preValues  []float64
		currValues []float64
		ts         int64
		call       *influxql.PromSubCall
		expected   float64
		ok         bool
	}{
		{[]int64{}, []int64{}, []float64{}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 0, false},                                                                //no data
		{[]int64{1000000000}, []int64{}, []float64{10}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 0, true},                                                     //one point
		{[]int64{1000000000, 2000000000}, []int64{300000000, 4000000000}, []float64{10, 20}, []float64{20, 10}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 1, true},          //normal
		{[]int64{1000000000, 2000000000}, []int64{300000000, 4000000000}, []float64{10, math.NaN()}, []float64{20, 10}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 1, true},  //NAN
		{[]int64{1000000000, 2000000000}, []int64{300000000, 4000000000}, []float64{10, math.Inf(1)}, []float64{20, 10}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 2, true}, //INF
		{[]int64{1000000000, 2000000000}, []int64{300000000, 4000000000}, []float64{10, math.Inf(-1)}, []float64{20, 10}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000)}, 2, true},
	}
	for _, tc := range testCases {
		result, ok := resets(tc.preTimes, tc.currTimes, tc.preValues, tc.currValues, tc.ts, tc.call)
		equal := Float64Equal(result, tc.expected)
		if !equal {
			t.Errorf("TestResets failed: expected %v but got %v", tc.expected, result)
		}
		assertEqual(t, true, equal)
		assertEqual(t, ok, tc.ok)
	}

}

func TestFloatHoltWinters(t *testing.T) {

	testCases := []struct {
		preTimes   []int64
		currTimes  []int64
		preValues  []float64
		currValues []float64
		ts         int64
		call       *influxql.PromSubCall
		expected   float64
		ok         bool
	}{
		{[]int64{1e9, 2e9}, []int64{3e9, 4e9}, []float64{1, 2}, []float64{3, 4}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 0.3}, &influxql.NumberLiteral{Val: 0.3}}}, 3.999999999999999, true}, //normal
		{[]int64{1e9}, []int64{}, []float64{1}, []float64{}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 0.3}, &influxql.NumberLiteral{Val: 0.3}}}, 0, false},                                    //  points is less than 2
		{[]int64{1e9, 2e9}, []int64{3e9, 4e9}, []float64{1, 2}, []float64{3, 4}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 1.3}, &influxql.NumberLiteral{Val: 0.3}}}, 0, false},                //normal
	}
	for _, tc := range testCases {
		result, ok := holtWintersProm(tc.preTimes, tc.currTimes, tc.preValues, tc.currValues, tc.ts, tc.call)
		equal := Float64Equal(result, tc.expected)
		if !equal {
			t.Errorf("TestResets failed: expected %v but got %v", tc.expected, result)
		}
		assertEqual(t, true, equal)
		assertEqual(t, ok, tc.ok)
	}
	//  Test the case is contained NaN
	result, ok := holtWintersProm([]int64{1e9}, []int64{2e9}, []float64{math.NaN()}, []float64{2}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 0.3}, &influxql.NumberLiteral{Val: 0.3}}})
	assertEqual(t, true, math.IsNaN(result))
	assertEqual(t, true, ok)

	//  Test the case is contained Inf()
	result, ok = holtWintersProm([]int64{1e9}, []int64{2e9}, []float64{math.Inf(1)}, []float64{2}, 0, &influxql.PromSubCall{Range: time.Duration(1000000000), InArgs: []influxql.Expr{&influxql.NumberLiteral{Val: 0.3}, &influxql.NumberLiteral{Val: 0.3}}})
	assertEqual(t, true, math.IsNaN(result))
	assertEqual(t, true, ok)

}
