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

package engine

import (
	"math"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	assert1 "github.com/stretchr/testify/assert"
)

var times = []int64{1 * 1e9, 2 * 1e9, 3 * 1e9, 4 * 1e9}
var values = []float64{1, 2, 3, 4}

func Test_floatPresentOverTimeReduce(t *testing.T) {
	_, _, ok := floatPresentOverTimeReduce(times, values, 3, 0)
	assert1.Equal(t, true, ok)

	_, _, ok = floatPresentOverTimeReduce(times, values, 0, 3)
	assert1.Equal(t, false, ok)
}

func Test_floatPresentOverTimeMerge(t *testing.T) {
	present := floatPresentOverTimeMerge()
	v, ok := present(nil, times, nil, values, 0, 0, nil)
	assert1.Equal(t, float64(0), v)
	assert1.Equal(t, true, ok)

	v, ok = present(nil, times, nil, values, 0, 2, nil)
	assert1.Equal(t, false, ok)
	assert1.Equal(t, float64(1), v)
}

func Test_floatChangesMerger(t *testing.T) {
	changesFn := floatChangesMerger()
	v, ok := changesFn(nil, times, nil, values, 0, 0, nil)
	assert1.Equal(t, true, ok)

	v, ok = changesFn(nil, times, nil, values, 0, 2, nil)
	assert1.Equal(t, false, ok)
	assert1.Equal(t, float64(3), v)
}

func Test_floatMadOverTimeMerger(t *testing.T) {
	tests := []struct {
		name   string
		prevT  []int64
		currT  []int64
		prevV  []float64
		curV   []float64
		ts     int64
		c      int
		param  *ReducerParams
		want   float64
		wantOk bool
	}{
		{
			name:   "empty inputs",
			prevT:  []int64{},
			currT:  []int64{},
			prevV:  []float64{},
			curV:   []float64{},
			ts:     0,
			c:      0,
			param:  nil,
			want:   math.NaN(),
			wantOk: true,
		},
		{
			name:   "non-empty inputs",
			prevT:  []int64{1, 2, 3},
			currT:  []int64{4, 5},
			prevV:  []float64{1.1, 2.2, 3.3},
			curV:   []float64{4.4, 5.5},
			ts:     0,
			c:      5,
			param:  nil,
			want:   1.1000000000000005,
			wantOk: false,
		},
		{
			name:   "c == 0",
			prevT:  []int64{1, 2, 3},
			currT:  []int64{4, 5},
			prevV:  []float64{1.1, 2.2, 3.3},
			curV:   []float64{4.4, 5.5},
			ts:     0,
			c:      0,
			param:  nil,
			want:   math.NaN(),
			wantOk: true,
		},
		{
			name:   "timesCount == 0",
			prevT:  []int64{},
			currT:  []int64{},
			prevV:  []float64{1.1, 2.2, 3.3},
			curV:   []float64{4.4, 5.5},
			ts:     0,
			c:      5,
			param:  nil,
			want:   math.NaN(),
			wantOk: true,
		},
		{
			name:   "valuesCount == 0",
			prevT:  []int64{1, 2, 3},
			currT:  []int64{4, 5},
			prevV:  []float64{},
			curV:   []float64{},
			ts:     0,
			c:      5,
			param:  nil,
			want:   math.NaN(),
			wantOk: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, ok := floatMadOverTimeMerger()(test.prevT, test.currT, test.prevV, test.curV, test.ts, test.c, test.param)
			equal := executor.Float64Equal(result, test.want)
			if !equal {
				t.Errorf("floatMadOverTimeMerger() failed: expected %v but got %v", test.want, result)
			}
			assert1.Equal(t, true, equal)
			assert1.Equal(t, test.wantOk, ok)
		})
	}
}

func Test_linearMergeFunc(t *testing.T) {
	tests := []struct {
		name    string
		isDeriv bool
		scalar  float64
		t1      []int64
		t2      []int64
		v1      []float64
		v2      []float64
		ts      int64
		param   *ReducerParams
		want    float64
		wantOk  bool
	}{
		{"predictLinear_simple", false, 2.0, []int64{1, 2, 3}, []int64{4, 5, 6}, []float64{7.0, 8.0, 9.0}, []float64{10.0, 11.0, 12.0}, 0, &ReducerParams{offset: 0}, 2000000006.000002, false},
		{"predictLinear_Inf", false, 2.0, []int64{1, 2, 3}, []int64{4, 5, 6}, []float64{math.Inf(1), math.Inf(1), math.Inf(1)}, []float64{math.Inf(1), math.Inf(1), math.Inf(1)}, 0, &ReducerParams{offset: 0}, math.NaN(), false},
		{"predictLinear_offset", false, 3.0, []int64{1, 2, 3}, []int64{4, 5, 6}, []float64{7.0, 8.0, 9.0}, []float64{10.0, 11.0, 12.0}, 0, &ReducerParams{offset: 10}, 3000000015.999983, false},
		{"predictLinear_constant", false, 3.0, []int64{1, 2, 3}, nil, []float64{3, 3, 3}, nil, 0, &ReducerParams{offset: 0}, 3, false},
		{"derivative_offset", true, 3.0, []int64{1, 2, 3}, []int64{4, 5, 6}, []float64{7.0, 8.0, 9.0}, []float64{10.0, 11.0, 12.0}, 0, &ReducerParams{offset: 10}, 999999999.9999943, false},
		{"derivative_constant", true, 3.0, []int64{1, 2, 3}, nil, []float64{3, 3, 3}, nil, 0, &ReducerParams{offset: 0}, 0, false},
	}

	for _, test := range tests {
		result, ok := linearMergeFunc(test.isDeriv, test.scalar)(test.t1, test.t2, test.v1, test.v2, 0, len(test.v1), test.param)
		equal := executor.Float64Equal(result, test.want)
		if !equal {
			t.Errorf("linearMergeFunc(): test case %s failed: expected %v but got %v", test.name, test.want, result)
		}
		assert1.Equal(t, true, equal)
		assert1.Equal(t, test.wantOk, ok)
	}
}
