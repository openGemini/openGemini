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

package util_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closeObject struct {
	err error
}

func (o *closeObject) Close() error {
	return o.err
}

type String string

func (s String) Close() error {
	fmt.Println(111)
	return fmt.Errorf("%s", s)
}

func TestMustClose(t *testing.T) {
	var o *closeObject
	util.MustClose(o)

	o = &closeObject{err: fmt.Errorf("some error")}
	util.MustClose(o)

	var s String
	util.MustClose(s)
}

func TestMustRun(t *testing.T) {
	o := &closeObject{err: fmt.Errorf("some error")}
	util.MustRun(o.Close)

	util.MustRun(nil)
}

func BenchmarkIsObjectNil(b *testing.B) {
	o := &closeObject{err: fmt.Errorf("some error")}
	var s String

	for i := 0; i < b.N; i++ {
		util.IsObjectNil(o)
		util.IsObjectNil(s)
	}
}

func TestCeilToPower2(t *testing.T) {
	assert.Equal(t, uint64(1), util.CeilToPower2(1))
	assert.Equal(t, uint64(2), util.CeilToPower2(2))
	assert.Equal(t, uint64(4), util.CeilToPower2(4))
	assert.Equal(t, uint64(8), util.CeilToPower2(5))
	assert.Equal(t, uint64(16), util.CeilToPower2(9))
	assert.Equal(t, uint64(32), util.CeilToPower2(26))
}

func TestNumberOfTrailingZeros(t *testing.T) {
	assert.Equal(t, 63, util.NumberOfTrailingZeros(0x8000000000000000))
}

func TestIntLimit(t *testing.T) {
	assert.Equal(t, 8, util.IntLimit(8, 64, 0))
	assert.Equal(t, 64, util.IntLimit(8, 64, 66))
	assert.Equal(t, 32, util.IntLimit(8, 64, 32))
}

func TestIntMin(t *testing.T) {
	assert.Equal(t, 5, util.Min(5, 7))
	assert.Equal(t, 5, util.Min(7, 5))
}

func TestAllocSlice(t *testing.T) {
	buf := make([]byte, 0, 30)
	var sub []byte

	size := 20
	buf, sub = util.AllocSlice(buf, size)
	require.Equal(t, size, len(buf))
	require.Equal(t, size, len(sub))

	buf, sub = util.AllocSlice(buf, size)
	require.Equal(t, size*cpu.GetCpuNum(), cap(buf))
	require.Equal(t, size, len(sub))
}

func TestDivisionCeil(t *testing.T) {
	require.Equal(t, 0, util.DivisionCeil(10, 0))
	require.Equal(t, 1, util.DivisionCeil(10, 11))
	require.Equal(t, 4, util.DivisionCeil(10, 3))
	require.Equal(t, 2, util.DivisionCeil(10, 8))
	require.Equal(t, 2, util.DivisionCeil(10, 9))
	require.Equal(t, 1, util.DivisionCeil(10, 10))
}

func TestIntersectSortedSliceInt(t *testing.T) {
	testCases := []struct {
		name     string
		a, b     []int
		expected []int
	}{
		{
			name:     "empty slices",
			a:        []int{},
			b:        []int{},
			expected: []int{},
		},
		{
			name:     "one empty slice",
			a:        []int{1, 2, 3},
			b:        []int{},
			expected: []int{},
		},
		{
			name:     "no intersection",
			a:        []int{1, 2, 3},
			b:        []int{4, 5, 6},
			expected: []int{},
		},
		{
			name:     "one element intersection 1",
			a:        []int{1, 2, 3},
			b:        []int{3, 4, 5},
			expected: []int{3},
		},
		{
			name:     "one element intersection 2",
			a:        []int{1, 2, 3},
			b:        []int{3, 4, 5, 6, 7},
			expected: []int{3},
		},
		{
			name:     "one element intersection 3",
			a:        []int{1, 2, 3, 9, 10},
			b:        []int{3, 4, 5},
			expected: []int{3},
		},
		{
			name:     "multiple element intersection 1",
			a:        []int{1, 2, 3, 4, 5},
			b:        []int{3, 4, 5, 6, 7},
			expected: []int{3, 4, 5},
		},
		{
			name:     "multiple element intersection 2",
			a:        []int{3, 4, 5, 6, 7, 8, 9},
			b:        []int{1, 2, 3, 4, 5},
			expected: []int{3, 4, 5},
		},
		{
			name:     "multiple element intersection 3",
			a:        []int{1, 2, 3, 4, 5},
			b:        []int{3, 4, 5, 6, 7, 8, 9},
			expected: []int{3, 4, 5},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := util.IntersectSortedSliceInt(tc.a, tc.b)
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("expected %v, but got %v", tc.expected, result)
			}
		})
	}
}

func TestUnionSortedSliceInt(t *testing.T) {
	testCases := []struct {
		name     string
		a, b     []int
		expected []int
	}{
		{
			name:     "empty slices",
			a:        []int{},
			b:        []int{},
			expected: []int{},
		},
		{
			name:     "one empty slice",
			a:        []int{1, 2, 3},
			b:        []int{},
			expected: []int{1, 2, 3},
		},
		{
			name:     "no overlap",
			a:        []int{1, 2, 3},
			b:        []int{4, 5, 6},
			expected: []int{1, 2, 3, 4, 5, 6},
		},
		{
			name:     "one element overlap 1",
			a:        []int{1, 2, 3},
			b:        []int{3, 4, 5},
			expected: []int{1, 2, 3, 4, 5},
		},
		{
			name:     "one element overlap 2",
			a:        []int{1, 2, 3},
			b:        []int{3, 4, 5, 6, 7},
			expected: []int{1, 2, 3, 4, 5, 6, 7},
		},
		{
			name:     "one element overlap 3",
			a:        []int{1, 2, 3, 6, 7},
			b:        []int{3, 4, 5},
			expected: []int{1, 2, 3, 4, 5, 6, 7},
		},
		{
			name:     "multiple element overlap 1",
			a:        []int{1, 2, 3, 4, 5},
			b:        []int{3, 4, 5, 6, 7},
			expected: []int{1, 2, 3, 4, 5, 6, 7},
		},
		{
			name:     "multiple element overlap 2",
			a:        []int{1, 2, 3, 4, 5},
			b:        []int{3, 4, 5, 6, 7, 8, 9},
			expected: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:     "multiple element overlap 3",
			a:        []int{1, 2, 3, 4, 5, 8, 9},
			b:        []int{3, 4, 5, 6, 7},
			expected: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := util.UnionSortedSliceInt(tc.a, tc.b)
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("expected %v, but got %v", tc.expected, result)
			}
		})
	}
}
