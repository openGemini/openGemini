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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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

func TestFindIntersectionIndex(t *testing.T) {
	type testCase[T1, T2 any] struct {
		name          string
		slice1        []T1
		slice2        []T2
		compareFunc   func(T1, T2) int
		fn            func(i, j int) error
		expectedError error
		expectedRes   [][]int
	}
	type Person struct {
		Name string
		Age  int
	}
	compareFunc := func(a, b Person) int {
		return strings.Compare(a.Name, b.Name)
	}
	testCases := []testCase[Person, Person]{
		{
			name: "Normal case with intersection1",
			slice1: []Person{
				{Name: "Alice", Age: 25},
				{Name: "Bob", Age: 30},
				{Name: "Charlie", Age: 35},
			},
			slice2: []Person{
				{Name: "Bob", Age: 30},
				{Name: "Charlie", Age: 35},
				{Name: "David", Age: 40},
			},
			compareFunc:   compareFunc,
			expectedRes:   [][]int{{1, 0}, {2, 1}},
			expectedError: nil,
		},
		{
			name: "Normal case with intersection2",
			slice1: []Person{
				{Name: "Bob", Age: 30},
				{Name: "Charlie", Age: 35},
			},
			slice2: []Person{
				{Name: "Alice", Age: 35},
				{Name: "Charlie", Age: 35},
				{Name: "David", Age: 40},
			},
			compareFunc:   compareFunc,
			expectedRes:   [][]int{{1, 1}},
			expectedError: nil,
		},
		{
			name: "Normal case with intersection3",
			slice1: []Person{
				{Name: "Bob", Age: 30},
				{Name: "Yale", Age: 35},
				{Name: "Zip", Age: 20},
			},
			slice2: []Person{
				{Name: "Alice", Age: 35},
				{Name: "Charlie", Age: 35},
				{Name: "Zip", Age: 40},
			},
			compareFunc:   compareFunc,
			expectedRes:   [][]int{{2, 2}},
			expectedError: nil,
		},
		{
			name: "No intersection",
			slice1: []Person{
				{Name: "Alice", Age: 25},
				{Name: "Bob", Age: 30},
			},
			slice2: []Person{
				{Name: "Charlie", Age: 35},
				{Name: "David", Age: 40},
			},
			compareFunc:   compareFunc,
			expectedError: nil,
			expectedRes:   [][]int{},
		},
		{
			name:          "Empty slices",
			slice1:        []Person{},
			slice2:        []Person{},
			compareFunc:   compareFunc,
			expectedRes:   [][]int{},
			expectedError: nil,
		},
		{
			name:          "Nil slices",
			slice1:        nil,
			slice2:        nil,
			compareFunc:   compareFunc,
			expectedError: nil,
			expectedRes:   [][]int{},
		},
		{
			name: "Invalid input: compareFunc is nil",
			slice1: []Person{
				{Name: "Bob", Age: 30},
			},
			slice2:        []Person{},
			expectedRes:   [][]int{},
			expectedError: errors.New("compareFunc or callbackFunc cannot be nil"),
		},
		{
			name:        "Invalid input: compareFunc return a invalid number",
			slice1:      []Person{{Name: "Bob", Age: 30}},
			slice2:      []Person{{Name: "Bob", Age: 30}},
			expectedRes: [][]int{},
			compareFunc: func(a Person, b Person) int {
				return 10
			},
			expectedError: errors.New("invalid compareFunc result"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result [][]int
			err := util.FindIntersectionIndex(tc.slice1, tc.slice2, tc.compareFunc, func(i, j int) error {
				result = append(result, []int{i, j})
				return nil
			})

			if err != nil && err.Error() != tc.expectedError.Error() {
				t.Errorf("Expected error %v, got %v", tc.expectedError, err)
			}

			if !equalSlices(result, tc.expectedRes) {
				t.Errorf("Expected result %v, got %v", tc.expectedRes, result)
			}
		})
	}
}

func equalSlices(a, b [][]int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}
func TestSyncMapStore(t *testing.T) {

	m := util.NewSyncMap[string, int]()

	t.Run("store and load", func(t *testing.T) {
		key := "key1"
		value := 100
		m.Store(key, value)
		stored, exists := m.Load(key)
		require.True(t, exists)
		assert.Equal(t, value, stored)

	})
}

func TestNewSyncMap(t *testing.T) {

	m := util.NewSyncMap[string, int]()

	t.Run("new SyncMap", func(t *testing.T) {
		key := "key1"
		value := 100
		m.Store(key, value)
		stored, exists := m.Load(key)
		require.True(t, exists)
		assert.Equal(t, value, stored)
	})
}

func TestSyncMap_Len(t *testing.T) {

	m := util.NewSyncMap[string, int]()

	t.Run("store and load", func(t *testing.T) {
		key := "key1"
		value := 100
		m.Store(key, value)
		l := m.Len()
		_, exists := m.Load(key)
		require.True(t, exists)
		assert.Equal(t, 1, l)
	})
}

func TestSyncMap_Load(t *testing.T) {

	m := util.NewSyncMap[string, int]()

	t.Run("store and load", func(t *testing.T) {
		key := "key1"
		value := 100
		m.Store(key, value)
		stored, exists := m.Load(key)
		require.True(t, exists)
		assert.Equal(t, value, stored)
	})
}

func TestSyncMap_LoadOrStore(t *testing.T) {
	t.Run("store or load", func(t *testing.T) {
		m := util.NewSyncMap[string, int]()
		key := "existing"
		m.Store(key, 42)

		creatorCalled := false
		v, loaded := m.LoadOrStore(key, func() int {
			creatorCalled = true
			return 100
		})

		assert.True(t, !creatorCalled)
		assert.Equal(t, 42, v)
		assert.Equal(t, true, loaded)

	})
}

func TestSyncMap_Range(t *testing.T) {

	t.Run("Iterates all key-value pairs", func(t *testing.T) {
		m := util.NewSyncMap[string, int]()
		m.Store("a", 1)
		m.Store("b", 2)
		m.Store("c", 3)

		var visitedKeys []string
		var visitedValues []int

		m.Range(func(k string, v int) {
			visitedKeys = append(visitedKeys, k)
			visitedValues = append(visitedValues, v)
		})

		// Verify 3 elements were iterated
		assert.Len(t, visitedKeys, 3, "Should iterate 3 keys")
		assert.Len(t, visitedValues, 3, "Should iterate 3 values")

		// Verify all elements were covered
		assert.ElementsMatch(t, []string{"a", "b", "c"}, visitedKeys, "Iterated keys don't match")
		assert.ElementsMatch(t, []int{1, 2, 3}, visitedValues, "Iterated values don't match")
	})

	t.Run("Multiple concurrent iterations are safe", func(t *testing.T) {
		m := util.NewSyncMap[string, string]()
		for i := 0; i < 100; i++ {
			key := "key_" + string(rune(i))
			m.Store(key, "value_"+string(rune(i)))
		}

		const routines = 5
		var counter atomic.Int32
		var wg sync.WaitGroup
		wg.Add(routines)

		for i := 0; i < routines; i++ {
			go func() {
				defer wg.Done()
				m.Range(func(k, v string) {
					counter.Add(1)
				})
			}()
		}
		wg.Wait()

		assert.Equal(t, routines*100, int(counter.Load()), "Callback count should be routines * 100")
	})

}

func TestSplitUint64(t *testing.T) {
	original := uint64(0)
	high, low := util.SplitUint64(original)
	if high != 0 || low != 0 {
		t.Errorf("expected high and low to be 0, got high: %d, low: %d", high, low)
	}

	original = uint64(0xFFFFFFFFFFFFFFFF)
	high, low = util.SplitUint64(original)
	if high != uint32(0xFFFFFFFF) || low != uint32(0xFFFFFFFF) {
		t.Errorf("expected high and low to be uint32(0xFFFFFFFF), got high: %d, low: %d", high, low)
	}

	original = uint64(0xFFFFFFFF)
	high, low = util.SplitUint64(original)
	if high != 0 || low != uint32(0xFFFFFFFF) {
		t.Errorf("expected high to be 0 and low to be uint32(0xFFFFFFFF), got high: %d, low: %d", high, low)
	}

	original = uint64(0x100000000)
	high, low = util.SplitUint64(original)
	if high != 1 || low != 0 {
		t.Errorf("expected high to be 1 and low to be 0, got high: %d, low: %d", high, low)
	}
}
