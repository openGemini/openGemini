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
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
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

func TestMedian(t *testing.T) {
	type medianTestCase struct {
		name    string
		data    []float64
		want    float64
		wantErr error
	}

	testCases := []medianTestCase{
		{name: "empty slice", data: []float64{}, want: 0, wantErr: errors.New("empty data")},
		{name: "single element (int)", data: []float64{5}, want: 5, wantErr: nil},
		{name: "single element (float)", data: []float64{3.14}, want: 3.14, wantErr: nil},
		{name: "odd length (sorted)", data: []float64{1, 2, 3, 4, 5}, want: 3, wantErr: nil},
		{name: "odd length (unsorted)", data: []float64{5, 2, 7, 1, 3}, want: 3, wantErr: nil},
		{name: "even length (sorted)", data: []float64{1, 2, 3, 4}, want: 2.5, wantErr: nil},
		{name: "even length (unsorted)", data: []float64{4, 1, 3, 2}, want: 2.5, wantErr: nil},
		{name: "negative numbers (odd)", data: []float64{-5, -3, -1}, want: -3, wantErr: nil},
		{name: "mixed positive and negative (odd)", data: []float64{-3, 0, 5}, want: 0, wantErr: nil},
		{name: "mixed positive and negative (even)", data: []float64{-2, -1, 1, 2}, want: 0, wantErr: nil},
		{name: "large numbers", data: []float64{1000000, 2000000, 3000000, 4000000}, want: 2500000, wantErr: nil},
		{name: "small decimals", data: []float64{0.1, 0.2, 0.3, 0.6, 0.4, 0.5}, want: 0.35, wantErr: nil},
		{
			name:    "extreme float values",
			data:    []float64{math.SmallestNonzeroFloat64, math.MaxFloat64, -math.MaxFloat64, 0},
			want:    (math.SmallestNonzeroFloat64 + 0) / 2,
			wantErr: nil,
		},
		{
			name:    "precision sensitive (small differences)",
			data:    []float64{0.123456789012345, 0.123456789012346, 0.123456789012344, 0.123456789012347},
			want:    (0.123456789012345 + 0.123456789012346) / 2,
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := util.Median(tc.data)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}
			require.InDelta(t, tc.want, got, util.PrecisionThreshold,
				"Median calculation error: want %v, got %v", tc.want, got)
		})
	}
}

func TestTrimMean(t *testing.T) {
	type trimMeanTestCase struct {
		name            string
		data            []float64
		proportiontocut float64
		want            float64
		wantErr         error
	}

	testCases := []trimMeanTestCase{
		{name: "empty data", data: []float64{}, proportiontocut: 0.1, want: 0, wantErr: errors.New("empty data")},
		{name: "negative proportion", data: []float64{1, 2, 3}, proportiontocut: -0.1, want: 0, wantErr: errors.New("proportionToCut must be in [0, 0.5)")},
		{name: "proportion >=0.5", data: []float64{1, 2, 3}, proportiontocut: 0.5, want: 0, wantErr: errors.New("proportionToCut must be in [0, 0.5)")},
		{name: "proportion 0 (no trim)", data: []float64{1, 2, 3, 4, 5}, proportiontocut: 0, want: 3.0, wantErr: nil},
		{name: "odd length with trim", data: []float64{1, 2, 3, 4, 5, 6, 7}, proportiontocut: 0.2, want: 4.0, wantErr: nil},
		{name: "even length with trim", data: []float64{10, 20, 30, 40, 50, 60}, proportiontocut: 0.2, want: 35.0, wantErr: nil},
		{name: "unsorted data", data: []float64{5, 3, 1, 2, 4}, proportiontocut: 0.2, want: 3.0, wantErr: nil},
		{name: "with duplicates", data: []float64{2, 2, 3, 3, 3, 3, 4, 4}, proportiontocut: 0.25, want: 3.0, wantErr: nil},
		{name: "negative numbers", data: []float64{-5, -3, -1, 1, 3, 5}, proportiontocut: 0.2, want: 0.0, wantErr: nil},
		{name: "mixed positive and negative", data: []float64{-10, -5, 0, 5, 10, 15, 20}, proportiontocut: 0.2, want: 5.0, wantErr: nil},
		{name: "large numbers", data: []float64{100, 200, 300, 400, 500, 600, 700, 800, 900}, proportiontocut: 0.2, want: 500.0, wantErr: nil},
		{name: "small decimals", data: []float64{0.1, 0.2, 0.3, 0.4, 0.5}, proportiontocut: 0.2, want: 0.3, wantErr: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := util.TrimMean(tc.data, tc.proportiontocut)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			require.InDelta(t, tc.want, got, util.PrecisionThreshold,
				"TrimMean calculation error: want %v, got %v", tc.want, got)
		})
	}
}

func TestQuantile(t *testing.T) {
	type quantileTestCase struct {
		name    string
		data    []float64
		q       float64
		want    float64
		wantErr error
	}

	testCases := []quantileTestCase{
		{name: "empty data", data: []float64{}, q: 0.5, want: 0, wantErr: errors.New("empty data")},
		{name: "q < 0", data: []float64{1, 2, 3}, q: -0.1, want: 0, wantErr: errors.New("q must be in [0, 1], got -0.1")},
		{name: "q > 1", data: []float64{1, 2, 3}, q: 1.1, want: 0, wantErr: errors.New("q must be in [0, 1], got 1.1")},
		{name: "single element", data: []float64{5}, q: 0.5, want: 5, wantErr: nil},
		{name: "q=0 (min)", data: []float64{3, 1, 2}, q: 0, want: 1, wantErr: nil},
		{name: "q=1 (max)", data: []float64{3, 1, 2}, q: 1, want: 3, wantErr: nil},
		{name: "odd length median (q=0.5)", data: []float64{1, 3, 5, 7, 9}, q: 0.5, want: 5, wantErr: nil},
		{name: "even length median (q=0.5)", data: []float64{1, 3, 5, 7}, q: 0.5, want: 4, wantErr: nil},            // (3 + 5)/2
		{name: "linear interpolation (q=0.25)", data: []float64{10, 20, 30, 40}, q: 0.25, want: 17.5, wantErr: nil}, // index=0.75 → k=0, d=0.75 → (1-0.75)*10 + 0.75*20 = 17.5
		{name: "linear interpolation (q=0.75)", data: []float64{10, 20, 30, 40}, q: 0.75, want: 32.5, wantErr: nil}, // index=2.25 → k=2, d=0.25 → 0.75*30 + 0.25*40 = 32.5
		{name: "unsorted data (q=0.3)", data: []float64{5, 2, 8, 1, 9}, q: 0.3, want: 2.6, wantErr: nil},            // index=1.2 → k=1, d=0.2 → 0.8*2 + 0.2*5 = 2.8
		{name: "duplicate values", data: []float64{2, 2, 2, 2}, q: 0.5, want: 2, wantErr: nil},
		{name: "negative numbers", data: []float64{-5, -3, -1, 1}, q: 0.75, want: -0.5, wantErr: nil},      // → 0.75*(-1) + 0.25*1 = -0.5
		{name: "mixed sign (q=0.6)", data: []float64{-10, 0, 10, 20, 30}, q: 0.6, want: 14, wantErr: nil},  // index=2.4 → k=2（10）, d=0.4 → 0.6*10 + 0.4*20 = 14
		{name: "large numbers", data: []float64{100, 200, 300, 400, 500}, q: 0.9, want: 460, wantErr: nil}, // index=3.6 → k=3（400）, d=0.6 → 0.4*400 + 0.6*500 = 460
		{name: "small decimals", data: []float64{0.1, 0.2, 0.3, 0.4}, q: 0.3, want: 0.19, wantErr: nil},    // index=0.9 → k=0（0.1）, d=0.9 → 0.1*0.1 + 0.9*0.2 = 0.19
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := util.Quantile(tc.data, tc.q)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tc.wantErr.Error(), err.Error())
				return
			}
			require.NoError(t, err)
			require.InDelta(t, tc.want, got, util.PrecisionThreshold,
				"quantile calculation error: want %v, got %v", tc.want, got)
		})
	}
}

func TestWaitTimeOut(t *testing.T) {
	var run = func() bool {
		wg := sync.WaitGroup{}
		wg.Add(1)

		ok := false
		go func() {
			util.WaitTimeOut(wg.Wait, wg.Done, time.Millisecond*100)
			fmt.Println(222)
			ok = true
		}()

		for range 10 {
			if ok {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
		return ok
	}
	require.True(t, run())

	pt := gomonkey.ApplyFunc(util.TickerRun, func(d time.Duration, stopSignal <-chan struct{}, onTick func(), onStop func()) {
		panic(1)
	})
	defer pt.Reset()
	require.False(t, run())
}

func TestMergeMaps(t *testing.T) {
	type args struct {
		oldMap map[string]int
		newMap map[string]int
	}
	tests := []struct {
		name string
		args args
		want map[string]int
	}{
		{
			name: "Both maps are non-nil and have no overlapping keys",
			args: args{
				oldMap: map[string]int{"a": 1, "b": 2},
				newMap: map[string]int{"c": 3, "d": 4},
			},
			want: map[string]int{"a": 1, "b": 2, "c": 3, "d": 4},
		},
		{
			name: "Both maps are non-nil and have overlapping keys",
			args: args{
				oldMap: map[string]int{"a": 1, "b": 2},
				newMap: map[string]int{"b": 3, "c": 4},
			},
			want: map[string]int{"a": 1, "b": 3, "c": 4},
		},
		{
			name: "Old map is nil",
			args: args{
				oldMap: nil,
				newMap: map[string]int{"a": 1, "b": 2},
			},
			want: map[string]int{"a": 1, "b": 2},
		},
		{
			name: "New map is nil",
			args: args{
				oldMap: map[string]int{"a": 1, "b": 2},
				newMap: nil,
			},
			want: map[string]int{"a": 1, "b": 2},
		},
		{
			name: "Both maps are nil",
			args: args{
				oldMap: nil,
				newMap: nil,
			},
			want: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := util.MergeMaps(tt.args.oldMap, tt.args.newMap); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MergeMaps() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEpochDivisor(t *testing.T) {
	require.Equal(t, int64(1), util.EpochDivisor("rfc3339"))
	require.Equal(t, int64(1), util.EpochDivisor("ns"))
	require.Equal(t, int64(1), util.EpochDivisor("mm"))

	require.Equal(t, int64(time.Microsecond), util.EpochDivisor("u"))
	require.Equal(t, int64(time.Microsecond), util.EpochDivisor("us"))
	require.Equal(t, int64(time.Millisecond), util.EpochDivisor("ms"))
	require.Equal(t, int64(time.Second), util.EpochDivisor("s"))
	require.Equal(t, int64(time.Minute), util.EpochDivisor("m"))
	require.Equal(t, int64(time.Hour), util.EpochDivisor("h"))
}

// TestCalculateSum tests the CalculateSum function
func TestCalculateSum(t *testing.T) {
	type sumTestCase struct {
		name     string
		values   []int64
		expected int64
	}

	// Test int64 values
	intTestCases := []sumTestCase{
		{name: "empty slice", values: []int64{}, expected: 0},
		{name: "single element", values: []int64{5}, expected: 5},
		{name: "multiple positive numbers", values: []int64{1, 2, 3, 4, 5}, expected: 15},
		{name: "multiple negative numbers", values: []int64{-1, -2, -3, -4, -5}, expected: -15},
		{name: "mixed positive and negative", values: []int64{-1, 2, -3, 4, -5}, expected: -3},
		{name: "with zero", values: []int64{0, 0, 0}, expected: 0},
		{name: "large numbers", values: []int64{1000000, 2000000, 3000000}, expected: 6000000},
	}

	for _, tc := range intTestCases {
		t.Run(tc.name, func(t *testing.T) {
			result := util.CalculateSum(tc.values)
			assert.Equal(t, tc.expected, result)
		})
	}

	// Test float64 values
	floatTestCases := []struct {
		name     string
		values   []float64
		expected float64
	}{
		{name: "empty slice", values: []float64{}, expected: 0.0},
		{name: "single element", values: []float64{3.14}, expected: 3.14},
		{name: "multiple positive numbers", values: []float64{1.1, 2.2, 3.3}, expected: 6.6},
		{name: "multiple negative numbers", values: []float64{-1.1, -2.2, -3.3}, expected: -6.6},
		{name: "mixed positive and negative", values: []float64{-1.5, 2.5, -3.5, 4.5}, expected: 2.0},
		{name: "with zero", values: []float64{0.0, 0.0, 0.0}, expected: 0.0},
		{name: "small decimals", values: []float64{0.1, 0.2, 0.3, 0.4}, expected: 1.0},
		{name: "precision test", values: []float64{0.1, 0.2}, expected: 0.3},
	}

	for _, tc := range floatTestCases {
		t.Run(tc.name, func(t *testing.T) {
			result := util.CalculateSum(tc.values)
			// Use InDelta for floating point comparison to handle precision issues
			assert.InDelta(t, tc.expected, result, util.PrecisionThreshold)
		})
	}
}

// TestCalculateMax tests the CalculateMax function
func TestCalculateMax(t *testing.T) {
	type maxTestCase[T util.NumberOnly] struct {
		name     string
		values   []T
		expected T
	}

	defaultInt64Max := int64(math.MinInt64)
	// Test int64 values
	intTestCases := []maxTestCase[int64]{
		{name: "empty slice", values: []int64{}, expected: math.MinInt64},
		{name: "single element", values: []int64{5}, expected: 5},
		{name: "ascending order", values: []int64{1, 2, 3, 4, 5}, expected: 5},
		{name: "descending order", values: []int64{5, 4, 3, 2, 1}, expected: 5},
		{name: "random order", values: []int64{3, 1, 4, 1, 5, 9, 2, 6}, expected: 9},
		{name: "all negative", values: []int64{-5, -3, -1, -2, -4}, expected: -1},
		{name: "mixed positive and negative", values: []int64{-5, 3, -1, 0, -2}, expected: 3},
		{name: "with min int64", values: []int64{math.MinInt64, 0, 1}, expected: 1},
	}

	for _, tc := range intTestCases {
		t.Run(tc.name, func(t *testing.T) {
			result := util.CalculateMax(tc.values, defaultInt64Max)
			assert.Equal(t, tc.expected, result)
		})
	}

	defaultFloat64Max := -math.MaxFloat64
	// Test float64 values
	floatTestCases := []maxTestCase[float64]{
		{name: "empty slice", values: []float64{}, expected: -math.MaxFloat64},
		{name: "single element", values: []float64{3.14}, expected: 3.14},
		{name: "maximum values", values: []float64{1.1, -math.MaxFloat64, 3.3}, expected: 3.3},
		{name: "all positive", values: []float64{1.1, 2.2, 3.3}, expected: 3.3},
		{name: "all negative", values: []float64{-1.1, -2.2, -3.3}, expected: -1.1},
		{name: "mixed positive and negative", values: []float64{-1.5, 2.5, -3.5, 4.5}, expected: 4.5},
		{name: "positive and zero", values: []float64{0.0, 1.5, -2.5}, expected: 1.5},
		{name: "with nan", values: []float64{1.0, math.NaN(), 2.0}, expected: 2.0},
	}

	for _, tc := range floatTestCases {
		t.Run(tc.name, func(t *testing.T) {
			result := util.CalculateMax(tc.values, defaultFloat64Max)
			assert.InDelta(t, tc.expected, result, util.PrecisionThreshold)
		})
	}
}

// TestCalculateMin tests the CalculateMin function
func TestCalculateMin(t *testing.T) {
	type minTestCase[T util.NumberOnly] struct {
		name     string
		values   []T
		expected T
	}

	defaultInt64Min := int64(math.MaxInt64)
	// Test int64 values
	intTestCases := []minTestCase[int64]{
		{name: "empty slice", values: []int64{}, expected: math.MaxInt64},
		{name: "single element", values: []int64{5}, expected: 5},
		{name: "ascending order", values: []int64{1, 2, 3, 4, 5}, expected: 1},
		{name: "descending order", values: []int64{5, 4, 3, 2, 1}, expected: 1},
		{name: "random order", values: []int64{3, 1, 4, 1, 5, 9, 2, 6}, expected: 1},
		{name: "all negative", values: []int64{-5, -3, -1, -2, -4}, expected: -5},
		{name: "mixed positive and negative", values: []int64{-5, 3, -1, 0, -2}, expected: -5},
		{name: "with max int64", values: []int64{math.MaxInt64, 0, 1}, expected: 0},
	}

	for _, tc := range intTestCases {
		t.Run(tc.name, func(t *testing.T) {
			result := util.CalculateMin(tc.values, defaultInt64Min)
			assert.Equal(t, tc.expected, result)
		})
	}

	defaultFloat64Min := math.MaxFloat64
	// Test float64 values
	floatTestCases := []struct {
		name     string
		values   []float64
		expected float64
	}{
		{name: "empty slice", values: []float64{}, expected: math.MaxFloat64},
		{name: "single element", values: []float64{3.14}, expected: 3.14},
		{name: "minimum values", values: []float64{1.1, math.MaxFloat64, 3.3}, expected: 1.1},
		{name: "all positive", values: []float64{1.1, 2.2, 3.3}, expected: 1.1},
		{name: "all negative", values: []float64{-1.1, -2.2, -3.3}, expected: -3.3},
		{name: "mixed positive and negative", values: []float64{-1.5, 2.5, -3.5, 4.5}, expected: -3.5},
		{name: "positive and zero", values: []float64{0.0, 1.5, -2.5}, expected: -2.5},
		{name: "with nan", values: []float64{1.0, math.NaN(), 2.0}, expected: 1.0},
	}

	for _, tc := range floatTestCases {
		t.Run(tc.name, func(t *testing.T) {
			result := util.CalculateMin(tc.values, defaultFloat64Min)
			assert.InDelta(t, tc.expected, result, util.PrecisionThreshold)
		})
	}
}
