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

// This code is implemented by referring to https://github.com/UWHustle/Efficiently-Searching-In-Memory-Sorted-Arrays.
package binarysearch

import (
	"encoding/binary"
	"fmt"
	"sort"
)

func BinarySearchByKey(a []byte, sz int, compFunc func(x []byte) int) (int, int) {
	if len(a)%sz != 0 {
		panic(fmt.Sprintf("x is not a multiple of a: %d %d", len(a), sz))
	}

	iterCount := 0
	low, high := 0, len(a)-sz
	for low < high {
		iterCount++
		m := int(uint(low+high) >> 1)
		m -= m % sz
		comp := compFunc(a[m : m+sz])
		if comp == 0 {
			return m, iterCount
		} else if comp > 0 {
			low = m + sz
		} else {
			high = m
		}
	}

	if compFunc(a[low:low+sz]) == 0 {
		return low, iterCount
	}

	return len(a), iterCount
}

type ISSet struct {
	A             *[]byte
	Slope         float64
	F_aL          float64
	F_width_range float64
}

//lint:ignore U1000 test used only
func newISSet(a []byte, sz int) *ISSet {
	low, high := int(0), int(len(a)-sz)
	lowValue := float64(binary.BigEndian.Uint32(a[0:sz]))
	highValue := float64(binary.BigEndian.Uint32(a[high : high+sz]))

	return &ISSet{
		A:             &a,
		Slope:         float64(high-low) / (highValue - lowValue),
		F_aL:          lowValue,
		F_width_range: float64(high-low) / (highValue - lowValue),
	}
}

func (is *ISSet) Intrpolation1(value int, mid int, sz int) int {
	valueMid := float64(binary.BigEndian.Uint32((*is.A)[mid : mid+sz]))
	expected := mid + int((float64(value)-valueMid)*is.Slope)
	if int(valueMid) < value && expected == mid {
		expected += sz
	} else if int(valueMid) > value && expected == mid {
		expected -= sz
	}

	expected -= expected % sz
	return expected
}

func (is *ISSet) Intrpolation2(value int, sz int) int {
	expected := int((float64(value) - is.F_aL) * is.Slope)
	expected -= expected % sz
	return expected
}

type TSSet struct {
	A                    *[]byte
	d                    int
	y_1                  int
	a_0, diff_scale, d_a float64
}

//lint:ignore U1000 test used only
func newTSSet(a []byte, sz int) *TSSet {

	d := len(a) >> 1
	d -= d % sz

	y_1 := int(binary.BigEndian.Uint32(a[d : d+sz]))
	value0 := int(binary.BigEndian.Uint32(a[0:sz]))
	valueLen := int(binary.BigEndian.Uint32(a[len(a)-1*sz:]))

	diff_y_01 := value0 - y_1
	a_0 := 0.0
	if diff_y_01 == y_1-valueLen {
		a_0 = 0.99999999999999
	} else {
		a_0 = float64(diff_y_01) / float64(y_1-valueLen)
	}

	diff_scale := float64(value0) - a_0*float64(valueLen)
	d_a := (1.0 + a_0) * float64(d)

	return &TSSet{
		A:          &a,
		d:          d,
		y_1:        y_1,
		a_0:        a_0,
		diff_scale: diff_scale,
		d_a:        d_a,
	}
}

func (ts *TSSet) Intrpolation1(value int, x_0 int, x_1 int, x_2 int, sz int) int {
	valuex_0 := float64(binary.BigEndian.Uint32((*ts.A)[x_0 : x_0+sz]))
	valuex_1 := float64(binary.BigEndian.Uint32((*ts.A)[x_1 : x_1+sz]))
	valuex_2 := float64(binary.BigEndian.Uint32((*ts.A)[x_2 : x_2+sz]))
	valuey := float64(value)
	y_0 := float64(valuex_0 - valuey)
	y_1 := float64(valuex_1 - valuey)
	y_2 := float64(valuex_2 - valuey)
	error := y_1 * float64(x_1-x_2) * float64(x_1-x_0) * (y_2 - y_0) /
		(y_2*float64(x_1-x_2)*(y_0-y_1) +
			y_0*float64(x_1-x_0)*(y_1-y_2))
	id := x_1 + int(error)
	id -= id % sz
	return id
}

func (ts *TSSet) Intrpolation2(value int, sz int) int {
	valuey := float64(value)
	return ts.d + int(ts.d_a*(float64(ts.y_1)-valuey)/(ts.diff_scale-valuey*(ts.a_0+1.0)))
}

func InterpolationSearchByOffset(a []byte, sz int32, offset int32) (int, int) {
	if int32(len(a))%sz != 0 {
		panic(fmt.Sprintf("x is not a multiple of a: %d %d", len(a), sz))
	}

	iterCount := 0
	low, high := int32(0), int32(len(a))-sz
	lowValue := int32(binary.BigEndian.Uint32(a[0:4]))
	highValue := int32(binary.BigEndian.Uint32(a[high : high+4]))

	for low <= high && offset >= lowValue && offset <= highValue {
		if low == high {
			return int(low), iterCount
		}

		iterCount++

		pos := low + ((high-low)/(highValue-lowValue))*(offset-lowValue)
		pos -= pos % sz
		off := int32(binary.BigEndian.Uint32(a[pos : pos+4]))
		if off == offset {
			return int(pos), iterCount
		} else if off < offset {
			low = pos + sz
		} else {
			high = pos - sz
		}

		lowValue = int32(binary.BigEndian.Uint32(a[low : low+4]))
		highValue = int32(binary.BigEndian.Uint32(a[high : high+4]))
	}

	return len(a) + 1, iterCount
}

func sequenceSearch(sz int, start int, a []byte, reverse bool, compFu func(x []byte) int) (int, int) {
	iterCount := 0
	if !reverse {
		for {
			for i := 0; i < 8; i++ {
				iterCount++
				comp := compFu(a[start+i*sz : start+i*sz+sz])
				if comp < 0 {
					return len(a), iterCount
				} else if comp == 0 {
					return start + i*sz, iterCount
				}
			}
			start = start + 8*sz
		}
	}

	for {
		for i := 0; i < 8; i++ {
			iterCount++
			comp := compFu(a[start-i*sz : start-i*sz+sz])
			if comp > 0 {
				return len(a), iterCount
			} else if comp == 0 {
				return start - i*sz, iterCount
			}
		}
		start = start - 8*sz
	}

}

func linear_search(sz int, start int, a []byte, compFu func(x []byte) int) (int, int) {
	comp := compFu(a[start : start+sz])
	if comp >= 0 {
		return sequenceSearch(sz, start, a, false, compFu)
	} else {
		return sequenceSearch(sz, start, a, true, compFu)
	}
}

func SIPSearchByKey(a []byte, is *ISSet, sz int, x []byte, value int, compFu func(x []byte) int, guard_off int) (int, int, int) {
	if len(a)%sz != 0 {
		panic(fmt.Sprintf("x is not a multiple of a: %d %d", len(a), sz))
	}

	low, high := 0, len(a)-sz
	next := is.Intrpolation2(value, sz)
	iterCount := 1

	for {

		comp := compFu(a[next : next+sz])
		if comp == 0 {
			return next, 0, iterCount
		} else if comp > 0 {
			low = next + sz
		} else {
			high = next - sz
		}
		if low == high {
			return high, 0, iterCount
		}

		next = is.Intrpolation1(value, next, sz)
		iterCount++

		if next+guard_off >= high {
			id, count := sequenceSearch(sz, high, a, true, compFu)
			return id, count, iterCount
		} else if next-guard_off <= low {
			id, count := sequenceSearch(sz, low, a, false, compFu)
			return id, count, iterCount
		}
	}
}

func TIPSearchByKey(a []byte, ts *TSSet, sz int, value int, compFu func(x []byte) int, guard_off int) (int, int, int) {
	if len(a)%sz != 0 {
		panic(fmt.Sprintf("x is not a multiple of a: %d %d", len(a), sz))
	}
	low, high := 0, len(a)-sz
	next1 := int(uint(low+high) >> 1)
	next1 -= next1 % sz
	next2 := ts.Intrpolation2(value, sz)
	next2 -= next2 % sz

	keyValue := int32(value)

	iterCount := 1

	for {
		if next2-next1 <= guard_off && next2-next1 >= -guard_off {
			id, count := linear_search(sz, next2, a, compFu)
			return id, count, iterCount
		}

		if next1 < low || next1 > high || next1 == next2 || next2 < low || next2 > high {
			return len(a), 0, iterCount
		}

		valueNext1 := int32(binary.BigEndian.Uint32(a[next1 : next1+sz]))
		valueNext2 := int32(binary.BigEndian.Uint32(a[next2 : next2+sz]))

		if valueNext1 != valueNext2 {
			if next1 < next2 {
				if valueNext1 > keyValue {
					return len(a), 0, iterCount
				}
				low = next1
			} else {
				if valueNext1 < keyValue {
					return len(a), 0, iterCount
				}
				high = next1
			}

			if next2+guard_off >= high {
				id, count := sequenceSearch(sz, high, a, true, compFu)
				return id, count, iterCount
			} else if next2-guard_off <= low {
				id, count := sequenceSearch(sz, low, a, false, compFu)
				return id, count, iterCount
			}
		}

		next1 = next2

		if low >= high || low < 0 || high > len(a) || next1 == low || next1 == high {
			return len(a), 0, iterCount
		}

		next2 = ts.Intrpolation1(value, low, next1, high, sz)

		if next2 < low || next2 > high {
			return len(a), 0, iterCount
		}
	}

}

// UpperBoundInt64 looks for the first element sorted in ascending order that is greater than or equal to the target value.
// return the index of that element if it exists, otherwise return -1.
func UpperBoundInt64Ascending(a []int64, x int64) int {
	if len(a) == 0 || a[len(a)-1] < x {
		return -1
	}
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

// LowerBoundInt64 looks for the first element sorted in ascending order that is smaller than the target value.
// return the index of that element if it exists, otherwise return -1.
func LowerBoundInt64Ascending(a []int64, x int64) int {
	if len(a) == 0 || a[0] >= x {
		return -1
	}
	return sort.Search(len(a), func(i int) bool { return a[i] >= x }) - 1
}

// LowerBoundInt64Descending looks for the first element sorted in descending order that is greater than or equal to the target value.
// return the index of that element if it exists, otherwise return -1.
func UpperBoundInt64Descending(a []int64, x int64) int {
	if len(a) == 0 || a[0] < x {
		return -1
	}
	idx := sort.Search(len(a), func(i int) bool { return a[i] < x })

	for idx >= 0 {
		if idx == len(a) {
			return idx - 1
		}
		if a[idx] >= x {
			return idx
		}
		idx--
	}
	return idx
}

// LowerBoundInt64Descending looks for the first element sorted in descending order that is smaller than the target value.
// return the index of that element if it exists, otherwise return -1.
func LowerBoundInt64Descending(a []int64, x int64) int {
	if len(a) == 0 || a[len(a)-1] >= x {
		return -1
	}
	return sort.Search(len(a), func(i int) bool { return a[i] < x })
}
