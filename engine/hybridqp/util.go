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
	"sort"
	"strings"
	"time"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

const (
	// SecToNs is the number of nanoseconds in a second.
	SecToNs = int64(time.Second)
)

// MustParseExpr parses an expression. Panic on error.
func MustParseExpr(s string) influxql.Expr {
	p := influxql.NewParser(strings.NewReader(s))
	defer p.Release()
	expr, err := p.ParseExpr()
	panicInPackage(err)
	return expr
}

func panicInPackage(err error) {
	if err != nil {
		panic(err)
	}
}

// Interval represents a repeating interval for a query.
type Interval struct {
	Duration time.Duration
	Offset   time.Duration
}

// IsZero returns true if the interval has no duration.
func (i Interval) IsZero() bool { return i.Duration == 0 }

func Abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func BinarySearch(target int, nums []int) bool {
	left := 0
	right := len(nums) - 1
	for left <= right {
		mid := left + (right-left)/2
		if target == nums[mid] {
			return true
		}
		if target > nums[mid] {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func SortS1ByS2(s1, s2 []int) {
	for i := range s2 {
		for j := range s1 {
			if s2[i] == s1[j] && j < len(s1)-1 {
				s1[j], s1[j+1] = s1[j+1], s1[j]
			}
		}
	}
}

func intersect(a, b []int) []int {
	c := make([]int, 0, len(a))
	sort.Ints(a)
	for _, v := range b {
		if BinarySearch(v, a) {
			c = append(c, v)
		}
	}
	return c
}

func Intersect(a, b []int) []int {
	if len(a) == 0 || len(b) == 0 {
		return []int{}
	}
	if len(a) < len(b) {
		return intersect(a, b)
	}
	return intersect(b, a)
}

func IsSubSlice(subS, s []int) bool {
	if len(subS) == 0 || len(s) == 0 || len(subS) > len(s) {
		return false
	}
	for i := range subS {
		if subS[i] != s[i] {
			return false
		}
	}
	return true
}

func CompareSlice(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Ints(a)
	sort.Ints(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func BinarySearchForNils(array []uint16, k uint16) int {
	low := 0
	high := len(array) - 1
	/*
		Array is an array that does not repeat in ascending order.
		It always meets the requirements of i<=array[i] and k<=array[k]. Therefore, high can be directly set to k.
	*/
	if high > int(k) {
		high = int(k)
	}
	/*
		The calculation num = array[k]-k indicates the maximum number of nil before array[k].
		In the worst case, any nil is before k. Therefore, low can be k-num.
	*/
	if int(k) < len(array) && low < int(k)-(int(array[k])-int(k)) {
		low = int(k) - (int(array[k]) - int(k))
	}

	for low+16 <= high {
		midIdx := int(uint32(low+high) >> 1)
		midVal := array[midIdx]
		if midVal < k {
			low = midIdx + 1
		} else if midVal > k {
			high = midIdx - 1
		} else {
			return midIdx
		}
	}

	for low <= high {
		val := array[low]
		if val == k {
			return low
		}
		if val > k {
			break
		}
		low++
	}
	return -(low + 1)
}

func LinearInterpolateInteger(currTime, prevTime, nextTime int64, prevValue, nextValue int64) int64 {
	return int64(float64(nextValue-prevValue)/float64(nextTime-prevTime)*float64(currTime-prevTime) + float64(prevValue))
}

func LinearInterpolateFloat(currTime, prevTime, nextTime int64, prevValue, nextValue float64) float64 {
	return (nextValue-prevValue)/float64(nextTime-prevTime)*float64(currTime-prevTime) + prevValue
}

func TransToFloat(v interface{}) (float64, bool) {
	switch v := v.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

func TransToInteger(v interface{}) (int64, bool) {
	switch v := v.(type) {
	case float64:
		return int64(v), true
	case int:
		return int64(v), true
	case uint64:
		return int64(v), true
	case int64:
		return v, true
	default:
		return int64(0), false
	}
}

func TransToString(v interface{}) (string, bool) {
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return s, true
}

func TransToBoolean(v interface{}) (bool, bool) {
	b, ok := v.(bool)
	if !ok {
		return false, false
	}
	return b, true
}

func EqualMap(tags1, tags2 map[string]string) bool {
	isSameTag := true
	if len(tags1) != len(tags2) {
		isSameTag = false
	} else {
		for btk, btv := range tags1 {
			if tv, ok := tags2[btk]; !ok {
				isSameTag = false
				break
			} else if tv != btv {
				isSameTag = false
				break
			}
		}
	}
	return isSameTag
}

func MaxInt64(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

func MaxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func AbsInt(x int) int {
	if x >= 0 {
		return x
	}
	return -x
}
