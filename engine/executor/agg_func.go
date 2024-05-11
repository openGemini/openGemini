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
	"sort"

	"github.com/openGemini/openGemini/lib/util"
)

func TopCmpByTimeReduce[T util.NumberOnly](a, b *PointItem[T]) bool {
	if a.time != b.time {
		return a.time < b.time
	}
	return a.value > b.value
}

func TopCmpByValueReduce[T util.NumberOnly](a, b *PointItem[T]) bool {
	if a.value != b.value {
		return a.value < b.value
	}
	return a.time > b.time
}

func BottomCmpByValueReduce[T util.NumberOnly](a, b *PointItem[T]) bool {
	if a.value != b.value {
		return a.value > b.value
	}
	return a.time > b.time
}

func BottomCmpByTimeReduce[T util.NumberOnly](a, b *PointItem[T]) bool {
	if a.time != b.time {
		return a.time < b.time
	}
	return a.value < b.value
}

func FrontDiffFunc[T util.NumberOnly](prev, curr T) T {
	return prev - curr
}

func BehindDiffFunc[T util.NumberOnly](prev, curr T) T {
	return curr - prev
}

func AbsoluteDiffFunc[T util.NumberOnly](prev, curr T) T {
	res := prev - curr
	if res >= 0 {
		return res
	}
	return -res
}

func NewMedianReduce[T util.NumberOnly](si *SliceItem[T]) (int, int64, float64, bool) {
	length := len(si.value)
	if length == 0 {
		return -1, 0, 0, true
	}
	if length == 1 {
		return -1, si.time[0], float64(si.value[0]), false
	}

	sort.Stable(si)

	if length%2 == 0 {
		lowvalue, highvalue := si.value[length/2-1], si.value[length/2]
		return -1, si.time[length/2-1], float64(lowvalue) + float64(highvalue-lowvalue)/2, false
	}
	return -1, si.time[length/2], float64(si.value[length/2]), false
}

func NewModeReduce[T util.ExceptBool](si *SliceItem[T]) (int, int64, float64, bool) {
	length := len(si.value)
	start := 0
	end := length - 1
	if length == 0 {
		return 0, 0, 0, true
	}

	sort.Stable(si)
	curri := start
	currFreq := 0
	currValue := si.value[start]
	modei := start
	modeFreq := 0
	for i := start; i <= end; i++ {
		if si.value[i] != currValue {
			currFreq = 1
			currValue = si.value[i]
			curri = i
			continue
		}
		currFreq++
		if modeFreq > currFreq || (modeFreq == currFreq && si.time[curri] > si.time[modei]) {
			continue
		}
		modeFreq = currFreq
		modei = curri
	}
	return modei, 0, 0, false
}

func CountReduce(c Chunk, ordinal, start, end int) (int, int64, bool) {
	var count int64
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		count = int64(end - start)
		return start, count, count == 0
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	count = int64(ve - vs)
	return start, count, count == 0
}

func AbsentReduce(c Chunk, ordinal, start, end int) (int, int64, bool) {
	var count int64
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		count = int64(end - start)
		if count > 0 {
			return start, 1, false
		}
		return start, 0, true
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	count = int64(ve - vs)
	if count > 0 {
		return start, 1, false
	}
	return start, 0, true
}
