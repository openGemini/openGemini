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
	"bytes"
	"math"
	"sort"

	"github.com/openGemini/openGemini/engine/hybridqp"
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

func NewBooleanModeReduce(BooleanSliceItem *BooleanSliceItem) (int, int64, float64, bool) {
	length := len(BooleanSliceItem.value)
	if length == 0 {
		return 0, 0, 0, true
	}
	if length == 1 {
		return 0, 0, 0, false
	}

	truei := -1
	TrueFreq := 0
	falsei := -1
	FalseFreq := 0
	for i := 0; i < length; i++ {
		if BooleanSliceItem.value[i] {
			if truei == -1 {
				truei = i
			}
			TrueFreq++
		} else {
			if falsei == -1 {
				falsei = i
			}
			FalseFreq++
		}
	}
	if TrueFreq >= FalseFreq {
		return truei, 0, 0, false
	}
	return falsei, 0, 0, false
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

func FloatSumReduce(c Chunk, ordinal, start, end int) (int, float64, bool) {
	var sum float64
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		for i := start; i < end; i++ {
			sum += c.Column(ordinal).FloatValue(i)
		}
		return start, sum, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	for i := vs; i < ve; i++ {
		sum += c.Column(ordinal).FloatValue(i)
	}
	return start, sum, false
}

func IntegerSumReduce(c Chunk, ordinal, start, end int) (int, int64, bool) {
	var sum int64
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		for i := start; i < end; i++ {
			sum += c.Column(ordinal).IntegerValue(i)
		}
		return start, sum, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	for i := vs; i < ve; i++ {
		sum += c.Column(ordinal).IntegerValue(i)
	}
	return start, sum, false
}

func FloatMinReduce(c Chunk, ordinal, start, end int) (int, float64, bool) {
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		minValue, minIndex := c.Column(ordinal).FloatValue(start), start
		for i := start; i < end; i++ {
			v := c.Column(ordinal).FloatValue(i)
			if v < minValue || (v == minValue && c.TimeByIndex(i) < c.TimeByIndex(minIndex)) {
				minIndex = i
				minValue = v
			}
		}
		return minIndex, minValue, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	minValue, minIndex := c.Column(ordinal).FloatValue(vs), c.Column(ordinal).GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := c.Column(ordinal).FloatValue(i), c.Column(ordinal).GetTimeIndex(i)
		if v < minValue || (v == minValue && c.TimeByIndex(index) < c.TimeByIndex(minIndex)) {
			minIndex = index
			minValue = v
		}
	}
	return minIndex, minValue, false
}

func IntegerMinReduce(c Chunk, ordinal, start, end int) (int, int64, bool) {
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		minValue, minIndex := c.Column(ordinal).IntegerValue(start), start
		for i := start; i < end; i++ {
			v := c.Column(ordinal).IntegerValue(i)
			if v < minValue || (v == minValue && c.TimeByIndex(i) < c.TimeByIndex(minIndex)) {
				minIndex = i
				minValue = v
			}
		}
		return minIndex, minValue, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	minValue, minIndex := c.Column(ordinal).IntegerValue(vs), c.Column(ordinal).GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := c.Column(ordinal).IntegerValue(i), c.Column(ordinal).GetTimeIndex(i)
		if v < minValue || (v == minValue && c.TimeByIndex(index) < c.TimeByIndex(minIndex)) {
			minIndex = index
			minValue = v
		}
	}
	return minIndex, minValue, false
}

func BooleanMinReduce(c Chunk, ordinal, start, end int) (int, bool, bool) {
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		minValue, minIndex := c.Column(ordinal).BooleanValue(start), start
		for i := start; i < end; i++ {
			v := c.Column(ordinal).BooleanValue(i)
			if (v != minValue && !v) || (v == minValue && c.TimeByIndex(i) < c.TimeByIndex(minIndex)) {
				minIndex = i
				minValue = v
			}
		}
		return minIndex, minValue, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, false, true
	}
	minValue, minIndex := c.Column(ordinal).BooleanValue(vs), c.Column(ordinal).GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := c.Column(ordinal).BooleanValue(i), c.Column(ordinal).GetTimeIndex(i)
		if (v != minValue && !v) || (v == minValue && c.TimeByIndex(index) < c.TimeByIndex(minIndex)) {
			minIndex = index
			minValue = v
		}
	}
	return minIndex, minValue, false
}

func CountMerge[T util.NumberOnly](prevPoint, currPoint *Point[T]) {
	if currPoint.isNil {
		return
	}
	if prevPoint.isNil {
		prevPoint.Assign(currPoint)
		prevPoint.isNil = false
		return
	}
	prevPoint.value += currPoint.value
}

func SumMerge[T util.NumberOnly](prevPoint, currPoint *Point[T]) {
	if currPoint.isNil {
		return
	}
	if prevPoint.isNil {
		prevPoint.Assign(currPoint)
		prevPoint.isNil = false
		return
	}
	prevPoint.value += currPoint.value
}

func MinMerge[T util.NumberOnly](prevPoint, currPoint *Point[T]) {
	if currPoint.isNil {
		return
	}
	if prevPoint.isNil || (currPoint.value < prevPoint.value) ||
		(currPoint.value == prevPoint.value && currPoint.time < prevPoint.time) {
		prevPoint.Assign(currPoint)
		prevPoint.isNil = false
	}
}

func BooleanMinMerge(prevPoint, currPoint *Point[bool]) {
	if currPoint.isNil {
		return
	}
	if prevPoint.isNil || (currPoint.value != prevPoint.value && !currPoint.value) ||
		(currPoint.value == prevPoint.value && currPoint.time < prevPoint.time) {
		prevPoint.Assign(currPoint)
		prevPoint.isNil = false
	}
}

func MaxMerge[T util.NumberOnly](prevPoint, currPoint *Point[T]) {
	if currPoint.isNil {
		return
	}
	if prevPoint.isNil || (currPoint.value > prevPoint.value) ||
		(currPoint.value == prevPoint.value && currPoint.time < prevPoint.time) {
		prevPoint.Assign(currPoint)
		prevPoint.isNil = false
	}
}

func BooleanMaxMerge(prevPoint, currPoint *Point[bool]) {
	if currPoint.isNil {
		return
	}
	if prevPoint.isNil || (currPoint.value != prevPoint.value && currPoint.value) ||
		(currPoint.value == prevPoint.value && currPoint.time < prevPoint.time) {
		prevPoint.Assign(currPoint)
		prevPoint.isNil = false
	}
}

func FirstMerge[T util.NumberOnly](prevPoint, currPoint *Point[T]) {
	if prevPoint.isNil || (currPoint.time < prevPoint.time) ||
		(currPoint.time == prevPoint.time && currPoint.value > prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func StringFirstMerge(prevPoint, currPoint *StringPoint) {
	if prevPoint.isNil || (currPoint.time < prevPoint.time) ||
		(currPoint.time == prevPoint.time && bytes.Compare(currPoint.value, prevPoint.value) > 0) {
		prevPoint.Assign(currPoint)
	}
}

func BooleanFirstMerge(prevPoint, currPoint *Point[bool]) {
	if prevPoint.isNil || (currPoint.time < prevPoint.time) ||
		(currPoint.time == prevPoint.time && !currPoint.value && prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func LastMerge[T util.NumberOnly](prevPoint, currPoint *Point[T]) {
	if prevPoint.isNil || (currPoint.time > prevPoint.time) ||
		(currPoint.time == prevPoint.time && currPoint.value > prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func FirstTimeColMerge[T util.NumberOnly](prevPoint, currPoint *Point[T]) {
	if prevPoint.isNil || (currPoint.time < prevPoint.time) ||
		(currPoint.time == prevPoint.time && currPoint.value > prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func LastTimeColMerge[T util.NumberOnly](prevPoint, currPoint *Point[T]) {
	if prevPoint.isNil || (currPoint.time > prevPoint.time) ||
		(currPoint.time == prevPoint.time && currPoint.value > prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func AbsentMerge[T util.NumberOnly](prevPoint, currPoint *Point[T]) {
	if prevPoint.isNil && currPoint.isNil {
		prevPoint.isNil = true
		prevPoint.value = 0
		return
	}
	prevPoint.isNil = false
	prevPoint.value = 1
}

func IrateUpdate[T util.NumberOnly](prevPoints, currPoints [2]*Point[T]) {
	samePrevPoint := (!prevPoints[0].isNil && !prevPoints[1].isNil) &&
		(prevPoints[0].time == prevPoints[1].time && prevPoints[0].value == prevPoints[1].value)
	for i := range currPoints {
		if currPoints[i].isNil || currPoints[i].time < prevPoints[0].time ||
			(currPoints[i].time == prevPoints[0].time && currPoints[i].value < prevPoints[0].value) {
			if samePrevPoint {
				prevPoints[0].time, prevPoints[0].value = currPoints[i].time, currPoints[i].value
			}
			continue
		}
		if (i > 0 && !currPoints[i-1].isNil) &&
			(currPoints[i].time == currPoints[i-1].time && currPoints[i].value == currPoints[i-1].value) {
			continue
		}
		if currPoints[i].time > prevPoints[0].time ||
			(currPoints[i].time == prevPoints[0].time && currPoints[i].value > prevPoints[0].value) {
			if currPoints[i].time > prevPoints[1].time ||
				(currPoints[i].time == prevPoints[1].time && currPoints[i].value > prevPoints[1].value) {
				prevPoints[0].time, prevPoints[0].value = prevPoints[1].time, prevPoints[1].value
				prevPoints[1].time, prevPoints[1].value = currPoints[i].time, currPoints[i].value
			} else {
				prevPoints[0].time, prevPoints[0].value = currPoints[i].time, currPoints[i].value
			}
		}
	}
}

func RateUpdate[T util.NumberOnly](prevPoints, currPoints [2]*Point[T]) {
	for i := range currPoints {
		if currPoints[i].isNil {
			continue
		}
		if currPoints[i].time < prevPoints[0].time ||
			(currPoints[i].time == prevPoints[0].time && currPoints[i].value > prevPoints[0].value) {
			prevPoints[0].time = currPoints[i].time
			prevPoints[0].value = currPoints[i].value
		}
		if currPoints[i].time > prevPoints[1].time ||
			(currPoints[i].time == prevPoints[1].time && currPoints[i].value > prevPoints[1].value) {
			prevPoints[1].time = currPoints[i].time
			prevPoints[1].value = currPoints[i].value
		}
	}
}

func RateFinalReduce[T util.NumberOnly](firstTime int64, lastTime int64, firstValue T, lastValue T,
	interval *hybridqp.Interval) (float64, bool) {
	if lastTime == firstTime || interval.Duration == 0 {
		return 0, true
	}
	rate := float64(lastValue-firstValue) / (float64(lastTime-firstTime) / float64(interval.Duration))
	return rate, false
}

func RateMerge[T util.NumberOnly](prevPoints [2]*Point[T], interval *hybridqp.Interval) (float64, bool) {
	return RateFinalReduce(prevPoints[0].time, prevPoints[1].time,
		prevPoints[0].value, prevPoints[1].value, interval)
}

func IrateFinalReduce[T util.NumberOnly](ft int64, st int64, fv T, sv T,
	interval *hybridqp.Interval) (float64, bool) {
	if st == ft || interval.Duration == 0 {
		return 0, true
	}
	rate := float64(sv-fv) / (float64(st-ft) / float64(interval.Duration))
	return rate, false
}

func IrateMerge[T util.NumberOnly](prevPoints [2]*Point[T], interval *hybridqp.Interval) (float64, bool) {
	return RateFinalReduce(prevPoints[0].time, prevPoints[1].time,
		prevPoints[0].value, prevPoints[1].value, interval)
}

func SlidingWindowMergeFunc[T util.ExceptString](prevWindow, currWindow *SlidingWindow[T], fpm PointMerge[T]) {
	for i := 0; i < prevWindow.Len(); i++ {
		fpm(prevWindow.points[i], currWindow.points[i])
	}
}

func FloatMaxReduce(c Chunk, ordinal, start, end int) (int, float64, bool) {
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		maxValue, maxIndex := c.Column(ordinal).FloatValue(start), start
		for i := start; i < end; i++ {
			v := c.Column(ordinal).FloatValue(i)
			if v > maxValue || (v == maxValue && c.TimeByIndex(i) < c.TimeByIndex(maxIndex)) {
				maxIndex = i
				maxValue = v
			}
		}
		return maxIndex, maxValue, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	maxValue, maxIndex := c.Column(ordinal).FloatValue(vs), c.Column(ordinal).GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := c.Column(ordinal).FloatValue(i), c.Column(ordinal).GetTimeIndex(i)
		if v > maxValue || (v == maxValue && c.TimeByIndex(index) < c.TimeByIndex(maxIndex)) {
			maxIndex = index
			maxValue = v
		}
	}
	return maxIndex, maxValue, false
}

func IntegerMaxReduce(c Chunk, ordinal, start, end int) (int, int64, bool) {
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		maxValue, maxIndex := c.Column(ordinal).IntegerValue(start), start
		for i := start; i < end; i++ {
			v := c.Column(ordinal).IntegerValue(i)
			if v > maxValue || (v == maxValue && c.TimeByIndex(i) < c.TimeByIndex(maxIndex)) {
				maxIndex = i
				maxValue = v
			}
		}
		return maxIndex, maxValue, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	maxValue, maxIndex := c.Column(ordinal).IntegerValue(vs), c.Column(ordinal).GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := c.Column(ordinal).IntegerValue(i), c.Column(ordinal).GetTimeIndex(i)
		if v > maxValue || (v == maxValue && c.TimeByIndex(index) < c.TimeByIndex(maxIndex)) {
			maxIndex = index
			maxValue = v
		}
	}
	return maxIndex, maxValue, false
}

func BooleanMaxReduce(c Chunk, ordinal, start, end int) (int, bool, bool) {
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		maxValue, maxIndex := c.Column(ordinal).BooleanValue(start), start
		for i := start; i < end; i++ {
			v := c.Column(ordinal).BooleanValue(i)
			if (v != maxValue && v) || (v == maxValue && c.TimeByIndex(i) < c.TimeByIndex(maxIndex)) {
				maxIndex = i
				maxValue = v
			}
		}
		return maxIndex, maxValue, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, false, true
	}
	maxValue, maxIndex := c.Column(ordinal).BooleanValue(vs), c.Column(ordinal).GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := c.Column(ordinal).BooleanValue(i), c.Column(ordinal).GetTimeIndex(i)
		if (v != maxValue && v) || (v == maxValue && c.TimeByIndex(index) < c.TimeByIndex(maxIndex)) {
			maxIndex = index
			maxValue = v
		}
	}
	return maxIndex, maxValue, false
}

type SliceReduce[T util.NumberOnly] func(floatItem *SliceItem[T]) (index int, time int64, value float64, isNil bool)

func NewPercentileReduce[T util.NumberOnly](percentile float64) SliceReduce[T] {
	return func(floatSliceItem *SliceItem[T]) (int, int64, float64, bool) {
		length := len(floatSliceItem.value)
		if length == 0 {
			return 0, int64(0), float64(0), true
		}

		sort.Sort(floatSliceItem)

		i := int(math.Floor(float64(length)*percentile/100.0+0.5)) - 1
		if i < 0 {
			i = 0
		} else if i >= length {
			i = length - 1
		}
		return i, int64(0), float64(0), false
	}
}

func NewStdDevReduce[T util.NumberOnly]() SliceReduce[T] {
	return func(floatSliceItem *SliceItem[T]) (int, int64, float64, bool) {
		length := len(floatSliceItem.value)
		if length == 1 {
			return -1, int64(0), float64(0), false
		} else if length == 0 {
			return -1, int64(0), float64(0), true
		} else {
			sum := T(0)
			sum2 := T(0)
			count := float64(length)
			stddev := float64(0)
			for _, v := range floatSliceItem.value {
				sum += v
				sum2 += v * v
			}
			stddev = math.Sqrt((float64(sum2)/count - math.Pow(float64(sum)/count, 2)) * count / (count - 1))
			return -1, int64(0), stddev, false
		}
	}
}

func FloatFirstReduce(c Chunk, ordinal, start, end int) (int, float64, bool) {
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		firstValue, firstIndex := c.Column(ordinal).FloatValue(start), start
		for i := start; i < end; i++ {
			v := c.Column(ordinal).FloatValue(i)
			if c.TimeByIndex(i) < c.TimeByIndex(firstIndex) ||
				(c.TimeByIndex(i) == c.TimeByIndex(firstIndex) && v > firstValue) {
				firstIndex = i
				firstValue = v
			}
		}
		return firstIndex, firstValue, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	firstValue, firstIndex := c.Column(ordinal).FloatValue(vs), int(c.Column(ordinal).GetTimeIndex(vs))
	for i := vs; i < ve; i++ {
		v, index := c.Column(ordinal).FloatValue(i), int(c.Column(ordinal).GetTimeIndex(i))
		if c.TimeByIndex(index) < c.TimeByIndex(firstIndex) ||
			(c.TimeByIndex(index) == c.TimeByIndex(firstIndex) && v > firstValue) {
			firstIndex = index
			firstValue = v
		}
	}
	return firstIndex, firstValue, false
}

func IntegerFirstReduce(c Chunk, ordinal, start, end int) (int, int64, bool) {
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		firstValue, firstIndex := c.Column(ordinal).IntegerValue(start), start
		for i := start; i < end; i++ {
			v := c.Column(ordinal).IntegerValue(i)
			if c.TimeByIndex(i) < c.TimeByIndex(firstIndex) ||
				(c.TimeByIndex(i) == c.TimeByIndex(firstIndex) && v > firstValue) {
				firstIndex = i
				firstValue = v
			}
		}
		return firstIndex, firstValue, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	firstValue, firstIndex := c.Column(ordinal).IntegerValue(vs), int(c.Column(ordinal).GetTimeIndex(vs))
	for i := vs; i < ve; i++ {
		v, index := c.Column(ordinal).IntegerValue(i), int(c.Column(ordinal).GetTimeIndex(i))
		if c.TimeByIndex(index) < c.TimeByIndex(firstIndex) ||
			(c.TimeByIndex(index) == c.TimeByIndex(firstIndex) && v > firstValue) {
			firstIndex = index
			firstValue = v
		}
	}
	return firstIndex, firstValue, false
}

func StringFirstReduce(c Chunk, ordinal, start, end int) (int, string, bool) {
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		firstValue, firstIndex := c.Column(ordinal).StringValue(start), start
		for i := start; i < end; i++ {
			v := c.Column(ordinal).StringValue(i)
			if c.TimeByIndex(i) < c.TimeByIndex(firstIndex) ||
				(c.TimeByIndex(i) == c.TimeByIndex(firstIndex) && v > firstValue) {
				firstIndex = i
				firstValue = v
			}
		}
		return firstIndex, firstValue, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, "", true
	}
	firstValue, firstIndex := c.Column(ordinal).StringValue(vs), int(c.Column(ordinal).GetTimeIndex(vs))
	for i := vs; i < ve; i++ {
		v, index := c.Column(ordinal).StringValue(i), int(c.Column(ordinal).GetTimeIndex(i))
		if c.TimeByIndex(index) < c.TimeByIndex(firstIndex) ||
			(c.TimeByIndex(index) == c.TimeByIndex(firstIndex) && v > firstValue) {
			firstIndex = index
			firstValue = v
		}
	}
	return firstIndex, firstValue, false
}

func BooleanFirstReduce(c Chunk, ordinal, start, end int) (int, bool, bool) {
	if c.Column(ordinal).NilCount() == 0 {
		// fast path
		firstValue, firstIndex := c.Column(ordinal).BooleanValue(start), start
		for i := start; i < end; i++ {
			v := c.Column(ordinal).BooleanValue(i)
			if c.TimeByIndex(i) < c.TimeByIndex(firstIndex) ||
				(c.TimeByIndex(i) == c.TimeByIndex(firstIndex) && !v && firstValue) {
				firstIndex = i
				firstValue = v
			}
		}
		return firstIndex, firstValue, false
	}

	// slow path
	vs, ve := c.Column(ordinal).GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, false, true
	}
	firstValue, firstIndex := c.Column(ordinal).BooleanValue(vs), int(c.Column(ordinal).GetTimeIndex(vs))
	for i := vs; i < ve; i++ {
		v, index := c.Column(ordinal).BooleanValue(i), int(c.Column(ordinal).GetTimeIndex(i))
		if c.TimeByIndex(index) < c.TimeByIndex(firstIndex) ||
			(c.TimeByIndex(index) == c.TimeByIndex(firstIndex) && !v && firstValue) {
			firstIndex = index
			firstValue = v
		}
	}
	return firstIndex, firstValue, false
}
