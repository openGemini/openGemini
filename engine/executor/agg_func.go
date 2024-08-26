// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

func CountReduce(c Chunk, values []int64, ordinal, start, end int) (int, int64, bool) {
	var count int64
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		// fast path
		count = int64(end - start)
		return start, count, count == 0
	}

	// slow path
	vs, ve := column.GetRangeValueIndexV2(start, end)
	count = int64(ve - vs)
	return start, count, count == 0
}

func AbsentReduce(c Chunk, values []int64, ordinal, start, end int) (int, int64, bool) {
	var count int64
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		// fast path
		count = int64(end - start)
		if count > 0 {
			return start, 1, false
		}
		return start, 0, true
	}

	// slow path
	vs, ve := column.GetRangeValueIndexV2(start, end)
	count = int64(ve - vs)
	if count > 0 {
		return start, 1, false
	}
	return start, 0, true
}

func SumReduce[T util.NumberOnly](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	var sum T
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		// fast path
		for i := start; i < end; i++ {
			sum += values[i]
		}
		return start, sum, false
	}

	// slow path
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	for i := vs; i < ve; i++ {
		sum += values[i]
	}
	return start, sum, false
}

func MinReduce[T util.NumberOnly](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	column := c.Column(ordinal)
	times := c.Time()
	if column.NilCount() == 0 {
		// fast path
		minValue, minIndex := values[start], start
		for i := start; i < end; i++ {
			v := values[i]
			if v < minValue || (v == minValue && times[i] < times[minIndex]) {
				minIndex = i
				minValue = v
			}
		}
		return minIndex, minValue, false
	}

	// slow path
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	minValue, minIndex := values[vs], column.GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := values[i], column.GetTimeIndex(i)
		if v < minValue || (v == minValue && times[index] < times[minIndex]) {
			minIndex = index
			minValue = v
		}
	}
	return minIndex, minValue, false
}

func BooleanMinReduce(c Chunk, values []bool, ordinal, start, end int) (int, bool, bool) {
	column := c.Column(ordinal)
	times := c.Time()
	if column.NilCount() == 0 {
		// fast path
		minValue, minIndex := values[start], start
		for i := start; i < end; i++ {
			v := values[i]
			if (v != minValue && !v) || (v == minValue && times[i] < times[minIndex]) {
				minIndex = i
				minValue = v
			}
		}
		return minIndex, minValue, false
	}

	// slow path
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, false, true
	}
	minValue, minIndex := values[vs], column.GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := values[i], column.GetTimeIndex(i)
		if (v != minValue && !v) || (v == minValue && times[index] < times[minIndex]) {
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

func StringLastMerge(prevPoint, currPoint *StringPoint) {
	if prevPoint.isNil || (currPoint.time > prevPoint.time) ||
		(currPoint.time == prevPoint.time && bytes.Compare(currPoint.value, prevPoint.value) > 0) {
		prevPoint.Assign(currPoint)
	}
}

func BooleanLastMerge(prevPoint, currPoint *Point[bool]) {
	if prevPoint.isNil || (currPoint.time > prevPoint.time) ||
		(currPoint.time == prevPoint.time && currPoint.value && !prevPoint.value) {
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

func MaxReduce[T util.NumberOnly](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	column := c.Column(ordinal)
	times := c.Time()
	if column.NilCount() == 0 {
		// fast path
		maxValue, maxIndex := values[start], start
		for i := start; i < end; i++ {
			v := values[i]
			if v > maxValue || (v == maxValue && times[i] < times[maxIndex]) {
				maxIndex = i
				maxValue = v
			}
		}
		return maxIndex, maxValue, false
	}

	// slow path
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	maxValue, maxIndex := values[vs], column.GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := values[i], column.GetTimeIndex(i)
		if v > maxValue || (v == maxValue && times[index] < times[maxIndex]) {
			maxIndex = index
			maxValue = v
		}
	}
	return maxIndex, maxValue, false
}

func BooleanMaxReduce(c Chunk, values []bool, ordinal, start, end int) (int, bool, bool) {
	column := c.Column(ordinal)
	times := c.Time()
	if column.NilCount() == 0 {
		// fast path
		maxValue, maxIndex := values[start], start
		for i := start; i < end; i++ {
			v := values[i]
			if (v != maxValue && v) || (v == maxValue && times[i] < times[maxIndex]) {
				maxIndex = i
				maxValue = v
			}
		}
		return maxIndex, maxValue, false
	}

	// slow path
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, false, true
	}
	maxValue, maxIndex := values[vs], column.GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := values[i], column.GetTimeIndex(i)
		if (v != maxValue && v) || (v == maxValue && times[index] < times[maxIndex]) {
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

func NewStdReduce(isStddev bool) SliceReduce[float64] {
	return func(floatSliceItem *SliceItem[float64]) (int, int64, float64, bool) {
		length := len(floatSliceItem.value)
		if length == 1 {
			return -1, int64(0), float64(0), false
		}
		if length == 0 {
			return -1, int64(0), float64(0), true
		}
		var count, floatValue float64
		floatMean := floatSliceItem.value[0]

		for _, v := range floatSliceItem.value {
			count++
			delta := v - floatMean
			floatMean += delta / count
			floatValue += delta * (v - floatMean)
		}

		std := floatValue / count
		if isStddev {
			std = math.Sqrt(std)
		}
		return -1, floatSliceItem.time[0], std, false
	}
}

func BooleanFirstReduce(c Chunk, values []bool, ordinal, start, end int) (int, bool, bool) {
	column := c.Column(ordinal)
	times := c.Time()
	if column.NilCount() == 0 {
		// fast path
		firstValue, firstIndex := values[start], start
		for i := start; i < end; i++ {
			v := values[i]
			if times[i] < times[firstIndex] ||
				(times[i] == times[firstIndex] && !v && firstValue) {
				firstIndex = i
				firstValue = v
			}
		}
		return firstIndex, firstValue, false
	}

	// slow path
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, false, true
	}
	firstValue, firstIndex := values[vs], column.GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := values[i], column.GetTimeIndex(i)
		if times[index] < times[firstIndex] ||
			(times[index] == times[firstIndex] && !v && firstValue) {
			firstIndex = index
			firstValue = v
		}
	}
	return firstIndex, firstValue, false
}

func FirstReduce[T util.ExceptBool](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	column := c.Column(ordinal)
	times := c.Time()
	if column.NilCount() == 0 {
		// fast path
		firstValue, firstIndex := values[start], start
		for i := start; i < end; i++ {
			v := values[i]
			if times[i] < times[firstIndex] ||
				(times[i] == times[firstIndex] && v > firstValue) {
				firstIndex = i
				firstValue = v
			}
		}
		return firstIndex, firstValue, false
	}

	// slow path
	var defaultValue T
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, defaultValue, true
	}
	firstValue, firstIndex := values[vs], column.GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := values[i], column.GetTimeIndex(i)
		if times[index] < times[firstIndex] ||
			(times[index] == times[firstIndex] && v > firstValue) {
			firstIndex = index
			firstValue = v
		}
	}
	return firstIndex, firstValue, false
}

func IrateFastReduce[T util.NumberOnly](c Chunk, values []T, ordinal, start, end int) (int, int, T, T, bool) {
	if end-start == 0 {
		return 0, 0, 0, 0, true
	}
	if end-start == 1 {
		v := values[start]
		return start, start, v, v, false
	}
	var (
		fi, si int
		fv, sv T
	)
	if c.TimeByIndex(start) < c.TimeByIndex(start+1) || (c.TimeByIndex(start) == c.TimeByIndex(start+1) && fv > sv) {
		fi, si, fv, sv = start, start+1, values[start], values[start+1]
	} else {
		fi, si, fv, sv = start+1, start, values[start+1], values[start]
	}
	if end-start == 2 {
		return fi, si, fv, sv, false
	}
	for i := start + 2; i < end; i++ {
		v := values[i]
		if c.TimeByIndex(i) < c.TimeByIndex(fi) ||
			(c.TimeByIndex(i) == c.TimeByIndex(fi) && v < fv) {
			continue
		}
		if c.TimeByIndex(i) > c.TimeByIndex(fi) ||
			(c.TimeByIndex(i) == c.TimeByIndex(fi) && v > fv) {
			if c.TimeByIndex(i) > c.TimeByIndex(si) ||
				(c.TimeByIndex(i) == c.TimeByIndex(si) && v > sv) {
				fi, fv = si, sv
				si, sv = i, v
			} else {
				fi, fv = i, v
			}
		}
	}
	return fi, si, fv, sv, false
}

func IrateSlowReduce[T util.NumberOnly](c Chunk, values []T, ordinal, start, end int) (int, int, T, T, bool) {
	column := c.Column(ordinal)
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return 0, 0, 0, 0, true
	}
	if ve-vs == 1 {
		v := values[vs]
		return start, start, v, v, false
	}
	var (
		fi, si int
		fv, sv T
	)
	fv, sv = values[vs], values[vs+1]
	fi, si = column.GetTimeIndex(vs), column.GetTimeIndex(vs+1)
	if !(c.TimeByIndex(fi) < c.TimeByIndex(si) || (c.TimeByIndex(fi) == c.TimeByIndex(si) && fv > sv)) {
		fi, si, fv, sv = si, fi, sv, fv
	}
	if ve-vs == 2 {
		return fi, si, fv, sv, false
	}
	for i := vs + 2; i < ve; i++ {
		v, index := values[i], column.GetTimeIndex(i)
		if c.TimeByIndex(index) < c.TimeByIndex(fi) ||
			(c.TimeByIndex(index) == c.TimeByIndex(fi) && v > fv) {
			continue
		}
		if c.TimeByIndex(index) > c.TimeByIndex(fi) ||
			(c.TimeByIndex(index) == c.TimeByIndex(fi) && v > fv) {
			if c.TimeByIndex(index) > c.TimeByIndex(si) ||
				(c.TimeByIndex(index) == c.TimeByIndex(si) && v > sv) {
				fi, fv = si, sv
				si, sv = index, v
			} else {
				fi, fv = index, v
			}
		}
	}
	return fi, si, fv, sv, false
}

func IrateMiddleReduce[T util.NumberOnly](c Chunk, values []T, ordinal, start, end int) (int, int, T, T, bool) {
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		// fast path
		return IrateFastReduce(c, values, ordinal, start, end)
	}

	// slow path
	return IrateSlowReduce(c, values, ordinal, start, end)
}

func RateFastReduce[T util.NumberOnly](c Chunk, values []T, ordinal, start, end int) (int, int, T, T, bool) {
	if end-start == 0 {
		return 0, 0, 0, 0, true
	}
	firstValue, firstIndex := values[start], start
	lastValue, lastIndex := firstValue, firstIndex
	times := c.Time()
	for i := start; i < end; i++ {
		v := values[i]
		if times[i] < times[firstIndex] ||
			(times[i] == times[firstIndex] && v > firstValue) {
			firstIndex = i
			firstValue = v
		}
		if times[i] > times[lastIndex] ||
			(times[i] == times[lastIndex] && v > lastValue) {
			lastIndex = i
			lastValue = v
		}
	}
	return firstIndex, lastIndex, firstValue, lastValue, false
}

func RateLowReduce[T util.NumberOnly](c Chunk, values []T, ordinal, start, end int) (int, int, T, T, bool) {
	column := c.Column(ordinal)
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return 0, 0, 0, 0, true
	}
	firstValue, firstIndex := values[vs], column.GetTimeIndex(vs)
	lastValue, lastIndex := firstValue, firstIndex
	times := c.Time()
	for i := vs; i < ve; i++ {
		v, index := values[i], column.GetTimeIndex(i)
		if times[index] < times[firstIndex] ||
			(times[index] == times[firstIndex] && v > firstValue) {
			firstIndex = index
			firstValue = v
		}
		if times[index] > times[lastIndex] ||
			(times[index] == times[lastIndex] && v > lastValue) {
			lastIndex = index
			lastValue = v
		}
	}
	return firstIndex, lastIndex, firstValue, lastValue, false
}

func RateMiddleReduce[T util.NumberOnly](c Chunk, values []T, ordinal, start, end int) (int, int, T, T, bool) {
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		// fast path
		return RateFastReduce(c, values, ordinal, start, end)
	}

	// slow path
	return RateLowReduce(c, values, ordinal, start, end)
}

func LastReduce[T util.ExceptBool](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	column := c.Column(ordinal)
	times := c.Time()
	if column.NilCount() == 0 {
		// fast path
		lastValue, lastIndex := values[start], start
		for i := start; i < end; i++ {
			v := values[i]
			if times[i] > times[lastIndex] ||
				(times[i] == times[lastIndex] && v > lastValue) {
				lastIndex = i
				lastValue = v
			}
		}
		return lastIndex, lastValue, false
	}

	// slow path
	var defaultValue T
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, defaultValue, true
	}
	lastValue, lastIndex := values[vs], column.GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := values[i], column.GetTimeIndex(i)
		if times[index] > times[lastIndex] ||
			(times[index] == times[lastIndex] && v > lastValue) {
			lastIndex = index
			lastValue = v
		}
	}
	return lastIndex, lastValue, false
}

func BooleanLastReduce(c Chunk, values []bool, ordinal, start, end int) (int, bool, bool) {
	column := c.Column(ordinal)
	times := c.Time()
	if column.NilCount() == 0 {
		// fast path
		lastValue, lastIndex := values[start], start
		for i := start; i < end; i++ {
			v := values[i]
			if times[i] > times[lastIndex] ||
				(times[i] == times[lastIndex] && v && !lastValue) {
				lastIndex = i
				lastValue = v
			}
		}
		return lastIndex, lastValue, false
	}

	// slow path
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, false, true
	}
	lastValue, lastIndex := values[vs], column.GetTimeIndex(vs)
	for i := vs; i < ve; i++ {
		v, index := values[i], column.GetTimeIndex(i)
		if times[index] > times[lastIndex] ||
			(times[index] == times[lastIndex] && v && !lastValue) {
			lastIndex = index
			lastValue = v
		}
	}
	return lastIndex, lastValue, false
}

func BooleanLastTimeColFastReduce(c Chunk, values []bool, ordinal, start, end int) (int, bool, bool) {
	column := c.Column(ordinal)
	// fast path
	lastValue, lastIndex := values[start], start
	// column time is not initialized in the subquery
	if len(column.ColumnTimes()) == 0 {
		times := c.Time()
		for i := start; i < end; i++ {
			v := values[i]
			if times[i] > times[lastIndex] ||
				(times[i] == times[lastIndex] && v && !lastValue) {
				lastIndex = i
				lastValue = v
			}
		}
		return lastIndex, lastValue, false
	}
	// column time is initialized
	times := column.ColumnTimes()
	for i := start; i < end; i++ {
		v := values[i]
		if times[i] > times[lastIndex] ||
			(times[i] == times[lastIndex] && v && !lastValue) {
			lastIndex = i
			lastValue = v
		}
	}
	return lastIndex, lastValue, false
}

func BooleanLastTimeColSlowReduce(c Chunk, values []bool, ordinal, start, end int) (int, bool, bool) {
	// slow path
	column := c.Column(ordinal)
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, false, true
	}
	// column time is not initialized in the subquery
	if len(column.ColumnTimes()) == 0 {
		lastValue, lastIndex := values[vs], column.GetTimeIndex(vs)
		times := c.Time()
		for i := start; i < end; i++ {
			if column.IsNilV2(i) {
				continue
			}
			v := values[column.GetValueIndexV2(i)]
			if times[i] > times[lastIndex] ||
				(times[i] == times[lastIndex] && v && !lastValue) {
				lastIndex = i
				lastValue = v
			}
		}
		return lastIndex, lastValue, false
	}
	// column time is initialized
	lastValue, lastIndex := values[vs], vs
	times := column.ColumnTimes()
	for i := vs; i < ve; i++ {
		v := values[i]
		if times[i] > times[lastIndex] ||
			(times[i] == times[lastIndex] && v && !lastValue) {
			lastIndex = i
			lastValue = v
		}
	}
	return lastIndex, lastValue, false
}

func BooleanLastTimeColReduce(c Chunk, values []bool, ordinal, start, end int) (int, bool, bool) {
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		return BooleanLastTimeColFastReduce(c, values, ordinal, start, end)
	}
	return BooleanLastTimeColSlowReduce(c, values, ordinal, start, end)
}

func LastTimeColFastReduce[T util.ExceptBool](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	// fast path
	column := c.Column(ordinal)
	lastValue, lastIndex := values[start], start
	// column time is not initialized in the subquery
	if len(column.ColumnTimes()) == 0 {
		times := c.Time()
		for i := start; i < end; i++ {
			v := values[i]
			if times[i] > times[lastIndex] ||
				(times[i] == times[lastIndex] && v > lastValue) {
				lastIndex = i
				lastValue = v
			}
		}
		return lastIndex, lastValue, false
	}
	// column time is initialized
	times := column.ColumnTimes()
	for i := start; i < end; i++ {
		v := values[i]
		if times[i] > times[lastIndex] ||
			(times[i] == times[lastIndex] && v > lastValue) {
			lastIndex = i
			lastValue = v
		}
	}
	return lastIndex, lastValue, false
}

func LastTimeColSlowReduce[T util.ExceptBool](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	// slow path
	var defaultValue T
	column := c.Column(ordinal)
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, defaultValue, true
	}
	// column time is not initialized in the subquery
	if len(column.ColumnTimes()) == 0 {
		lastValue, lastIndex := values[vs], column.GetTimeIndex(vs)
		times := c.Time()
		for i := start; i < end; i++ {
			if column.IsNilV2(i) {
				continue
			}
			v := values[column.GetValueIndexV2(i)]
			if times[i] > times[lastIndex] ||
				(times[i] == times[lastIndex] && v > lastValue) {
				lastIndex = i
				lastValue = v
			}
		}
		return lastIndex, lastValue, false
	}
	// column time is initialized
	lastValue, lastIndex := values[vs], vs
	times := column.ColumnTimes()
	for i := vs; i < ve; i++ {
		v := values[i]
		if times[i] > times[lastIndex] ||
			(times[i] == times[lastIndex] && v > lastValue) {
			lastIndex = i
			lastValue = v
		}
	}
	return lastIndex, lastValue, false
}

func LastTimeColReduce[T util.ExceptBool](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		return LastTimeColFastReduce(c, values, ordinal, start, end)
	}
	return LastTimeColSlowReduce(c, values, ordinal, start, end)
}

func FirstTimeColFastReduce[T util.ExceptBool](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	// fast path
	column := c.Column(ordinal)
	firstValue, firstIndex := values[start], start
	// column time is not initialized in the subquery
	if len(column.ColumnTimes()) == 0 {
		times := c.Time()
		for i := start; i < end; i++ {
			v := values[i]
			if times[i] < times[firstIndex] ||
				(times[i] == times[firstIndex] && v > firstValue) {
				firstIndex = i
				firstValue = v
			}
		}
		return firstIndex, firstValue, false
	}
	// column time is initialized
	times := column.ColumnTimes()
	for i := start; i < end; i++ {
		v := values[i]
		if times[i] < times[firstIndex] ||
			(times[i] == times[firstIndex] && v > firstValue) {
			firstIndex = i
			firstValue = v
		}
	}
	return firstIndex, firstValue, false
}

func FirstTimeColSlowReduce[T util.ExceptBool](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	// slow path
	var defaultValue T
	column := c.Column(ordinal)
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, defaultValue, true
	}
	// column time is not initialized in the subquery
	if len(column.ColumnTimes()) == 0 {
		firstValue, firstIndex := values[vs], column.GetTimeIndex(vs)
		times := c.Time()
		for i := start; i < end; i++ {
			if column.IsNilV2(i) {
				continue
			}
			v := values[column.GetValueIndexV2(i)]
			if times[i] < times[firstIndex] ||
				(times[i] == times[firstIndex] && v > firstValue) {
				firstIndex = i
				firstValue = v
			}
		}
		return firstIndex, firstValue, false
	}
	// column time is initialized
	times := column.ColumnTimes()
	firstValue, firstIndex := values[vs], vs
	for i := vs; i < ve; i++ {
		v := values[i]
		if times[i] < times[firstIndex] ||
			(times[i] == times[firstIndex] && v > firstValue) {
			firstIndex = i
			firstValue = v
		}
	}
	return firstIndex, firstValue, false
}

func FirstTimeColReduce[T util.ExceptBool](c Chunk, values []T, ordinal, start, end int) (int, T, bool) {
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		return FirstTimeColFastReduce(c, values, ordinal, start, end)
	}
	return FirstTimeColSlowReduce(c, values, ordinal, start, end)
}

func BooleanFirstTimeColFastReduce(c Chunk, values []bool, ordinal, start, end int) (int, bool, bool) {
	// fast path
	column := c.Column(ordinal)
	firstValue, firstIndex := values[start], start
	// column time is not initialized in the subquery
	if len(column.ColumnTimes()) == 0 {
		times := c.Time()
		for i := start; i < end; i++ {
			v := values[i]
			if times[i] < times[firstIndex] ||
				(times[i] == times[firstIndex] && !v && firstValue) {
				firstIndex = i
				firstValue = v
			}
		}
		return firstIndex, firstValue, false
	}
	times := column.ColumnTimes()
	for i := start; i < end; i++ {
		v := values[i]
		if times[i] < times[firstIndex] ||
			(times[i] == times[firstIndex] && !v && firstValue) {
			firstIndex = i
			firstValue = v
		}
	}
	return firstIndex, firstValue, false
}

func BooleanFirstTimeColSlowReduce(c Chunk, values []bool, ordinal, start, end int) (int, bool, bool) {
	// slow path
	column := c.Column(ordinal)
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, false, true
	}
	// column time is not initialized in the subquery
	if len(column.ColumnTimes()) == 0 {
		firstValue, firstIndex := values[vs], column.GetTimeIndex(vs)
		times := c.Time()
		for i := start; i < end; i++ {
			if column.IsNilV2(i) {
				continue
			}
			v := values[column.GetValueIndexV2(i)]
			if times[i] < times[firstIndex] ||
				(times[i] == times[firstIndex] && !v && firstValue) {
				firstIndex = i
				firstValue = v
			}
		}
		return firstIndex, firstValue, false
	}
	// column time is initialized
	times := column.ColumnTimes()
	firstValue, firstIndex := values[vs], vs
	for i := vs; i < ve; i++ {
		v := values[i]
		if times[i] < times[firstIndex] ||
			(times[i] == times[firstIndex] && !v && firstValue) {
			firstIndex = i
			firstValue = v
		}
	}
	return firstIndex, firstValue, false
}

func BooleanFirstTimeColReduce(c Chunk, values []bool, ordinal, start, end int) (int, bool, bool) {
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		return BooleanFirstTimeColFastReduce(c, values, ordinal, start, end)
	}
	return BooleanFirstTimeColSlowReduce(c, values, ordinal, start, end)
}
