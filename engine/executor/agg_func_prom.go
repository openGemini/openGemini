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
	"sort"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const smallDeltaTolerance = 1e-12

func MinPromReduce(c Chunk, values []float64, ordinal, start, end int) (int, float64, bool) {
	minValue := values[start]
	for i := start + 1; i < end; i++ {
		v := values[i]
		if minValue > v || math.IsNaN(minValue) {
			minValue = v
		}
	}
	return start, minValue, false
}

func MinPromMerge(prevPoint, currPoint *Point[float64]) {
	if prevPoint.value > currPoint.value || math.IsNaN(prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func MaxPromReduce(c Chunk, values []float64, ordinal, start, end int) (int, float64, bool) {
	maxValue := values[start]
	for i := start + 1; i < end; i++ {
		v := values[i]
		if maxValue < v || math.IsNaN(maxValue) {
			maxValue = v
		}
	}
	return start, maxValue, false
}

func MaxPromMerge(prevPoint, currPoint *Point[float64]) {
	if prevPoint.value < currPoint.value || math.IsNaN(prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func FloatCountPromReduce(c Chunk, values []float64, ordinal, start, end int) (int, float64, bool) {
	count := float64(end - start)
	return start, count, count == 0
}

func FloatCountPromMerge(prevPoint, currPoint *Point[float64]) {
	prevPoint.value += currPoint.value
}

func FloatHistogramQuantilePromReduce(p float64) FloatColReduceHistogramReduce {
	return func(buckets []bucket) float64 {
		if math.IsNaN(p) {
			return math.NaN()
		}

		if p < 0 {
			return math.Inf(-1)
		} else if p > 1 {
			return math.Inf(+1)
		}

		if !math.IsInf(buckets[len(buckets)-1].upperBound, +1) {
			return math.NaN()
		}

		ensureMonotonicAndIgnoreSmallDeltas(buckets, smallDeltaTolerance)

		if len(buckets) < 2 {
			return math.NaN()
		}

		countAll := buckets[len(buckets)-1].count
		if countAll == 0 {
			return math.NaN()
		}

		rank := p * countAll

		bucketIndex := sort.Search(len(buckets)-1, func(i int) bool { return buckets[i].count >= rank })

		if bucketIndex == len(buckets)-1 {
			return buckets[len(buckets)-2].upperBound
		}

		if bucketIndex == 0 && buckets[0].upperBound <= 0 {
			return buckets[0].upperBound
		}

		var (
			bucketStart float64
			bucketEnd   = buckets[bucketIndex].upperBound
			count       = buckets[bucketIndex].count
		)

		if bucketIndex > 0 {
			bucketStart = buckets[bucketIndex-1].upperBound
			count = count - buckets[bucketIndex-1].count
			rank = rank - buckets[bucketIndex-1].count
		}

		return bucketStart + (bucketEnd-bucketStart)*(rank/count)
	}
}

/*
Copyright 2015 The Prometheus Authors
This code is originally from: https://github.com/prometheus/prometheus/blob/main/promql/quantile.go
*/
func ensureMonotonicAndIgnoreSmallDeltas(buckets []bucket, tolerance float64) (bool, bool) {
	var forcedMonotonic, fixedPrecision bool
	prev := buckets[0].count
	for i := 1; i < len(buckets); i++ {
		curr := buckets[i].count // Assumed always positive.
		if curr == prev {
			// No correction needed if the counts are identical between buckets.
			continue
		}
		if almostEqual(prev, curr, tolerance) {
			//Silently correct numerically insignificant differences from floating
			//point precision errors, regardless of direction.
			//Do not update the 'prev' value as we are ignoring the difference.
			buckets[i].count = prev
			fixedPrecision = true
			continue
		}
		if curr < prev {
			// Force monotonicity by removing any decreases regardless of magnitude.
			// Do not update the 'prev' value as we are ignoring the decrease.
			buckets[i].count = prev
			forcedMonotonic = true
			continue
		}
		prev = curr
	}
	return forcedMonotonic, fixedPrecision
}

/*
Copyright 2015 The Prometheus Authors
This code is originally from: https://github.com/prometheus/prometheus/blob/main/promql/quantile.go
*/
// almostEqual returns true if a and b differ by less than their sum
// multiplied by epsilon.
func almostEqual(a, b, epsilon float64) bool {
	minNormal := math.Float64frombits(0x0010000000000000)
	// NaN has no equality but for testing we still want to know whether both values
	// are NaN.
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}

	// Cf. http://floating-point-gui.de/errors/comparison/
	if a == b {
		return true
	}

	absSum := math.Abs(a) + math.Abs(b)
	diff := math.Abs(a - b)

	if a == 0 || b == 0 || absSum < minNormal {
		return diff < epsilon*minNormal
	}
	return diff/math.Min(absSum, math.MaxFloat64) < epsilon
}

type CallFn func([]int64, []int64, []float64, []float64, int64, *influxql.PromSubCall) (float64, bool)

var promSubqueryFunc map[string]CallFn = make(map[string]CallFn)

func init() {
	promSubqueryFunc["rate_prom"] = rate
}

func CalcReduceResult(prevT, currT []int64, prevV, currV []float64, isCounter bool) (int64, int64, float64, float64, float64) {
	var firstTime, lastTime int64
	var firstValue, lastValue float64
	if len(prevT) > 0 {
		firstTime = prevT[0]
		firstValue = prevV[0]
		if len(currT) > 0 {
			lastTime = currT[len(currT)-1]
			lastValue = currV[len(currV)-1]
		} else {
			lastTime = prevT[len(prevT)-1]
			lastValue = prevV[len(prevV)-1]
		}
	} else {
		firstTime, lastTime = currT[0], currT[len(currT)-1]
		firstValue, lastValue = currV[0], currV[len(currV)-1]
	}
	reduceResult := lastValue - firstValue
	if isCounter {
		prev := firstValue
		for _, cur := range prevV {
			if cur < prev {
				reduceResult += prev
			}
			prev = cur
		}
		for _, cur := range currV {
			if cur < prev {
				reduceResult += prev
			}
			prev = cur
		}
	}
	return firstTime, lastTime, firstValue, lastValue, reduceResult
}

func rate(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	pointCount := len(preTimes) + len(currTimes)
	if pointCount <= 1 {
		return 0, false
	}
	firstTime, lastTime, firstValue, _, reduceResult := CalcReduceResult(preTimes, currTimes, preValues, currValues, true)
	if lastTime == firstTime || call.Range.Nanoseconds() == 0 {
		return 0, false
	}
	rangeStart, rangeEnd := ts-call.Range.Nanoseconds(), ts

	// Duration between first/last samples and boundary of range.
	durationToStart := float64(firstTime-rangeStart) / 1e9
	durationToEnd := float64(rangeEnd-lastTime) / 1e9

	sampledInterval := float64(lastTime-firstTime) / 1e9
	averageDurationBetweenSamples := sampledInterval / float64(pointCount-1)

	if reduceResult > 0 && pointCount > 0 && firstValue >= 0 {
		durationToZero := sampledInterval * (firstValue / reduceResult)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	// If the first/last samples are close to the boundaries of the range,
	// extrapolate the result. This is as we expect that another sample
	// will exist given the spacing between samples we've seen thus far,
	// with an allowance for noise.
	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart < extrapolationThreshold {
		extrapolateToInterval += durationToStart
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	if durationToEnd < extrapolationThreshold {
		extrapolateToInterval += durationToEnd
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	factor := extrapolateToInterval / sampledInterval
	factor /= call.Range.Seconds()
	reduceResult *= factor
	return reduceResult, true
}
