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
)

const smallDeltaTolerance = 1e-12

func MinPromReduce(c Chunk, ordinal, start, end int) (int, float64, bool) {
	minValue := c.Column(ordinal).FloatValue(start)
	for i := start + 1; i < end; i++ {
		v := c.Column(ordinal).FloatValue(i)
		if minValue > v || math.IsNaN(minValue) {
			minValue = v
		}
	}
	return start, minValue, false
}

func MinPromMerge(prevPoint, currPoint *FloatPoint) {
	if prevPoint.value > currPoint.value || math.IsNaN(prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func MaxPromReduce(c Chunk, ordinal, start, end int) (int, float64, bool) {
	maxValue := c.Column(ordinal).FloatValue(start)
	for i := start + 1; i < end; i++ {
		v := c.Column(ordinal).FloatValue(i)
		if maxValue < v || math.IsNaN(maxValue) {
			maxValue = v
		}
	}
	return start, maxValue, false
}

func MaxPromMerge(prevPoint, currPoint *FloatPoint) {
	if prevPoint.value < currPoint.value || math.IsNaN(prevPoint.value) {
		prevPoint.Assign(currPoint)
	}
}

func FloatCountPromReduce(c Chunk, ordinal, start, end int) (int, float64, bool) {
	count := float64(end - start)
	return start, count, count == 0
}

func FloatCountPromMerge(prevPoint, currPoint *FloatPoint) {
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
