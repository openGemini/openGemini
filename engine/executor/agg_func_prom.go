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
	"strconv"

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
	promSubqueryFunc["rate_prom"] = rate(true, true)
	promSubqueryFunc["delta_prom"] = rate(false, false)
	promSubqueryFunc["increase"] = rate(false, true)
	promSubqueryFunc["irate_prom"] = irate(true)
	promSubqueryFunc["idelta_prom"] = irate(false)
	promSubqueryFunc["stdvar_over_time_prom"] = stdVarOverTime(false)
	promSubqueryFunc["stddev_over_time_prom"] = stdVarOverTime(true)
	promSubqueryFunc["min_over_time"] = minOverTime
	promSubqueryFunc["max_over_time"] = maxOverTime
	promSubqueryFunc["sum_over_time"] = sumOverTime
	promSubqueryFunc["count_over_time"] = countOverTime
	promSubqueryFunc["avg_over_time"] = avgOverTime
	promSubqueryFunc["mad_over_time_prom"] = madOverTime
	promSubqueryFunc["predict_linear"] = predictLinear(false)
	promSubqueryFunc["deriv"] = predictLinear(true)
	promSubqueryFunc["absent_over_time_prom"] = intervalExistMark
	promSubqueryFunc["present_over_time_prom"] = intervalExistMark
	promSubqueryFunc["last_over_time_prom"] = lastOverTime
	promSubqueryFunc["quantile_over_time_prom"] = quantileOverTime
	promSubqueryFunc["changes_prom"] = changes
	promSubqueryFunc["resets_prom"] = resets
	promSubqueryFunc["holt_winters_prom"] = holtWintersProm

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

func rate(isRate bool, isCounter bool) CallFn {
	return func(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
		pointCount := len(preTimes) + len(currTimes)
		if pointCount <= 1 {
			return 0, false
		}
		firstTime, lastTime, firstValue, _, reduceResult := CalcReduceResult(preTimes, currTimes, preValues, currValues, isCounter)
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

		if durationToStart >= extrapolationThreshold {
			durationToStart = averageDurationBetweenSamples / 2
		}
		extrapolateToInterval += durationToStart

		if durationToEnd >= extrapolationThreshold {
			durationToEnd = averageDurationBetweenSamples / 2
		}
		extrapolateToInterval += durationToEnd

		factor := extrapolateToInterval / sampledInterval
		if isRate {
			factor /= call.Range.Seconds()
		}
		reduceResult *= factor
		return reduceResult, true
	}
}

func GroupReduce(c Chunk, values []float64, ordinal, start, end int) (int, float64, bool) {
	column := c.Column(ordinal)
	if column.NilCount() == 0 {
		// fast path
		return start, float64(1), false
	}

	// slow path
	vs, ve := column.GetRangeValueIndexV2(start, end)
	if vs == ve {
		return start, 0, true
	}
	return start, float64(1), false
}

func GroupMerge(prevPoint, currPoint *Point[float64]) {
	prevPoint.value = float64(1)
}

func QuantileReduce(percentile float64) SliceReduce[float64] {
	return func(floatSliceItem *SliceItem[float64]) (int, int64, float64, bool) {
		length := len(floatSliceItem.value)
		if length == 0 {
			return -1, int64(0), math.NaN(), false
		}

		if percentile < 0 {
			return -1, floatSliceItem.time[0], math.Inf(-1), false
		} else if percentile > 1 {
			return -1, floatSliceItem.time[0], math.Inf(+1), false
		}

		sort.Sort(floatSliceItem)

		n := float64(length)
		rank := percentile * (n - 1)

		lowerIndex := math.Max(0, math.Floor(rank))
		upperIndex := math.Min(n-1, lowerIndex+1)

		weight := rank - math.Floor(rank)

		return -1, floatSliceItem.time[int(math.Floor(rank))], floatSliceItem.value[int(lowerIndex)]*(1-weight) + floatSliceItem.value[int(upperIndex)]*weight, false
	}
}

func CalcLastTwoPoints(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64) (int64, int64, float64, float64) {
	preLen := len(preTimes)
	currLen := len(currTimes)
	if currLen == 0 {
		if preLen < 2 {
			return 0, 0, 0, 0
		}
		return preTimes[preLen-2], preTimes[preLen-1], preValues[preLen-2], preValues[preLen-1]
	}
	if currLen == 1 {
		if preLen < 1 {
			return 0, 0, 0, 0
		}
		return preTimes[preLen-1], currTimes[0], preValues[preLen-1], currValues[0]
	}
	return currTimes[currLen-2], currTimes[currLen-1], currValues[currLen-2], currValues[currLen-1]
}

func irate(isRate bool) CallFn {
	return func(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
		pointCount := len(preTimes) + len(currTimes)
		if pointCount <= 1 {
			return 0, false
		}

		prevTime, lastTime, prevValue, lastValue := CalcLastTwoPoints(preTimes, currTimes, preValues, currValues)
		if lastTime == prevTime || call.Range.Nanoseconds() == 0 {
			return 0, false
		}

		var resultValue float64
		if isRate && lastValue < prevValue {
			resultValue = lastValue
		} else {
			resultValue = lastValue - prevValue
		}

		if isRate {
			// Convert to per-second.
			resultValue /= float64(lastTime-prevTime) / 1e9
		}
		return resultValue, true
	}
}

func KahanSumInc(inc, sum, c float64) (newSum, newC float64) {
	t := sum + inc
	if math.Abs(sum) >= math.Abs(inc) {
		c += (sum - t) + inc
	} else {
		c += (inc - t) + sum
	}
	return t, c
}

func stdVarOverTime(isStdDev bool) CallFn {
	return func(preTimes, currTimes []int64, preValues, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
		timesCount := len(preTimes) + len(currTimes)
		valuesCount := len(preValues) + len(currValues)
		if timesCount <= 1 || valuesCount <= 1 {
			return 0, false
		}

		var count float64
		var mean, cMean float64
		var aux, cAux float64
		var nanInfFlag bool

		nanInfFlag = false
		reduce := func(values []float64) {
			for _, v := range values {
				if math.IsNaN(v) || math.IsInf(v, 0) || nanInfFlag {
					nanInfFlag = true
					break
				}

				count++
				delta := v - (mean + cMean)
				mean, cMean = KahanSumInc(delta/count, mean, cMean)
				aux, cAux = KahanSumInc(delta*(v-(mean+cMean)), aux, cAux)
			}
		}
		reduce(preValues)
		reduce(currValues)

		if nanInfFlag {
			return math.NaN(), true
		}

		stdVar := (aux + cAux) / count
		if isStdDev {
			return math.Sqrt(stdVar), true
		}
		return stdVar, true
	}
}

func minOverTime(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	r := math.NaN()
	for _, v := range preValues {
		if math.IsNaN(r) || v < r {
			r = v
		}
	}
	for _, v := range currValues {
		if math.IsNaN(r) || v < r {
			r = v
		}
	}
	return r, true
}

func maxOverTime(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	r := math.NaN()
	for _, v := range preValues {
		if math.IsNaN(r) || v > r {
			r = v
		}
	}
	for _, v := range currValues {
		if math.IsNaN(r) || v > r {
			r = v
		}
	}
	return r, true
}

func sumOverTime(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	var sum, c float64
	for _, v := range preValues {
		sum, c = KahanSumInc(v, sum, c)
	}
	for _, v := range currValues {
		sum, c = KahanSumInc(v, sum, c)
	}
	if math.IsInf(sum, 0) {
		return sum, true
	}
	return sum + c, true
}

func countOverTime(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	return float64(len(preValues) + len(currValues)), true
}

func calculateSliceMean(values []float64, mean, count, rest float64) (float64, float64, float64) {
	for _, v := range values {
		count++
		if math.IsInf(mean, 0) {
			if math.IsInf(v, 0) && (mean > 0) == (v > 0) {
				// The `mean` and `v` values are `Inf` of the same sign.  They
				// can't be subtracted, but the value of `mean` is correct
				// already.
				continue
			}
			if !math.IsInf(v, 0) && !math.IsNaN(v) {
				// At this stage, the mean is an infinite. If the added
				// value is neither an Inf or a Nan, we can keep that mean
				// value.
				// This is required because our calculation below removes
				// the mean value, which would look like Inf += x - Inf and
				// end up as a NaN.
				continue
			}
		}
		mean, rest = KahanSumInc(v/count-mean/count, mean, rest)
	}
	return mean, count, rest
}

func avgOverTime(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	mean, count, rest := calculateSliceMean(preValues, 0, 0, 0)
	mean, _, rest = calculateSliceMean(currValues, mean, count, rest)
	if math.IsInf(mean, 0) {
		return mean, true
	}
	return mean + rest, true
}

func predictLinear(isDeriv bool) CallFn {
	return func(preTimes, currTimes []int64, preValues, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
		timesCount := len(preTimes) + len(currTimes)
		valuesCount := len(preValues) + len(currValues)
		if timesCount <= 1 || valuesCount <= 1 {
			return 0, false
		}

		var (
			fv         float64
			n          float64
			sumX, cX   float64
			sumY, cY   float64
			sumXY, cXY float64
			sumX2, cX2 float64
			constY     bool
			index      int
		)

		if len(preValues) > 0 {
			fv = preValues[0]
		} else {
			fv = currValues[0]
		}

		constY = true
		reduce := func(times []int64, values []float64) {
			for i, v := range values {
				index++
				// Set constY to false if any new y values are encountered.
				if constY && index > 0 && v != fv {
					constY = false
				}
				n += 1.0
				x := float64(times[i]-ts) / 1e9
				sumX, cX = KahanSumInc(x, sumX, cX)
				sumY, cY = KahanSumInc(v, sumY, cY)
				sumXY, cXY = KahanSumInc(x*v, sumXY, cXY)
				sumX2, cX2 = KahanSumInc(x*x, sumX2, cX2)
			}
		}

		reduce(preTimes, preValues)
		reduce(currTimes, currValues)

		if constY {
			if math.IsInf(fv, 0) {
				return math.NaN(), true
			}
			if isDeriv {
				return 0, true
			}
			return fv, true
		}

		sumX += cX
		sumY += cY
		sumXY += cXY
		sumX2 += cX2

		covXY := sumXY - sumX*sumY/n
		varX := sumX2 - sumX*sumX/n
		derivative := covXY / varX

		if isDeriv {
			return derivative, true
		}

		intercept := sumY/n - derivative*sumX/n
		scalar, _ := strconv.ParseFloat(call.InArgs[0].String(), 64)
		return derivative*scalar + intercept, true
	}
}

func Float64Equal(a, b float64) bool {
	equal := false
	if math.IsNaN(a) && math.IsNaN(b) {
		equal = true
	} else if math.IsInf(a, 0) && math.IsInf(b, 0) {
		equal = true
	} else if !math.IsNaN(a) && !math.IsInf(a, 0) && a == b {
		equal = true
	}
	return equal
}

/*
CalcQuantile calculates the q-quantile of a given dataset.
Parameters:

	q: the quantile to calculate (should be between 0 and 1).
	values: a slice of float64 numbers representing the dataset.

Return value:

	The calculated q-quantile.

Note: This function modifies the input slice by sorting its elements.

If q is less than 0 or greater than 1, it returns positive or negative infinity.
If the input slice is empty or q is NaN, it returns NaN.
*/
func CalcQuantile(q float64, values []float64) float64 {
	if len(values) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	sort.Float64s(values)
	n := float64(len(values))
	rank := q * (n - 1)
	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)
	weight := rank - math.Floor(rank)
	return values[int(lowerIndex)]*(1-weight) + values[int(upperIndex)]*weight
}

func CalcQuantile2(q float64, preValues []float64, currValues []float64) float64 {
	tmp := make([]float64, 0, len(preValues)+len(currValues))
	tmp = append(tmp, preValues...)
	tmp = append(tmp, currValues...)
	return CalcQuantile(q, tmp)
}

/*
CalcMad calculates the Mean Absolute Deviation (MAD) of a given dataset.
Parameters:

	values: a slice of float64 numbers representing the dataset to calculate the MAD.

Return value:

	The calculated MAD.

Note: This function modifies the input slice by changing its values in place.
*/
func CalcMad(values []float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	median := CalcQuantile(0.5, values)
	for i := 0; i < len(values); i++ {
		values[i] = math.Abs(values[i] - median)
	}
	return CalcQuantile(0.5, values)
}

func CalcMad2(preValues []float64, currValues []float64) float64 {
	tmp := make([]float64, 0, len(preValues)+len(currValues))
	tmp = append(tmp, preValues...)
	tmp = append(tmp, currValues...)
	return CalcMad(tmp)
}

func madOverTime(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	if len(preTimes)+len(currTimes) == 0 || len(preValues)+len(currValues) == 0 {
		return math.NaN(), false
	}
	return CalcMad2(preValues, currValues), true
}

func intervalExistMark(preTimes, currTimes []int64, preValues, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	if len(preTimes)+len(currTimes) == 0 || len(preValues)+len(currValues) == 0 {
		return 0, false
	}
	return 1, true
}
func lastOverTime(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	if len(currValues) > 0 && len(currTimes) > 0 {
		return currValues[len(currValues)-1], true
	}
	if len(preValues) > 0 && len(preTimes) > 0 {
		return preValues[len(preValues)-1], true
	}
	return 0, false
}

func quantileOverTime(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	if len(preTimes)+len(currTimes) < 1 || len(preValues)+len(currValues) < 1 {
		return 0, false
	}
	var expr influxql.Expr = call.InArgs[0]
	var percentage float64
	val, ok := expr.(*influxql.NumberLiteral)
	if !ok {
		return 0, false
	}
	percentage = val.Val
	return CalcQuantile2(percentage, preValues, currValues), true
}

func changes(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	pointCount := len(preTimes) + len(currTimes)
	valueCount := len(preValues) + len(currValues)

	if pointCount < 1 || valueCount < 1 {
		return 0, false
	}

	return CalcChange(preValues, currValues), true
}
func CalcChange(preValues []float64, currValues []float64) float64 {
	count := 0
	var pre float64
	var init bool
	if len(preValues) > 0 {
		pre = preValues[0]
		init = true
		pre, count = DoCalcChange(preValues[1:], pre, count)
	}
	if len(currValues) > 0 {
		if !init {
			pre = currValues[0]
			_, count = DoCalcChange(currValues[1:], pre, count)
		} else {
			_, count = DoCalcChange(currValues, pre, count)
		}
	}
	return float64(count)
}

func DoCalcChange(curV []float64, prev float64, changes int) (float64, int) {
	for _, sample := range curV {
		current := sample
		if current != prev && !(math.IsNaN(current) && math.IsNaN(prev)) {
			changes++
		}
		prev = current
	}
	return prev, changes
}

func resets(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	pointCount := len(preTimes) + len(currTimes)
	if pointCount <= 0 {
		return 0, false
	}
	if pointCount == 1 {
		return 0, true
	}
	return CalcResets(preValues, currValues), true
}

func DoCalcResets(prevV []float64, prev float64, resets int) (float64, int) {
	for _, sample := range prevV {
		if sample < prev {
			resets++
		}
		prev = sample
	}
	return prev, resets
}

func CalcResets(preV []float64, curV []float64) float64 {
	count := 0
	var pre float64
	var init bool
	if len(preV) > 0 {
		pre = preV[0]
		init = true
		pre, count = DoCalcResets(preV[1:], pre, count)
	}
	if len(curV) > 0 {
		if !init {
			pre = curV[0]
			_, count = DoCalcResets(curV[1:], pre, count)
		} else {
			_, count = DoCalcResets(curV, pre, count)
		}
	}
	return float64(count)
}

func holtWintersProm(preTimes []int64, currTimes []int64, preValues []float64, currValues []float64, ts int64, call *influxql.PromSubCall) (float64, bool) {
	pointCount := len(preTimes) + len(currTimes)
	if pointCount < 2 {
		return 0, false
	}
	var expr influxql.Expr = call.InArgs[0]
	var expr2 influxql.Expr = call.InArgs[1]
	var sf, tf float64
	val1, ok := expr.(*influxql.NumberLiteral)
	if !ok {
		return 0, false
	}
	val2, ok := expr2.(*influxql.NumberLiteral)
	if !ok {
		return 0, false
	}
	sf = val1.Val
	tf = val2.Val
	if sf < 0 || sf > 1 || tf < 0 || tf > 1 {
		return 0, false
	}
	return CalcHoltWinters(preValues, currValues, sf, tf), true
}

// Calculate the trend value at the given index i in raw data d.
// This is somewhat analogous to the slope of the trend at the given index.
// The argument "tf" is the trend factor.
// The argument "s0" is the computed smoothed value.
// The argument "s1" is the computed trend factor.
// The argument "b" is the raw input value.
func calcTrendValue(i int, tf, s0, s1, b float64) float64 {
	if i == 0 {
		return b
	}
	x := tf * (s1 - s0)
	y := (1 - tf) * b
	return x + y
}

func CalcHoltWinters(preV []float64, curV []float64, sf, tf float64) float64 {
	var s0, s1, b float64
	var tmp []float64
	if len(preV) > 0 {
		tmp = append(tmp, preV...)
	}
	if len(curV) > 0 {
		tmp = append(tmp, curV...)
	}

	for j := 0; j < len(tmp); j++ {
		if math.IsNaN(tmp[j]) || math.IsInf(tmp[j], 0) {
			return math.NaN()
		}
	}
	s1 = tmp[0]
	b = tmp[1] - tmp[0]
	var x, y float64
	for i := 1; i < len(tmp); i++ {
		// Scale the raw value against the smoothing factor.
		x = sf * tmp[i]
		// Scale the last smoothed value with the trend at this point.
		b = calcTrendValue(i-1, tf, s0, s1, b)
		y = (1 - sf) * (s1 + b)
		s0, s1 = s1, x+y
	}
	return s1

}
