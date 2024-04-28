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

package engine

import (
	"fmt"
	"math"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func init() {
	RegistryPromFunction("rate_prom", &rateOp{})
	RegistryPromFunction("irate_prom", &irateOp{})
	RegistryPromFunction("avg_over_time", &avgOp{})
	RegistryPromFunction("count_over_time", &countOp{})
	RegistryPromFunction("sum_over_time", &sumOp{})
	RegistryPromFunction("min_over_time", &minOp{})
	RegistryPromFunction("max_over_time", &maxOp{})
	RegistryPromFunction("last_over_time", &lastOp{})
	RegistryPromFunction("increase", &increaseOp{})
	RegistryPromFunction("deriv", &derivOp{})
	RegistryPromFunction("predict_linear", &PredictLinearOp{})
}

type PromFunction interface {
	CreateRoutine(param *PromFuncParam) (Routine, error)
}

type PromFuncParam struct {
	inOrdinal, outOrdinal int
	args                  []influxql.Expr
}

var factoryInstance = make(map[string]PromFunction)

func GetPromFunction(name string) PromFunction {
	return factoryInstance[name]
}

type PromFunctionFactory map[string]PromFunction

func RegistryPromFunction(name string, aggOp PromFunction) {
	_, ok := factoryInstance[name]
	if ok {
		return
	}
	factoryInstance[name] = aggOp
}

type rateOp struct{}

func (o *rateOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatPromRateReduce, floatPromRateMerge(true, true)), p.inOrdinal, p.outOrdinal), nil
}

type irateOp struct{}

func (o *irateOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatRateReducer(floatIRateReduce, floatIRateMerge, floatIRateUpdate), p.inOrdinal, p.outOrdinal), nil
}

type avgOp struct{}

func (o *avgOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatAvgReduce, floatAvgMergeFunc), p.inOrdinal, p.outOrdinal), nil
}

type countOp struct{}

func (o *countOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatPromCountReduce, floatPromCountMergeFunc), p.inOrdinal, p.outOrdinal), nil
}

type sumOp struct{}

func (o *sumOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatPromSumReduce, floatPromSumMergeFunc), p.inOrdinal, p.outOrdinal), nil
}

type minOp struct{}

func (o *minOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatPromMinReduce, floatPromMinMergeFunc), p.inOrdinal, p.outOrdinal), nil
}

type maxOp struct{}

func (o *maxOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatPromMaxReduce, floatPromMaxMergeFunc), p.inOrdinal, p.outOrdinal), nil
}

type lastOp struct{}

func (o *lastOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatPromLastReduce, floatPromLastMergeFunc), p.inOrdinal, p.outOrdinal), nil
}

type increaseOp struct{}

func (o *increaseOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatPromRateReduce, floatPromRateMerge(false, true)), p.inOrdinal, p.outOrdinal), nil
}

type derivOp struct{}

func (o *derivOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatPromDerivReduce, linearMergeFunc(true, 0)), p.inOrdinal, p.outOrdinal), nil
}

type PredictLinearOp struct{}

func (o *PredictLinearOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	var scalar float64
	switch arg := p.args[len(p.args)-1].(type) {
	case *influxql.IntegerLiteral:
		scalar = float64(arg.Val)
	case *influxql.NumberLiteral:
		scalar = arg.Val
	default:
		return nil, errno.NewError(errno.UnsupportedDataType, "argument of predict_linear", arg.String())
	}
	return NewRoutineImpl(newFloatSliceReducer(floatPromDerivReduce, linearMergeFunc(false, scalar)), p.inOrdinal, p.outOrdinal), nil
}

func newPromFuncProcessor(inSchema, outSchema record.Schemas, exprOpt []hybridqp.ExprOptions) (CoProcessor, error) {
	coProcessor := NewCoProcessorImpl()
	var inField, outField string
	for i := range exprOpt {
		switch expr := exprOpt[i].Expr.(type) {
		case *influxql.Call:
			inField, outField = expr.Args[0].(*influxql.VarRef).Val, exprOpt[i].Ref.Val
			inOrdinal, outOrdinal := inSchema.FieldIndex(inField), outSchema.FieldIndex(outField)
			param := &PromFuncParam{inOrdinal: inOrdinal, outOrdinal: outOrdinal, args: expr.Args}
			routine, err := GetPromFunction(expr.Name).CreateRoutine(param)
			if err != nil {
				return nil, err
			}
			coProcessor.AppendRoutine(routine)
		default:
			return nil, fmt.Errorf("invalid the expr type")
		}
	}
	return coProcessor, nil
}

func floatAvgReduce(times []int64, values []float64, start, end int) (int64, float64, bool) {
	if start == end {
		return 0, 0, true
	}
	var mean, count, c float64
	for i := start; i < end; i++ {
		count++
		if math.IsInf(mean, 0) {
			if math.IsInf(values[i], 0) && (mean > 0) == (values[i] > 0) {
				// The `mean` and `f.F` values are `Inf` of the same sign.  They
				// can't be subtracted, but the value of `mean` is correct
				// already.
				continue
			}
			if !math.IsInf(values[i], 0) && !math.IsNaN(values[i]) {
				// At this stage, the mean is an infinite. If the added
				// value is neither an Inf or a Nan, we can keep that mean
				// value.
				// This is required because our calculation below removes
				// the mean value, which would look like Inf += x - Inf and
				// end up as a NaN.
				continue
			}
		}
		mean, c = kahanSumInc(values[i]/count-mean/count, mean, c)
	}
	if math.IsInf(mean, 0) {
		return times[start], mean, false
	}
	return times[start], mean + c, false
}

func floatAvgMergeFunc(prevValue float64, currValue float64, prevCount, currCount int) (float64, int) {
	count := prevCount + currCount
	return (prevValue*float64(prevCount) + currValue*float64(currCount)) / float64(count), count
}

func floatPromCountReduce(times []int64, values []float64, start, end int) (int64, float64, bool) {
	if start == end {
		return 0, 0, true
	}
	return times[start], float64(end - start), false
}

func floatPromCountMergeFunc(prevValue float64, currValue float64, prevCount, currCount int) (float64, int) {
	return prevValue + currValue, prevCount + currCount
}

func floatPromSumReduce(times []int64, values []float64, start, end int) (int64, float64, bool) {
	if start == end {
		return 0, 0, true
	}
	var sum, c float64
	for i := start; i < end; i++ {
		sum, c = kahanSumInc(values[i], sum, c)
	}
	if math.IsInf(sum, 0) {
		return times[start], sum, false
	}
	return times[start], sum + c, false
}

func floatPromSumMergeFunc(prevValue float64, currValue float64, prevCount, currCount int) (float64, int) {
	return prevValue + currValue, prevCount + currCount
}

func floatPromMinReduce(times []int64, values []float64, start, end int) (int64, float64, bool) {
	if start == end {
		return 0, 0, true
	}
	minIndex, minValue := start, values[start]
	for i := start + 1; i < end; i++ {
		if values[i] < minValue || math.IsNaN(minValue) {
			minValue = values[i]
			minIndex = i
		}
	}
	return times[minIndex], minValue, false
}

func floatPromMinMergeFunc(prevValue float64, currValue float64, prevCount, currCount int) (float64, int) {
	count := prevCount + currCount
	if math.IsNaN(prevValue) {
		return currValue, count
	}
	if math.IsNaN(currValue) {
		return prevValue, count
	}
	if prevValue < currValue {
		return prevValue, count
	} else {
		return currValue, count
	}
}

func floatPromMaxReduce(times []int64, values []float64, start, end int) (int64, float64, bool) {
	if start == end {
		return 0, 0, true
	}
	maxIndex, maxValue := start, values[start]
	for i := start + 1; i < end; i++ {
		if values[i] > maxValue || math.IsNaN(maxValue) {
			maxValue = values[i]
			maxIndex = i
		}
	}
	return times[maxIndex], maxValue, false
}

func floatPromMaxMergeFunc(prevValue float64, currValue float64, prevCount, currCount int) (float64, int) {
	count := prevCount + currCount
	if math.IsNaN(prevValue) {
		return currValue, count
	}
	if math.IsNaN(currValue) {
		return prevValue, count
	}
	if prevValue > currValue {
		return prevValue, count
	} else {
		return currValue, count
	}
}

func floatPromLastReduce(times []int64, values []float64, start, end int) (int64, float64, bool) {
	if start == end {
		return 0, 0, true
	}
	return times[end-1], values[end-1], false
}

func floatPromLastMergeFunc(prevValue float64, currValue float64, prevCount, currCount int) (float64, int) {
	return currValue, prevCount + currCount
}

func floatPromRateReduce(times []int64, values []float64, start, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatPromRateMerge(isRate, isCounter bool) FloatSliceMergeFunc {
	return func(prevT, currT []int64, prevV, currV []float64, ts int64, pointCount int, param *ReducerParams) (float64, bool) {
		if pointCount <= 1 {
			return 0, true
		}
		firstTime, lastTime, firstValue, _, reduceResult := calcReduceResult(prevT, currT, prevV, currV, isCounter)
		if lastTime == firstTime || param.rangeDuration == 0 {
			return 0, true
		}
		rangeStart, rangeEnd := ts-param.rangeDuration, ts

		// Duration between first/last samples and boundary of range.
		durationToStart := float64(firstTime-rangeStart) / 1e9
		durationToEnd := float64(rangeEnd-lastTime) / 1e9

		sampledInterval := float64(lastTime-firstTime) / 1e9
		averageDurationBetweenSamples := sampledInterval / float64(pointCount-1)

		if isCounter && reduceResult > 0 && pointCount > 0 && firstValue >= 0 {
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
		resultValue := reduceResult * (extrapolateToInterval / sampledInterval)
		if isRate {
			resultValue = resultValue / float64(param.rangeDuration/1e9)
		}
		return resultValue, false
	}
}

func calcReduceResult(prevT, currT []int64, prevV, currV []float64, isCounter bool) (int64, int64, float64, float64, float64) {
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

func floatIRateReduce(times []int64, values []float64, start, end int) (int64, int64, float64, float64, bool) {
	if start >= end {
		return 0, 0, 0, 0, true
	}
	if end-start == 1 {
		return times[end-1], times[end-1], values[end-1], values[end-1], false
	}
	return times[end-2], times[end-1], values[end-2], values[end-1], false
}

func floatIRateMerge(prevTime int64, lastTime int64, prevValue float64, lastValue float64,
	ts int64, pointCount int, param *ReducerParams) (float64, bool) {
	if lastTime == prevTime || param.rangeDuration == 0 || pointCount < 2 {
		return 0, true
	}

	var resultValue float64
	if lastValue < prevValue {
		resultValue = lastValue
	} else {
		resultValue = lastValue - prevValue
	}

	sampledInterval := lastTime - prevTime
	if sampledInterval == 0 {
		// Avoid dividing by 0.
		return 0, true
	}

	// Convert to per-second.
	resultValue /= float64(sampledInterval) / 1e9
	return resultValue, false
}

func floatIRateUpdate(ft1, ft2, lt1, lt2 int64, fv1, fv2, lv1, lv2 float64) (int64, int64, float64, float64) {
	if ft2 < lt2 {
		return ft2, lt2, fv2, lv2
	}
	return lt1, lt2, lv1, lv2
}

func floatPromDerivReduce(times []int64, values []float64, start, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func linearMergeFunc(isDeriv bool, scalar float64) FloatSliceMergeFunc {
	return func(t1, t2 []int64, v1, v2 []float64, ts int64, pointCount int, param *ReducerParams) (float64, bool) {
		var fv float64
		if len(v1) > 0 {
			fv = v1[0]
		} else if len(v2) > 0 {
			fv = v2[0]
		} else {
			return 0, true
		}
		var (
			n          float64
			sumX, cX   float64
			sumY, cY   float64
			sumXY, cXY float64
			sumX2, cX2 float64
			constY     bool
			index      int
		)
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
				sumX, cX = kahanSumInc(x, sumX, cX)
				sumY, cY = kahanSumInc(v, sumY, cY)
				sumXY, cXY = kahanSumInc(x*v, sumXY, cXY)
				sumX2, cX2 = kahanSumInc(x*x, sumX2, cX2)
			}

		}
		reduce(t1, v1)
		reduce(t2, v2)

		if constY {
			if math.IsInf(fv, 0) {
				return math.NaN(), true
			}
			return 0, true
		}

		sumX += cX
		sumY += cY
		sumXY += cXY
		sumX2 += cX2

		covXY := sumXY - sumX*sumY/n
		varX := sumX2 - sumX*sumX/n

		deriv := covXY / varX
		if isDeriv {
			return deriv, false
		}
		intercept := sumY/n - deriv*sumX/n
		return (deriv*scalar + intercept), false
	}
}

type floatBuffer struct {
	times  []int64
	values []float64
	s, e   int
}

func newFloatBuffer() *floatBuffer {
	return &floatBuffer{}
}

func (b *floatBuffer) updateIndex(rangeStart, rangeEnd int64) {
	for b.s < len(b.times) {
		if b.times[b.s] >= rangeStart {
			break
		}
		b.s++
	}
	for b.e < len(b.times) {
		if b.times[b.e] > rangeEnd {
			break
		}
		b.e++
	}
}

func (b *floatBuffer) updateValue(times []int64, values []float64, param *ReducerParams, rangeEnd int64, start int) {
	defer func() {
		b.s, b.e = 0, 0
	}()
	var rangeStart int64
	if param.sameWindow {
		rangeStart = rangeEnd - param.rangeDuration
	} else {
		rangeStart = rangeEnd + param.step - param.rangeDuration
	}

	if start > 0 || len(b.times) == 0 || b.times[len(b.times)-1] < rangeStart {
		for start < len(times) {
			if times[start] >= rangeStart {
				b.times = b.times[:0]
				b.values = b.values[:0]
				b.times = append(b.times, times[start:]...)
				b.values = append(b.values, values[start:]...)
				return
			}
			start++
		}
		b.times = b.times[:0]
		b.values = b.values[:0]
		return
	}

	for b.s < len(times) {
		if b.times[b.s] >= rangeStart {
			b.times = append(b.times, times[b.s:]...)
			b.values = append(b.values, values[b.s:]...)
			return
		}
		b.s++
	}
	b.times = append(b.times, times...)
	b.values = append(b.values, values...)
}

type floatPoint struct {
	time  int64
	value float64
	count int
	isNil bool
}

func newFloatPoint() *floatPoint {
	return &floatPoint{isNil: true}
}

func (p *floatPoint) Set(time int64, value float64, count int) {
	p.time = time
	p.value = value
	p.count = count
	p.isNil = false
}

func (p *floatPoint) Reset() {
	p.isNil = true
}

// FloatRateReduceFunc is used to process intermediate calculation results. eg, calculates the first and last time points for rate.
type FloatRateReduceFunc func(times []int64, values []float64, start, end int) (int64, int64, float64, float64, bool)

// FloatRateMergeFunc is used to calculate the final result based on the intermediate result and function definition.
type FloatRateMergeFunc func(prevTime int64, currTime int64, prevValue float64, currValue float64, ts int64, pointCount int, param *ReducerParams) (float64, bool)

// FloatRateUpdateFunc is used to exchange intermediate calculation results.
type FloatRateUpdateFunc func(ft1, ft2, lt1, lt2 int64, fv1, fv2, lv1, lv2 float64) (int64, int64, float64, float64)

// floatRateReducer can be used as the base for calculating the rate/irate/delta/idelta/increase prom function.
type floatRateReducer struct {
	fr         FloatRateReduceFunc
	fm         FloatRateMergeFunc
	fu         FloatRateUpdateFunc
	prevStep   int64          // record the last step of the previous batch.
	prevPoints [2]*floatPoint // save the two points of the last calculation. For example, it indicates the first and last time points for rate.
	ringBuf    *floatBuffer   // store the last or next group of data values after each batch.
}

func newFloatRateReducer(
	fr FloatRateReduceFunc,
	fm FloatRateMergeFunc,
	fu FloatRateUpdateFunc,
) *floatRateReducer {
	return &floatRateReducer{
		fr:         fr,
		fm:         fm,
		fu:         fu,
		prevPoints: [2]*floatPoint{newFloatPoint(), newFloatPoint()},
		ringBuf:    newFloatBuffer(),
	}
}

func (r *floatRateReducer) Aggregate(p *ReducerEndpoint, param *ReducerParams) {
	var rangeEnd, rangeStart int64
	var start, end int
	inRecord, outRecord := p.InputPoint.Record, p.OutputPoint.Record
	inOrdinal, outOrdinal := p.InputPoint.Ordinal, p.OutputPoint.Ordinal
	times, values := inRecord.Times(), inRecord.ColVals[inOrdinal].FloatValues()
	numStep := len(param.intervalIndex) / 2
	firstIndex, lastIndex, bufCount := 0, numStep-1, len(r.ringBuf.times)
	for i := 0; i < numStep; i++ {
		rangeEnd = param.firstStep + int64(i)*param.step
		rangeStart = rangeEnd - param.rangeDuration
		start, end = int(param.intervalIndex[2*i]), int(param.intervalIndex[2*i+1])
		ft, lt, fv, lv, isNil := r.fr(times, values, start, end)
		bufNil := bufCount == 0 || r.ringBuf.times[bufCount-1] < rangeStart || (i == firstIndex && !r.prevPoints[0].isNil && !r.prevPoints[1].isNil)
		if isNil && bufNil {
			continue
		}
		var count int
		if !isNil && bufNil {
			count = end - start
		} else if isNil && !bufNil {
			r.ringBuf.updateIndex(rangeStart, rangeEnd)
			ft, lt, fv, lv, _ = r.fr(r.ringBuf.times, r.ringBuf.values, r.ringBuf.s, r.ringBuf.e)
			count = r.ringBuf.e - r.ringBuf.s
		} else {
			r.ringBuf.updateIndex(rangeStart, rangeEnd)
			ft1, lt1, fv1, lv1, _ := r.fr(r.ringBuf.times, r.ringBuf.values, r.ringBuf.s, r.ringBuf.e)
			ft, lt, fv, lv = r.fu(ft1, ft, lt1, lt, fv1, fv, lv1, lv)
			count = end - start + r.ringBuf.e - r.ringBuf.s
		}
		if i == firstIndex && (!r.prevPoints[0].isNil || !r.prevPoints[1].isNil) {
			r.prevPoints[0].time, r.prevPoints[1].time, r.prevPoints[0].value, r.prevPoints[1].value = r.fu(
				r.prevPoints[0].time, ft, r.prevPoints[1].time, lt, r.prevPoints[0].value, fv, r.prevPoints[1].value, lv,
			)
			r.prevPoints[0].count += count
			r.prevPoints[1].count += count
			r.doFirstWindow(outRecord, outOrdinal, param, rangeEnd, numStep == 1)
		} else if i == lastIndex && param.sameWindow {
			r.doLastWindow(ft, lt, fv, lv, count)
		} else {
			if param.step > 0 && i == firstIndex && bufCount > 0 {
				r.populateByPrevious(outRecord, outOrdinal, param)
			}
			r.doMiddleWindow(outRecord, outOrdinal, param, rangeEnd, ft, lt, fv, lv, count)
		}
	}
	r.prevStep = rangeEnd
	if param.lastRec {
		if param.step == 0 {
			return
		}
		nextStep := rangeEnd + param.step
		if nextStep > param.lastStep {
			return
		}
		r.populateByLast(outRecord, outOrdinal, param, times, values, nextStep, start)
	} else {
		r.ringBuf.updateValue(times, values, param, rangeEnd, start)
	}
}

func (r *floatRateReducer) doFirstWindow(ourRec *record.Record, outOrdinal int, param *ReducerParams, ts int64, onlyOneWindow bool) {
	if !onlyOneWindow || !param.sameWindow {
		if !r.prevPoints[0].isNil && !r.prevPoints[1].isNil {
			v, isNil := r.fm(r.prevPoints[0].time, r.prevPoints[1].time, r.prevPoints[0].value, r.prevPoints[1].value, ts, r.prevPoints[0].count, param)
			if !isNil {
				ourRec.ColVals[outOrdinal].AppendFloat(v)
				ourRec.AppendTime(ts + param.offset)
			}
		}
		r.prevPoints[0].Reset()
		r.prevPoints[1].Reset()
	}
}

func (r *floatRateReducer) doLastWindow(ft, lt int64, fv, lv float64, count int) {
	r.prevPoints[0].Set(ft, fv, count)
	r.prevPoints[1].Set(lt, lv, count)
}

func (r *floatRateReducer) doMiddleWindow(ourRec *record.Record, outOrdinal int, param *ReducerParams, ts, ft, lt int64, fv, lv float64, count int) {
	v, isNil := r.fm(ft, lt, fv, lv, ts, count, param)
	if !isNil {
		ourRec.ColVals[outOrdinal].AppendFloat(v)
		ourRec.AppendTime(ts + param.offset)
	}
}

func (r *floatRateReducer) populateByPrevious(ourRec *record.Record, outOrdinal int, param *ReducerParams) {
	var m, n int
	nextStep, lastStep := r.prevStep+param.step, param.firstStep-param.step
	bufCount := len(r.ringBuf.times)
	for te := nextStep; te <= lastStep; te += param.step {
		ts := te - param.rangeDuration
		for m < bufCount {
			if r.ringBuf.times[m] >= ts {
				break
			}
			m++
		}
		for n < bufCount {
			if r.ringBuf.times[n] > te {
				break
			}
			n++
		}
		if n-m >= 2 {
			r.doMiddleWindow(ourRec, outOrdinal, param, te, r.ringBuf.times[m], r.ringBuf.times[n-1], r.ringBuf.values[m], r.ringBuf.values[n-1], n-m)
		} else {
			break
		}
	}
}

func (r *floatRateReducer) populateByLast(outRecord *record.Record, outOrdinal int, param *ReducerParams, times []int64, values []float64, nextStep int64, start int) {
	bufCount := len(r.ringBuf.times)
	rowNum := len(times)
	for rangeEnd := nextStep; rangeEnd <= param.lastStep; rangeEnd += param.step {
		rangeStart := rangeEnd - param.rangeDuration
		bufNil := start > 0 || (bufCount == 0 || r.ringBuf.times[bufCount-1] < rangeStart)
		if bufNil || r.ringBuf.s >= bufCount {
			for start < len(times) {
				if times[start] >= rangeStart {
					r.doMiddleWindow(outRecord, outOrdinal, param, rangeEnd, times[start], times[rowNum-1], values[start], values[rowNum-1], rowNum-start)
					break
				}
				start++
			}
			continue
		}
		for r.ringBuf.s < bufCount {
			if r.ringBuf.times[r.ringBuf.s] >= rangeStart {
				break
			}
			r.ringBuf.s++
		}
		if r.ringBuf.s < bufCount {
			r.doMiddleWindow(outRecord, outOrdinal, param, rangeEnd, r.ringBuf.times[r.ringBuf.s], times[rowNum-1], r.ringBuf.values[r.ringBuf.s], values[rowNum-1], rowNum+bufCount-r.ringBuf.s)
		}
	}
}

type FloatSliceReduceFunc func(times []int64, values []float64, start, end int) ([]int64, []float64, bool)

type FloatSliceMergeFunc func(prevT, currT []int64, prevV, currV []float64, ts int64, count int, param *ReducerParams) (float64, bool)

type floatSliceReducer struct {
	fr         FloatSliceReduceFunc
	fm         FloatSliceMergeFunc
	prevStep   int64 // record the last step of the previous batch.
	prevPoints []*floatPoint
	ringBuf    *floatBuffer // store the last or next group of data values after each batch.
}

func newFloatSliceReducer(
	fr FloatSliceReduceFunc,
	fm FloatSliceMergeFunc,
) *floatSliceReducer {
	return &floatSliceReducer{
		fr:         fr,
		fm:         fm,
		prevPoints: []*floatPoint{},
		ringBuf:    newFloatBuffer(),
	}
}

func (r *floatSliceReducer) Aggregate(p *ReducerEndpoint, param *ReducerParams) {
	var rangeEnd, rangeStart int64
	var start, end int
	inRecord, outRecord := p.InputPoint.Record, p.OutputPoint.Record
	inOrdinal, outOrdinal := p.InputPoint.Ordinal, p.OutputPoint.Ordinal
	times, values := inRecord.Times(), inRecord.ColVals[inOrdinal].FloatValues()
	numStep := len(param.intervalIndex) / 2
	firstIndex, lastIndex, bufCount := 0, numStep-1, len(r.ringBuf.times)
	for i := 0; i < numStep; i++ {
		rangeEnd = param.firstStep + int64(i)*param.step
		rangeStart = rangeEnd - param.rangeDuration
		start, end = int(param.intervalIndex[2*i]), int(param.intervalIndex[2*i+1])
		curTimes, curValues, isNil := r.fr(times, values, start, end)
		var bufTimes []int64
		var bufValues []float64
		bufNil := bufCount == 0 || r.ringBuf.times[bufCount-1] < rangeStart
		if isNil && bufNil {
			continue
		}
		var count int
		if !isNil && bufNil {
			count = end - start
		} else {
			r.ringBuf.updateIndex(rangeStart, rangeEnd)
			bufTimes, bufValues, _ = r.fr(r.ringBuf.times, r.ringBuf.values, r.ringBuf.s, r.ringBuf.e)
			count = len(curTimes) + len(bufTimes)
		}
		if !(i == lastIndex && param.sameWindow) {
			if param.step > 0 && i == firstIndex && bufCount > 0 {
				r.populateByPrevious(outRecord, outOrdinal, param)
			}
			r.doMiddleWindow(outRecord, outOrdinal, param, rangeEnd, bufTimes, curTimes, bufValues, curValues, count)
		}
	}
	r.prevStep = rangeEnd
	if param.lastRec {
		if param.step == 0 {
			return
		}
		nextStep := rangeEnd + param.step
		if nextStep > param.lastStep {
			return
		}
		r.populateByLast(outRecord, outOrdinal, param, times, values, nextStep, start)
	} else {
		r.ringBuf.updateValue(times, values, param, rangeEnd, start)
	}
}

func (r *floatSliceReducer) populateByPrevious(outRecord *record.Record, outOrdinal int, param *ReducerParams) {
	var m, n int
	nextStep, lastStep := r.prevStep+param.step, param.firstStep-param.step
	bufCount := len(r.ringBuf.times)
	for te := nextStep; te <= lastStep; te += param.step {
		ts := te - param.rangeDuration
		for m < bufCount {
			if r.ringBuf.times[m] >= ts {
				break
			}
			m++
		}
		for n < bufCount {
			if r.ringBuf.times[n] > te {
				break
			}
			n++
		}
		if n-m >= 2 {
			r.doMiddleWindow(outRecord, outOrdinal, param, te, r.ringBuf.times[m:n], []int64{}, r.ringBuf.values[m:n], []float64{}, n-m)
		} else {
			break
		}
	}
}

func (r *floatSliceReducer) populateByLast(outRecord *record.Record, outOrdinal int, param *ReducerParams, times []int64, values []float64, nextStep int64, start int) {
	bufCount := len(r.ringBuf.times)
	rowNum := len(times)
	for rangeEnd := nextStep; rangeEnd <= param.lastStep; rangeEnd += param.step {
		rangeStart := rangeEnd - param.rangeDuration
		bufNil := start > 0 || bufCount == 0 || r.ringBuf.times[bufCount-1] < rangeStart
		if bufNil || r.ringBuf.s >= int(rangeStart) {
			for start < len(times) {
				if times[start] >= rangeStart {
					r.doMiddleWindow(outRecord, outOrdinal, param, rangeEnd, times[start:rowNum], []int64{}, values[start:rowNum], []float64{}, rowNum-start)
					break
				}
				start++
			}
			continue
		}
		for r.ringBuf.s < bufCount {
			if r.ringBuf.times[r.ringBuf.s] >= rangeStart {
				break
			}
			r.ringBuf.s++
		}
		if r.ringBuf.s < bufCount {
			r.doMiddleWindow(outRecord, outOrdinal, param, rangeEnd, r.ringBuf.times[r.ringBuf.s:rowNum], []int64{}, r.ringBuf.values[r.ringBuf.s:rowNum], []float64{}, rowNum+bufCount-r.ringBuf.s)
		}
	}
}

func (r *floatSliceReducer) doMiddleWindow(ourRec *record.Record, outOrdinal int, param *ReducerParams, ts int64, prevT, currT []int64, prevV, currV []float64, count int) {
	v, isNil := r.fm(prevT, currT, prevV, currV, ts, count, param)
	if !isNil {
		ourRec.ColVals[outOrdinal].AppendFloat(v)
		ourRec.AppendTime(ts + param.offset)
	}
}

// FloatIncAggReduceFunc is used to process intermediate calculation results.
type FloatIncAggReduceFunc func(times []int64, values []float64, start, end int) (int64, float64, bool)

// FloatIncAggMergeFunc is used to calculate the final result based on the intermediate result and function definition.
type FloatIncAggMergeFunc func(prevValue float64, currValue float64, prevCount, currCount int) (float64, int)

// floatIncAggReducer can be used as the base for calculating the {sum/count/min/max/avg/absent/last/stddev/stdvar}_over_time prom function.
type floatIncAggReducer struct {
	fr        FloatIncAggReduceFunc
	fm        FloatIncAggMergeFunc
	prevStep  int64
	offset    int64
	prevPoint *floatPoint
	ringBuf   *floatBuffer
}

func newFloatIncReducer(
	fr FloatIncAggReduceFunc,
	fm FloatIncAggMergeFunc,
) *floatIncAggReducer {
	return &floatIncAggReducer{
		fr:        fr,
		fm:        fm,
		prevPoint: newFloatPoint(),
		ringBuf:   newFloatBuffer(),
	}
}

func (r *floatIncAggReducer) Aggregate(p *ReducerEndpoint, param *ReducerParams) {
	var rangeEnd, rangeStart int64
	var start, end int
	r.offset = param.offset
	inRecord, outRecord := p.InputPoint.Record, p.OutputPoint.Record
	inOrdinal, outOrdinal := p.InputPoint.Ordinal, p.OutputPoint.Ordinal
	times, values := inRecord.Times(), inRecord.ColVals[inOrdinal].FloatValues()
	numStep := len(param.intervalIndex) / 2
	firstIndex, lastIndex, bufCount := 0, numStep-1, len(r.ringBuf.times)
	for i := 0; i < numStep; i++ {
		rangeEnd = param.firstStep + int64(i)*param.step
		rangeStart = rangeEnd - param.rangeDuration
		start, end = int(param.intervalIndex[2*i]), int(param.intervalIndex[2*i+1])
		t, v, isNil := r.fr(times, values, start, end)
		bufNil := bufCount == 0 || r.ringBuf.times[bufCount-1] < rangeStart || (i == firstIndex && !r.prevPoint.isNil)
		if isNil && bufNil {
			continue
		}
		var count int
		if !isNil && bufNil {
			count = end - start
		} else if isNil && !bufNil {
			r.ringBuf.updateIndex(rangeStart, rangeEnd)
			t, v, _ = r.fr(r.ringBuf.times, r.ringBuf.values, r.ringBuf.s, r.ringBuf.e)
			count = r.ringBuf.e - r.ringBuf.s
		} else {
			r.ringBuf.updateIndex(rangeStart, rangeEnd)
			_, v1, _ := r.fr(r.ringBuf.times, r.ringBuf.values, r.ringBuf.s, r.ringBuf.e)
			v, count = r.fm(v1, v, r.ringBuf.e-r.ringBuf.s, end-start)
		}
		if i == firstIndex && !r.prevPoint.isNil {
			v, count = r.fm(r.prevPoint.value, v, r.prevPoint.count, count)
			r.prevPoint.value, r.prevPoint.count = v, count
			r.doFirstWindow(outRecord, outOrdinal, param, rangeEnd, numStep == 1)
		} else if i == lastIndex && param.sameWindow {
			r.doLastWindow(t, v, count)
		} else {
			if param.step > 0 && i == firstIndex && bufCount > 0 {
				r.populateByPrevious(outRecord, outOrdinal, param)
			}
			r.doMiddleWindow(outRecord, outOrdinal, rangeEnd, v)
		}
	}
	r.prevStep = rangeEnd
	if param.lastRec {
		if param.step == 0 {
			return
		}
		nextStep := rangeEnd + param.step
		if nextStep > param.lastStep {
			return
		}
		r.populateByLast(outRecord, outOrdinal, param, times, values, nextStep, start)
	} else {
		r.ringBuf.updateValue(times, values, param, rangeEnd, start)
	}
}

func (r *floatIncAggReducer) doFirstWindow(ourRec *record.Record, outOrdinal int, param *ReducerParams, ts int64, onlyOneWindow bool) {
	if !onlyOneWindow || !param.sameWindow {
		if !r.prevPoint.isNil {
			ourRec.ColVals[outOrdinal].AppendFloat(r.prevPoint.value)
			ourRec.AppendTime(ts + param.offset)
		}
		r.prevPoint.Reset()
	}
}

func (r *floatIncAggReducer) doLastWindow(t int64, v float64, count int) {
	r.prevPoint.Set(t, v, count)
}

func (r *floatIncAggReducer) doMiddleWindow(ourRec *record.Record, outOrdinal int, ts int64, v float64) {
	ourRec.ColVals[outOrdinal].AppendFloat(v)
	ourRec.AppendTime(ts + r.offset)
}

func (r *floatIncAggReducer) populateByPrevious(ourRec *record.Record, outOrdinal int, param *ReducerParams) {
	var m, n int
	nextStep, lastStep := r.prevStep+param.step, param.firstStep-param.step
	bufCount := len(r.ringBuf.times)
	for te := nextStep; te <= lastStep; te += param.step {
		ts := te - param.rangeDuration
		for m < bufCount {
			if r.ringBuf.times[m] >= ts {
				break
			}
			m++
		}
		for n < bufCount {
			if r.ringBuf.times[n] > te {
				break
			}
			n++
		}
		if n > m {
			_, v, isNil := r.fr(r.ringBuf.times, r.ringBuf.values, m, n)
			if !isNil {
				r.doMiddleWindow(ourRec, outOrdinal, te, v)
			}
		} else {
			break
		}
	}
}

func (r *floatIncAggReducer) populateByLast(outRecord *record.Record, outOrdinal int, param *ReducerParams, times []int64, values []float64, nextStep int64, start int) {
	bufCount := len(r.ringBuf.times)
	rowNum := len(times)
	for rangeEnd := nextStep; rangeEnd <= param.lastStep; rangeEnd += param.step {
		rangeStart := rangeEnd - param.rangeDuration
		bufNil := start > 0 || (bufCount == 0 || r.ringBuf.times[bufCount-1] < rangeStart)
		if bufNil || r.ringBuf.s >= bufCount {
			for start < len(times) {
				if times[start] >= rangeStart {
					_, v, isNil := r.fr(times, values, start, rowNum)
					if !isNil {
						r.doMiddleWindow(outRecord, outOrdinal, rangeEnd, v)
					}
					break
				}
				start++
			}
			continue
		}
		for r.ringBuf.s < bufCount {
			if r.ringBuf.times[r.ringBuf.s] >= rangeStart {
				break
			}
			r.ringBuf.s++
		}
		if r.ringBuf.s < bufCount {
			_, v, isNil := r.fr(r.ringBuf.times, r.ringBuf.values, r.ringBuf.s, bufCount)
			if isNil {
				_, v, isNil = r.fr(times, values, 0, rowNum)
				if !isNil {
					r.doMiddleWindow(outRecord, outOrdinal, rangeEnd, v)
				}
			} else {
				_, v1, _ := r.fr(times, values, 0, rowNum)
				v, _ = r.fm(v, v1, bufCount-r.ringBuf.s, rowNum)
				r.doMiddleWindow(outRecord, outOrdinal, rangeEnd, v)
			}
		}
	}
}

func kahanSumInc(inc, sum, c float64) (newSum, newC float64) {
	t := sum + inc
	// Using Neumaier improvement, swap if next term larger than sum.
	if math.Abs(sum) >= math.Abs(inc) {
		c += (sum - t) + inc
	} else {
		c += (inc - t) + sum
	}
	return t, c
}
