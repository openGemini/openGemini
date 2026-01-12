// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package engine

import (
	"errors"
	"fmt"
	"math"

	"github.com/openGemini/openGemini/engine/executor"
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
	RegistryPromFunction("last_over_time_prom", &lastOp{})
	RegistryPromFunction("increase", &increaseOp{})
	RegistryPromFunction("deriv", &derivOp{})
	RegistryPromFunction("predict_linear", &predictLinearOp{})
	RegistryPromFunction("delta_prom", &deltaOp{})
	RegistryPromFunction("idelta_prom", &ideltaOp{})
	RegistryPromFunction("stdvar_over_time_prom", &stdVarOverTime{})
	RegistryPromFunction("stddev_over_time_prom", &stdDevOverTime{})
	RegistryPromFunction("present_over_time_prom", &intervalExistMark{})
	RegistryPromFunction("holt_winters_prom", &holtWintersOp{})
	RegistryPromFunction("changes_prom", &changesOp{})
	RegistryPromFunction("quantile_over_time_prom", &quantileOverTime{})
	RegistryPromFunction("resets_prom", &resetsOp{})
	RegistryPromFunction("absent_over_time_prom", &intervalExistMark{})
	RegistryPromFunction("mad_over_time_prom", &madOverTimeOp{})
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

// rate
type rateOp struct{}

func (o *rateOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatPromRateReduce, floatPromRateMerge(true, true)), p.inOrdinal, p.outOrdinal), nil
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
		firstTime, lastTime, firstValue, _, reduceResult := executor.CalcReduceResult(prevT, currT, prevV, currV, isCounter)
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

		if durationToStart >= extrapolationThreshold {
			durationToStart = averageDurationBetweenSamples / 2
		}
		extrapolateToInterval += durationToStart

		if durationToEnd >= extrapolationThreshold {
			durationToEnd = averageDurationBetweenSamples / 2
		}
		extrapolateToInterval += durationToEnd

		resultValue := reduceResult * (extrapolateToInterval / sampledInterval)
		if isRate {
			resultValue = resultValue / float64(param.rangeDuration/1e9)
		}
		return resultValue, false
	}
}

// irate
type irateOp struct{}

func (o *irateOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatRateReducer(floatIRateReduce, floatIRateMerge(true), floatIRateUpdate), p.inOrdinal, p.outOrdinal), nil
}

// avg_over_time
type avgOp struct{}

func (o *avgOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatAvgReduce, floatAvgMergeFunc), p.inOrdinal, p.outOrdinal), nil
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
		mean, c = executor.KahanSumInc(values[i]/count-mean/count, mean, c)
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

// count_over_time
type countOp struct{}

func (o *countOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatPromCountReduce, floatPromCountMergeFunc), p.inOrdinal, p.outOrdinal), nil
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

// sum_over_time
type sumOp struct{}

func (o *sumOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatPromSumReduce, floatPromSumMergeFunc), p.inOrdinal, p.outOrdinal), nil
}

func floatPromSumReduce(times []int64, values []float64, start, end int) (int64, float64, bool) {
	if start == end {
		return 0, 0, true
	}
	var sum, c float64
	for i := start; i < end; i++ {
		sum, c = executor.KahanSumInc(values[i], sum, c)
	}
	if math.IsInf(sum, 0) {
		return times[start], sum, false
	}
	return times[start], sum + c, false
}

func floatPromSumMergeFunc(prevValue float64, currValue float64, prevCount, currCount int) (float64, int) {
	return prevValue + currValue, prevCount + currCount
}

// min_over_time
type minOp struct{}

func (o *minOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatPromMinReduce, floatPromMinMergeFunc), p.inOrdinal, p.outOrdinal), nil
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

// max_over_time
type maxOp struct{}

func (o *maxOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatPromMaxReduce, floatPromMaxMergeFunc), p.inOrdinal, p.outOrdinal), nil
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

// last_over_time
type lastOp struct{}

func (o *lastOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatIncReducer(floatPromLastReduce, floatPromLastMergeFunc), p.inOrdinal, p.outOrdinal), nil
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

// increase
type increaseOp struct{}

func (o *increaseOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatPromRateReduce, floatPromRateMerge(false, true)), p.inOrdinal, p.outOrdinal), nil
}

// deriv
type derivOp struct{}

func (o *derivOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatPromDerivReduce, linearMergeFunc(true, 0)), p.inOrdinal, p.outOrdinal), nil
}

func floatPromDerivReduce(times []int64, values []float64, start, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func linearMergeFunc(isDeriv bool, scalar float64) FloatSliceMergeFunc {
	return func(t1, t2 []int64, v1, v2 []float64, ts int64, pointCount int, param *ReducerParams) (float64, bool) {
		if pointCount <= 1 {
			return 0, true
		}
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
		interceptTime := ts + param.offset
		reduce := func(times []int64, values []float64) {
			for i, v := range values {
				index++
				// Set constY to false if any new y values are encountered.
				if constY && index > 0 && v != fv {
					constY = false
				}
				n += 1.0
				x := float64(times[i]-interceptTime) / 1e9
				sumX, cX = executor.KahanSumInc(x, sumX, cX)
				sumY, cY = executor.KahanSumInc(v, sumY, cY)
				sumXY, cXY = executor.KahanSumInc(x*v, sumXY, cXY)
				sumX2, cX2 = executor.KahanSumInc(x*x, sumX2, cX2)
			}

		}
		reduce(t1, v1)
		reduce(t2, v2)

		if constY {
			if math.IsInf(fv, 0) {
				return math.NaN(), false
			}
			if isDeriv {
				return 0, false
			}
			return fv, false
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

// predictLinear
type predictLinearOp struct{}

func (o *predictLinearOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
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

// delta
type deltaOp struct{}

func (o *deltaOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatPromRateReduce, floatPromRateMerge(false, false)), p.inOrdinal, p.outOrdinal), nil
}

// idelta
type ideltaOp struct{}

func (o *ideltaOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatRateReducer(floatIRateReduce, floatIRateMerge(false), floatIRateUpdate), p.inOrdinal, p.outOrdinal), nil
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

func floatIRateMerge(isRate bool) FloatRateMergeFunc {
	return func(prevTime int64, lastTime int64, prevValue float64, lastValue float64,
		ts int64, pointCount int, param *ReducerParams) (float64, bool) {
		if lastTime == prevTime || param.rangeDuration == 0 || pointCount < 2 {
			return 0, true
		}

		var resultValue float64
		if isRate && lastValue < prevValue {
			resultValue = lastValue
		} else {
			resultValue = lastValue - prevValue
		}

		sampledInterval := lastTime - prevTime
		if sampledInterval == 0 {
			// Avoid dividing by 0.
			return 0, true
		}

		if isRate {
			// Convert to per-second.
			resultValue /= float64(sampledInterval) / 1e9
		}

		return resultValue, false
	}

}

func floatIRateUpdate(ft1, ft2, lt1, lt2 int64, fv1, fv2, lv1, lv2 float64) (int64, int64, float64, float64) {
	if ft2 < lt2 {
		return ft2, lt2, fv2, lv2
	}
	return lt1, lt2, lv1, lv2
}

// stdvar_over_time
type stdVarOverTime struct{}

func (r *stdVarOverTime) CreateRoutine(param *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatStdVarOverTimeReducer, floatStdVarOverTimeMerger(false)), param.inOrdinal, param.outOrdinal), nil
}

func floatStdVarOverTimeReducer(times []int64, values []float64, start int, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatStdVarOverTimeMerger(isStdDev bool) FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c < 1 {
			return 0, true
		}
		var count float64
		var mean, cMean float64
		var aux, cAux float64

		if len(prevV) == 0 && len(curV) == 0 {
			return 0.0, true
		}

		if len(prevV) != 0 {
			for _, f := range prevV {
				count++
				delta := f - (mean + cMean)
				mean, cMean = executor.KahanSumInc(delta/count, mean, cMean)
				aux, cAux = executor.KahanSumInc(delta*(f-(mean+cMean)), aux, cAux)
			}
		}

		if len(curV) != 0 {
			for _, f := range curV {
				count++
				delta := f - (mean + cMean)
				mean, cMean = executor.KahanSumInc(delta/count, mean, cMean)
				aux, cAux = executor.KahanSumInc(delta*(f-(mean+cMean)), aux, cAux)
			}
		}
		stdVar := (aux + cAux) / count
		if isStdDev {
			return math.Sqrt(stdVar), false
		}
		return stdVar, false
	}
}

// stddev_over_time
type stdDevOverTime struct{}

func (r *stdDevOverTime) CreateRoutine(param *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatStdVarOverTimeReducer, floatStdVarOverTimeMerger(true)), param.inOrdinal, param.outOrdinal), nil
}

// present_over_time
type intervalExistMark struct{}

func (o *intervalExistMark) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatPresentOverTimeReduce, floatPresentOverTimeMerge()), p.inOrdinal, p.outOrdinal), nil
}

func floatPresentOverTimeReduce(times []int64, values []float64, start, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatPresentOverTimeMerge() FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c < 1 {
			return 0, true
		}
		return 1, false
	}
}

// holt_winters
type holtWintersOp struct{}

func (r *holtWintersOp) CreateRoutine(param *PromFuncParam) (Routine, error) {
	// The smoothing factor argument.
	var sf float64
	// The trend factor argument.
	switch arg := param.args[1].(type) {
	case *influxql.NumberLiteral:
		sf = arg.Val
	case *influxql.IntegerLiteral:
		sf = float64(arg.Val)
	default:
		return nil, errors.New("the type of input args of holtWinters sf is unsupported")
	}
	var tf float64
	switch arg2 := param.args[2].(type) {
	case *influxql.NumberLiteral:
		tf = arg2.Val
	case *influxql.IntegerLiteral:
		tf = float64(arg2.Val)
	default:
		return nil, errors.New("the type of input args of holtWinters tf is unsupported")
	}

	return NewRoutineImpl(newFloatSliceReducer(floatHoltWintersReducer, floatHoltWintersMerger(sf, tf)), param.inOrdinal, param.outOrdinal), nil
}

func floatHoltWintersReducer(times []int64, values []float64, start int, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatHoltWintersMerger(sf float64, tf float64) FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c < 2 {
			return math.NaN(), true
		}
		return executor.CalcHoltWinters(prevV, curV, sf, tf), false
	}
}

// changes
type changesOp struct{}

func (r *changesOp) CreateRoutine(param *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatChangesReducer, floatChangesMerger()), param.inOrdinal, param.outOrdinal), nil
}

func floatChangesReducer(times []int64, values []float64, start int, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatChangesMerger() FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c < 1 {
			return math.NaN(), true
		}
		return executor.CalcChange(prevV, curV), false
	}
}

// resets
type resetsOp struct{}

func (r *resetsOp) CreateRoutine(param *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatResetsReducer, floatResetsMerger()), param.inOrdinal, param.outOrdinal), nil
}

func floatResetsReducer(times []int64, values []float64, start int, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatResetsMerger() FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		return executor.CalcResets(prevV, curV), false
	}
}

// quantile_over_time
type quantileOverTime struct{}

func (r *quantileOverTime) CreateRoutine(param *PromFuncParam) (Routine, error) {
	var percentile float64
	switch arg := param.args[1].(type) {
	case *influxql.NumberLiteral:
		percentile = arg.Val
	case *influxql.IntegerLiteral:
		percentile = float64(arg.Val)
	default:
		return nil, errors.New("the type of input args of quantile_prom iterator is unsupported")
	}
	return NewRoutineImpl(newFloatSliceReducer(floatStdVarOverTimeReducer, floatQuantileOverTimeMerger(percentile)), param.inOrdinal, param.outOrdinal), nil
}

func floatQuantileOverTimeMerger(percentile float64) FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c == 0 || len(prevT)+len(currT) == 0 || len(prevV)+len(curV) == 0 {
			return math.NaN(), true
		}
		return executor.CalcQuantile2(percentile, prevV, curV), false
	}
}

// mad_over_time
type madOverTimeOp struct{}

func (r *madOverTimeOp) CreateRoutine(param *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatMadOverTimeReducer, floatMadOverTimeMerger()), param.inOrdinal, param.outOrdinal), nil
}

func floatMadOverTimeReducer(times []int64, values []float64, start int, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatMadOverTimeMerger() FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c == 0 || len(prevT)+len(currT) == 0 || len(prevV)+len(curV) == 0 {
			return math.NaN(), true
		}
		return executor.CalcMad2(prevV, curV), false
	}
}
