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
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/record"
)

type floatBuffer struct {
	times  []int64
	values []float64
	s, e   int
}

func (b *floatBuffer) reset() {
	b.times = b.times[:0]
	b.values = b.values[:0]
	b.s = 0
	b.e = 0
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

	for b.s < len(b.times) {
		if b.times[b.s] >= rangeStart {
			b.times = append(b.times[b.s:], times...)
			b.values = append(b.values[b.s:], values...)
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

// slice reducer
type FloatSliceReduceFunc func(times []int64, values []float64, start, end int) ([]int64, []float64, bool)

type FloatSliceMergeFunc func(prevT, currT []int64, prevV, currV []float64, ts int64, count int, param *ReducerParams) (float64, bool)

type floatSliceReducer struct {
	fr       FloatSliceReduceFunc
	fm       FloatSliceMergeFunc
	prevStep int64        // record the last step of the previous batch.
	ringBuf  *floatBuffer // store the last or next group of data values after each batch.
}

func newFloatSliceReducer(
	fr FloatSliceReduceFunc,
	fm FloatSliceMergeFunc,
) *floatSliceReducer {
	return &floatSliceReducer{
		fr:      fr,
		fm:      fm,
		ringBuf: newFloatBuffer(),
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
		defer r.reset()
		if param.step == 0 || len(param.intervalIndex) == 0 {
			return
		}
		nextStep := rangeEnd + param.step
		if nextStep > param.lastStep {
			return
		}
		r.populateByLast(outRecord, outOrdinal, param, times, values, nextStep, start)
	} else {
		if len(param.intervalIndex) == 0 {
			return
		}
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
		r.doMiddleWindow(outRecord, outOrdinal, param, te, r.ringBuf.times[m:n], []int64{}, r.ringBuf.values[m:n], []float64{}, n-m)
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
					r.doMiddleWindow(outRecord, outOrdinal, param, rangeEnd, []int64{}, times[start:rowNum], []float64{}, values[start:rowNum], rowNum-start)
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
			r.doMiddleWindow(outRecord, outOrdinal, param, rangeEnd, r.ringBuf.times[r.ringBuf.s:bufCount], times[:rowNum], r.ringBuf.values[r.ringBuf.s:bufCount], values[:rowNum], rowNum+bufCount-r.ringBuf.s)
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

func (r *floatSliceReducer) reset() {
	r.prevStep = 0
	r.ringBuf.reset()
}

// incAgg Reducer
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
		defer r.reset()
		if param.step == 0 {
			return
		}
		nextStep := rangeEnd + param.step
		if nextStep > param.lastStep {
			return
		}
		r.populateByLast(outRecord, outOrdinal, param, times, values, nextStep, start)
	} else {
		if len(param.intervalIndex) == 0 {
			return
		}
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

func (r *floatIncAggReducer) reset() {
	r.offset = 0
	r.prevStep = 0
	r.ringBuf.reset()
	r.prevPoint.Reset()
}

// rate reducer
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
		defer r.reset()
		if param.step == 0 || len(param.intervalIndex) == 0 {
			return
		}
		nextStep := rangeEnd + param.step
		if nextStep > param.lastStep {
			return
		}
		r.populateByLast(outRecord, outOrdinal, param, times, values, nextStep, start)
	} else {
		if len(param.intervalIndex) == 0 {
			return
		}
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
			ft, lt, fv, lv, isNil := r.fr(r.ringBuf.times, r.ringBuf.values, m, n)
			if !isNil {
				r.doMiddleWindow(ourRec, outOrdinal, param, te, ft, lt, fv, lv, n-m)
			}
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
					ft, lt, fv, lv, isNil := r.fr(times, values, start, rowNum)
					if !isNil {
						r.doMiddleWindow(outRecord, outOrdinal, param, rangeEnd, ft, lt, fv, lv, rowNum-start)
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
		if r.ringBuf.s < bufCount && rowNum+bufCount-r.ringBuf.s >= 2 {
			ft, lt, fv, lv := executor.CalcLastTwoPoints(r.ringBuf.times, times, r.ringBuf.values, values)
			r.doMiddleWindow(outRecord, outOrdinal, param, rangeEnd, ft, lt, fv, lv, rowNum+bufCount-r.ringBuf.s)
		}
	}
}

func (r *floatRateReducer) reset() {
	r.prevStep = 0
	r.prevPoints[0].Reset()
	r.prevPoints[1].Reset()
	r.ringBuf.reset()
}
