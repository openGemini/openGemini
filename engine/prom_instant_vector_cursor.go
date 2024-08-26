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

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// InstantVectorCursor is used to sample and process data for prom instant_query or range_query.
// Sampling is the first step of original data processing for non-aggregated query and aggregated  query.
// For non-aggregated query, place InstantVectorCursor after seriesCursor.
// For aggregated query, replace aggregateCursor with the InstantVectorCursor.
// Give an example:
// data: (time, value)=> [(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0), (6, 6.0)]
// start=1, end=6, offset=0, step=2, LookUpDelta=3 => startSample=1, endSample=5
// sample data: (time, value)=> [(1, 1.0), (3, 3.0), (5, 4.0)]
type InstantVectorCursor struct {
	aggregateCursor

	lookUpDelta int64
	start       int64 // query start time.
	end         int64 // query end time.
	offset      int64 // query offset time.
	step        int64 // query resolution step width in duration format or float number of seconds.
	startSample int64 // start time for prom sampling
	endSample   int64 // end time for prom sampling
	firstStep   int64 // first step of each record for prom sampling
}

func NewInstantVectorCursor(input comm.KeyCursor, schema *executor.QuerySchema, globalPool *record.RecordPool, tr util.TimeRange) *InstantVectorCursor {
	c := &InstantVectorCursor{}
	c.aggregateCursor = *NewAggregateCursor(input, schema, globalPool, false)
	c.aggregateCursor.r = c

	c.lookUpDelta = schema.Options().GetPromLookBackDelta().Nanoseconds()
	c.step = schema.Options().GetPromStep().Nanoseconds()
	c.start = schema.Options().GetStartTime()
	c.end = schema.Options().GetEndTime()
	c.offset = schema.Options().GetPromQueryOffset().Nanoseconds()
	c.startSample = c.start + c.lookUpDelta
	if c.step == 0 {
		c.endSample = c.startSample
		c.firstStep = c.startSample
		c.reducerParams.lastStep = c.endSample
		c.reducerParams.rangeDuration = schema.Options().GetPromRange().Nanoseconds()
	} else {
		c.endSample = c.start + c.lookUpDelta + (c.end-(c.start+c.lookUpDelta))/c.step*c.step
		c.firstStep = getCurrStep(c.startSample, c.endSample, c.step, tr.Min)
		c.reducerParams.lastStep = getPrevStep(c.startSample, c.endSample, c.step, tr.Max)
	}
	return c
}

func (c *InstantVectorCursor) SinkPlan(plan hybridqp.QueryNode) {
	if plan.Schema().HasCall() && (plan.Schema().HasOptimizeCall() || plan.Schema().CanCallsPushdown()) {
		c.aggregateCursor.SinkPlan(plan)
	} else {
		c.aggregateCursor.input.SinkPlan(plan)
		schema := c.aggregateCursor.input.GetSchema()
		ops := plan.RowExprOptions()
		exprOpt := make([]hybridqp.ExprOptions, 0, len(ops))
		for i := range ops {
			exprOpt = append(exprOpt, hybridqp.ExprOptions{Expr: &ops[i].Ref, Ref: ops[i].Ref})
		}
		c.SetSchema(schema, schema, exprOpt)
	}
}

func (c *InstantVectorCursor) SetSchema(inSchema, outSchema record.Schemas, exprOpt []hybridqp.ExprOptions) {
	var err error
	c.coProcessor, err = newPromSampleProcessor(inSchema[:inSchema.Len()-1], outSchema[:outSchema.Len()-1], exprOpt)
	if err != nil {
		panic(err)
	}
	c.inSchema, c.outSchema, c.timeOrdinal = inSchema, outSchema, outSchema.Len()-1
}

func (c *InstantVectorCursor) reduce(inRecord, newRecord *record.Record) {
	c.computeIntervalIndex(inRecord)
	c.setReducerParams()
	c.coProcessor.WorkOnRecord(inRecord, newRecord, c.reducerParams)
	c.reset()
}

func (c *InstantVectorCursor) inNextWindowWithInfo(currRecord *record.Record) error {
	nextRecord, info, err := c.peekRecordWithInfo()
	if err != nil {
		return err
	}
	// padding is required before and after the last rec. Padding is required only before the previous rec.
	c.reducerParams.lastRec = (nextRecord == nil) || (c.fileInfo != nil && info != c.fileInfo)
	c.inNextWin = isSameWindow(currRecord, nextRecord, c.fileInfo, info, c.schema, c.startSample, c.endSample, c.step)
	return nil
}

func (c *InstantVectorCursor) inNextWindow(currRecord *record.Record) error {
	nextRecord, _, err := c.peekRecord()
	if err != nil {
		return err
	}
	// padding is required before and after the last rec. Padding is required only before the previous rec.
	c.reducerParams.lastRec = nextRecord == nil
	c.inNextWin = isSameWindow(currRecord, nextRecord, nil, nil, c.schema, c.startSample, c.endSample, c.step)
	return nil
}

// computeIntervalIndex as a key method ot use the dual-pointer algorithm to quickly find the start and end of the target step.
func (c *InstantVectorCursor) computeIntervalIndex(record *record.Record) {
	if c.step == 0 {
		c.intervalIndex = append(c.intervalIndex, 0, uint16(record.RowNums()))
		return
	}
	times := record.Times()
	firstStep := getCurrStep(c.startSample, c.endSample, c.step, times[0])
	lastStep := getCurrStep(c.startSample, c.endSample, c.step, times[len(times)-1])
	c.firstStep = firstStep
	if firstStep == lastStep {
		c.intervalIndex = append(c.intervalIndex, 0, uint16(record.RowNums()))
		return
	}
	var i, j int
	for end := firstStep; end <= lastStep; end += c.step {
		start := end - c.lookUpDelta
		for i < len(times) {
			if times[i] >= start {
				c.intervalIndex = append(c.intervalIndex, uint16(i))
				break
			}
			i++
		}
		for j < len(times) {
			if times[j] > end {
				c.intervalIndex = append(c.intervalIndex, uint16(j))
				break
			}
			j++
		}
		// the boundary value exceeds the maximum time.
		if len(c.intervalIndex)%2 != 0 {
			c.intervalIndex = append(c.intervalIndex, uint16(record.RowNums()))
		}
	}
}

func (c *InstantVectorCursor) setReducerParams() {
	c.reducerParams.sameWindow = c.inNextWin
	c.reducerParams.intervalIndex = c.intervalIndex
	c.reducerParams.step = c.step
	c.reducerParams.firstStep = c.firstStep
	c.reducerParams.lookBackDelta = c.lookUpDelta
	c.reducerParams.offset = c.offset
}

func (c *InstantVectorCursor) reset() {
	c.intervalIndex = c.intervalIndex[:0]
	c.inNextWin = false
}

func (c *InstantVectorCursor) Name() string {
	return "instant_vector_cursor"
}

func isSameWindow(
	currRecord, nextRecord *record.Record, currInfo, nextInfo *comm.FileInfo, schema hybridqp.Catalog,
	startSample, endSample, step int64,
) bool {
	if nextRecord == nil || currRecord.RowNums() == 0 {
		return false
	}
	if nextRecord.RowNums() == 0 {
		return true
	}

	if currInfo != nil && nextInfo != currInfo {
		return false
	}

	if schema.Options().GetPromStep() == 0 {
		return true
	}

	prevStep := getCurrStep(startSample, endSample, step, currRecord.Times()[currRecord.RowNums()-1])
	nextStep := getCurrStep(startSample, endSample, step, nextRecord.Times()[0])
	return prevStep == nextStep
}

func getPrevStep(startSample, endSample, step, t int64) int64 {
	if t <= startSample {
		return startSample
	}
	if t == endSample {
		return t
	}
	n := (t - startSample) / step
	return hybridqp.MinInt64(startSample+n*step, endSample)
}

func getCurrStep(startSample, endSample, step, t int64) int64 {
	if t <= startSample {
		return startSample
	}
	n, r := (t-startSample)/step, (t-startSample)%step
	if r > 0 {
		return hybridqp.MinInt64(startSample+(n+1)*step, endSample)
	}
	return hybridqp.MinInt64(t, endSample)
}

func newPromSampleProcessor(inSchema, outSchema record.Schemas, exprOpt []hybridqp.ExprOptions) (CoProcessor, error) {
	var haveCount bool
	var inField, outField string
	coProcessor := NewCoProcessorImpl()
	for i := range exprOpt {
		switch expr := exprOpt[i].Expr.(type) {
		case *influxql.Call:
			if expr.Name == "count_prom" {
				haveCount = true
			}
			inField, outField = expr.Args[0].(*influxql.VarRef).Val, exprOpt[i].Ref.Val
		case *influxql.VarRef:
			inField, outField = expr.Val, exprOpt[i].Ref.Val
		default:
		}
		routine, err := newPromSampleRoutineImpl(inSchema, outSchema, inField, outField, haveCount)
		if err != nil {
			return nil, err
		}
		coProcessor.AppendRoutine(routine)
	}
	return coProcessor, nil
}

func newPromSampleRoutineImpl(inSchema, outSchema record.Schemas, inField, outField string, haveCount bool) (Routine, error) {
	inOrdinal := inSchema.FieldIndex(inField)
	outOrdinal := outSchema.FieldIndex(outField)
	if inOrdinal < 0 || outOrdinal < 0 {
		return nil, fmt.Errorf("input and output schemas are not aligned for prom sample cursor")
	}
	dataType := inSchema.Field(inOrdinal).Type
	switch dataType {
	case influx.Field_Type_Float:
		if haveCount {
			return NewRoutineImpl(newFloatSampler(floatLastReduceFunc, appendIntegerCount), inOrdinal, outOrdinal), nil
		}
		return NewRoutineImpl(newFloatSampler(floatLastReduceFunc, appendFloatValue), inOrdinal, outOrdinal), nil
	default:
		return nil, fmt.Errorf("unsupported data type for prom sample cursor: %d", dataType)
	}
}

// note: last is designed in ascending order.
func floatLastReduceFunc(cv *record.ColVal, values []float64, start, end int) (int, float64, bool) {
	if start == end {
		return 0, 0, true
	}
	return floatLastReduce(cv, values, start, end)
}

func appendFloatValue(col *record.ColVal, value float64) {
	col.AppendFloat(value)
}

func appendIntegerCount(col *record.ColVal, _ float64) {
	col.AppendFloat(1)
}

type float2lFloatReduce func(col *record.ColVal, values []float64, bmStart, bmEnd int) (index int, value float64, isNil bool)

type appendValueFunc func(col *record.ColVal, value float64)

type floatSampler struct {
	fn      float2lFloatReduce
	fv      appendValueFunc
	offset  int64
	prevBuf *floatColBuf
}

func newFloatSampler(fn float2lFloatReduce, fv appendValueFunc) *floatSampler {
	return &floatSampler{
		fn:      fn,
		fv:      fv,
		prevBuf: newFloatColBuf(),
	}
}

func (r *floatSampler) PopulateByPrevious(outRecord *record.Record, param *ReducerParams, nextStep, lastStep int64, outOrdinal, timeIdx int) {
	for t := nextStep; t <= lastStep; t += param.step {
		if r.prevBuf.time < t-param.lookBackDelta {
			break
		}
		r.fv(outRecord.Column(outOrdinal), r.prevBuf.value)
		// multi-column aggregation calculation time is processed only once.
		if outOrdinal == 0 {
			outRecord.AppendTime(t + r.offset)
		}
	}
}

func (r *floatSampler) Aggregate(p *ReducerEndpoint, param *ReducerParams) {
	r.offset = param.offset
	inRecord, outRecord := p.InputPoint.Record, p.OutputPoint.Record
	inOrdinal, outOrdinal := p.InputPoint.Ordinal, p.OutputPoint.Ordinal
	values := inRecord.ColVals[inOrdinal].FloatValues()
	timeIdx, numStep := outRecord.ColNums()-1, len(param.intervalIndex)/2
	if param.step == 0 && param.rangeDuration > 0 {
		outRecord.AppendRec(inRecord, 0, inRecord.RowNums())
		return
	}
	var ts int64
	for i := 0; i < numStep; i++ {
		if param.sameWindow && i == numStep-1 {
			continue
		}
		idx, value, isNil := r.fn(&inRecord.ColVals[inOrdinal], values, int(param.intervalIndex[2*i]), int(param.intervalIndex[2*i+1]))
		if param.step > 0 {
			ts = param.firstStep + int64(i)*param.step
			if i == 0 && !r.prevBuf.isNil {
				// padding is required before each rec.
				nextStep, lastStep := int64(r.prevBuf.index)+param.step, param.firstStep-param.step
				if nextStep <= lastStep {
					r.PopulateByPrevious(outRecord, param, nextStep, lastStep, outOrdinal, timeIdx)
				}
			}
		} else {
			ts = inRecord.Time(idx)
		}
		if !isNil {
			r.fv(outRecord.Column(outOrdinal), value)
			// multi-column aggregation calculation time is processed only once.
			if outOrdinal == 0 {
				outRecord.AppendTime(ts + r.offset)
			}
		} else {
			if r.prevBuf.isNil || r.prevBuf.time < ts-param.lookBackDelta {
				continue
			}
			r.fv(outRecord.Column(outOrdinal), r.prevBuf.value)
			// multi-column aggregation calculation time is processed only once.
			if outOrdinal == 0 {
				outRecord.AppendTime(ts + r.offset)
			}
		}
	}
	idx, value, isNil := r.fn(&inRecord.ColVals[inOrdinal], values, 0, inRecord.RowNums())
	if !isNil {
		r.prevBuf.time = inRecord.Time(idx)
		r.prevBuf.index = int(param.firstStep + int64(numStep-1)*param.step)
		r.prevBuf.value = value
		r.prevBuf.isNil = false
	}
	if param.step > 0 && param.lastRec && !r.prevBuf.isNil {
		// padding is required after the last rec.
		nextStep := ts + param.step
		if nextStep <= param.lastStep {
			r.PopulateByPrevious(outRecord, param, nextStep, param.lastStep, outOrdinal, timeIdx)
		}
	}
}
