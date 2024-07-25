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
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
)

// RangeVectorCursor is used to process the calculation of the function with range duration for
// prom instant_query or range_query. This is a kind of sliding window calculation.
// For aggregated query, replace aggregateCursor with the RangeVectorCursor.
// Give an example: avg_over_time(value[3]) start=1, end=6, step=2
// rangDuration=3, startSample=1, endSample=5
// original   data: (time, value)=> [(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0), (6, 6.0)]
// interval  index: [start, end) => [[0, 1),       [0,3),                            [1, 4)]
// grouped    data: (time, value)=> [[(1, 1.0)],   [(1, 1.0), (2, 2.0), (3, 3.0)],   [(2, 2.0), (3, 3.0), (4, 4.0)]]
// aggregated data: (time, value)=> [ (1, 1.0),     (3, 2.0),                         (5, 3.0)]
type RangeVectorCursor struct {
	aggregateCursor

	lookUpDelta   int64
	rangeDuration int64
	start         int64 // query start time
	end           int64 // query end time
	offset        int64 // query offset time
	step          int64 // query resolution step width in duration format or float number of seconds.
	startSample   int64 // start time for prom sampling
	endSample     int64 // end time for prom sampling
	firstStep     int64 // first step of each record for prom sampling
}

func NewRangeVectorCursor(input comm.KeyCursor, schema *executor.QuerySchema, globalPool *record.RecordPool, tr util.TimeRange) *RangeVectorCursor {
	c := &RangeVectorCursor{}
	c.aggregateCursor = *NewAggregateCursor(input, schema, globalPool, false)
	c.aggregateCursor.r = c

	c.lookUpDelta = schema.Options().GetPromLookBackDelta().Nanoseconds()
	c.rangeDuration = schema.Options().GetPromRange().Nanoseconds()
	c.step = schema.Options().GetPromStep().Nanoseconds()
	c.start = schema.Options().GetStartTime()
	c.end = schema.Options().GetEndTime()
	c.offset = schema.Options().GetPromQueryOffset().Nanoseconds()
	c.startSample = c.start + c.rangeDuration
	if c.step == 0 {
		c.endSample = c.startSample
		c.firstStep = c.startSample
		c.reducerParams.lastStep = c.endSample
	} else {
		c.endSample = c.start + c.rangeDuration + (c.end-(c.start+c.rangeDuration))/c.step*c.step
		c.firstStep = getCurrStep(c.startSample, c.endSample, c.step, tr.Min)
		c.reducerParams.lastStep = getPrevStep(c.startSample, c.endSample, c.step, tr.Max)
	}
	return c
}

func (c *RangeVectorCursor) SinkPlan(plan hybridqp.QueryNode) {
	c.aggregateCursor.SinkPlan(plan)
}

func (c *RangeVectorCursor) SetSchema(inSchema, outSchema record.Schemas, exprOpt []hybridqp.ExprOptions) {
	var err error
	c.coProcessor, err = newPromFuncProcessor(inSchema, outSchema, exprOpt)
	if err != nil {
		panic(err)
	}
	c.inSchema, c.outSchema, c.timeOrdinal = inSchema, outSchema, outSchema.Len()-1
}

func (c *RangeVectorCursor) reduce(inRecord, newRecord *record.Record) {
	c.getIntervalIndex(inRecord)
	c.setReducerParams()
	c.coProcessor.WorkOnRecord(inRecord, newRecord, c.reducerParams)
	c.resetReducerParams()
}

func (c *RangeVectorCursor) inNextWindowWithInfo(currRecord *record.Record) error {
	nextRecord, info, err := c.peekRecordWithInfo()
	if err != nil {
		return err
	}
	// padding is required before and after the last rec. Padding is required only before the previous rec.
	c.reducerParams.lastRec = (nextRecord == nil) || (c.fileInfo != nil && info != c.fileInfo)
	c.inNextWin = isSameWindow(currRecord, nextRecord, c.fileInfo, info, c.schema, c.startSample, c.endSample, c.step)
	return nil
}

func (c *RangeVectorCursor) inNextWindow(currRecord *record.Record) error {
	nextRecord, _, err := c.peekRecord()
	if err != nil {
		return err
	}
	// padding is required before and after the last rec. Padding is required only before the previous rec.
	c.reducerParams.lastRec = nextRecord == nil
	c.inNextWin = isSameWindow(currRecord, nextRecord, nil, nil, c.schema, c.startSample, c.endSample, c.step)
	return nil
}

// getIntervalIndex as a key method ot use the dual-pointer algorithm to quickly find the start and end of the target step.
// intervalIndex stores the interval of each group, left-closed and right-open, for example, [[0, 2), [1, 3)].
func (c *RangeVectorCursor) getIntervalIndex(record *record.Record) {
	if c.step == 0 {
		c.firstStep = c.startSample
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
		start := end - c.rangeDuration
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

func (c *RangeVectorCursor) setReducerParams() {
	c.reducerParams.sameWindow = c.inNextWin
	c.reducerParams.intervalIndex = c.intervalIndex
	c.reducerParams.step = c.step
	c.reducerParams.firstStep = c.firstStep
	c.reducerParams.lookBackDelta = c.lookUpDelta
	c.reducerParams.offset = c.offset
	c.reducerParams.rangeDuration = c.rangeDuration
}

func (c *RangeVectorCursor) resetReducerParams() {
	c.intervalIndex = c.intervalIndex[:0]
	c.inNextWin = false
}

func (c *RangeVectorCursor) Name() string {
	return "range_vector_cursor"
}
