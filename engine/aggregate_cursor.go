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

package engine

import (
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// reducer represents the interface of the respective implementation method of aggregateCursor and InstantVectorCursor
type reducer interface {
	reduce(inRecord, newRecord *record.Record)
	inNextWindow(currRecord *record.Record) error
	inNextWindowWithInfo(currRecord *record.Record) error
	SetSchema(inSchema, outSchema record.Schemas, exprOpt []hybridqp.ExprOptions)
}

type aggregateCursor struct {
	r reducer

	init          bool
	multiCall     bool
	inNextWin     bool
	initColMeta   bool
	timeOrdinal   int
	maxRecordSize int
	auxTag        bool
	bufRecord     *record.Record
	bufInfo       *comm.FileInfo
	span          *tracing.Span
	schema        *executor.QuerySchema
	reducerParams *ReducerParams
	input         comm.KeyCursor
	sInfo         comm.SeriesInfoIntf
	fileInfo      *comm.FileInfo
	coProcessor   CoProcessor
	inSchema      record.Schemas
	outSchema     record.Schemas
	intervalIndex []uint16
	recordPool    *record.CircularRecordPool
	globalPool    *record.RecordPool
}

func NewAggregateCursor(input comm.KeyCursor, schema *executor.QuerySchema, globalPool *record.RecordPool, hasAuxTags bool) *aggregateCursor {
	c := &aggregateCursor{
		input:         input,
		schema:        schema,
		reducerParams: &ReducerParams{},
		globalPool:    globalPool,
	}
	c.auxTag = hasAuxTags
	limitSize := schema.Options().GetLimit() + schema.Options().GetOffset()
	if limitSize > 0 {
		c.maxRecordSize = util.Min(schema.Options().ChunkSizeNum(), limitSize)
	} else {
		c.maxRecordSize = schema.Options().ChunkSizeNum()
	}
	c.r = c
	return c
}

func (c *aggregateCursor) SetSchema(inSchema, outSchema record.Schemas, exprOpt []hybridqp.ExprOptions) {
	c.inSchema = inSchema
	c.outSchema = outSchema
	c.coProcessor, c.initColMeta, c.multiCall = newProcessor(inSchema[:inSchema.Len()-1], outSchema[:outSchema.Len()-1], exprOpt)
	c.reducerParams.multiCall = c.multiCall
	c.timeOrdinal = outSchema.Len() - 1
}
func (c *aggregateCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	if c.span != nil {
		start := time.Now()
		defer func() {
			c.span.Count(aggIterCount, 1)
			c.span.Count(aggIterCount, int64(time.Since(start)))
		}()
	}

	if !c.init {
		c.recordPool = record.NewCircularRecordPool(c.globalPool, aggCursorRecordNum, c.outSchema, c.initColMeta)
		c.intervalIndex = util.Bytes2Uint16Slice(bufferpool.Get())
		c.intervalIndex = c.intervalIndex[:0]
		c.init = true
	}

	newRecord := c.recordPool.Get()
	for {
		inRecord, info, err := c.nextRecordWithInfo()
		if err != nil {
			return nil, nil, err
		}
		if inRecord == nil {
			if newRecord.RowNums() > 0 {
				currInfo := c.fileInfo
				return newRecord, currInfo, err
			}
			return nil, nil, nil
		}
		if inRecord.RowNums() == 0 {
			continue
		}
		if c.fileInfo != nil && info != c.fileInfo {
			c.unreadRecordWithInfo(inRecord, info)
			currInfo := c.fileInfo
			c.fileInfo = info
			if newRecord.RowNums() > 0 {
				return newRecord, currInfo, err
			}
		}
		if newRecord.RowNums() >= c.maxRecordSize {
			currInfo := c.fileInfo
			c.unreadRecordWithInfo(inRecord, info)
			return newRecord, currInfo, err
		}
		c.fileInfo = info
		err = c.r.inNextWindowWithInfo(inRecord)
		if err != nil {
			return nil, nil, err
		}
		c.r.reduce(inRecord, newRecord)
	}
}

func (c *aggregateCursor) unreadRecordWithInfo(record *record.Record, info *comm.FileInfo) {
	c.bufRecord = record
	c.bufInfo = info
}

func (c *aggregateCursor) peekRecordWithInfo() (*record.Record, *comm.FileInfo, error) {
	inRecord, info, err := c.nextRecordWithInfo()
	if err != nil {
		return nil, nil, err
	}
	c.unreadRecordWithInfo(inRecord, info)
	return inRecord, info, nil
}

func (c *aggregateCursor) nextRecordWithInfo() (*record.Record, *comm.FileInfo, error) {
	bufRecord := c.bufRecord
	if bufRecord != nil {
		c.bufRecord = nil
		return bufRecord, c.bufInfo, nil
	}
	return c.input.NextAggData()
}

func (c *aggregateCursor) inNextWindowWithInfo(currRecord *record.Record) error {
	nextRecord, info, err := c.peekRecordWithInfo()
	if err != nil {
		return err
	}
	if nextRecord == nil || currRecord.RowNums() == 0 {
		c.inNextWin = false
		return nil
	}
	if nextRecord.RowNums() == 0 {
		c.inNextWin = true
		return nil
	}

	if c.fileInfo != nil && info != c.fileInfo {
		c.inNextWin = false
		return nil
	}

	if !c.schema.Options().HasInterval() {
		c.inNextWin = true
		return nil
	}

	lastTime := currRecord.Times()[currRecord.RowNums()-1]
	startTime, endTime := c.schema.Options().Window(nextRecord.Times()[0])
	if startTime <= lastTime && lastTime < endTime {
		c.inNextWin = true
	} else {
		c.inNextWin = false
	}
	return nil
}

func (c *aggregateCursor) SetOps(ops []*comm.CallOption) {
	c.input.SetOps(ops)
}

func (c *aggregateCursor) SinkPlan(plan hybridqp.QueryNode) {
	c.input.SinkPlan(plan.Children()[0])
	var outSchema record.Schemas
	outRowDataType := plan.RowDataType()
	ops := plan.RowExprOptions()

	opsCopy := make([]hybridqp.ExprOptions, len(ops))
	copy(opsCopy, ops)

	inSchema := c.input.GetSchema()
	tagRef := make(map[influxql.VarRef]bool)
	for _, expr := range outRowDataType.Fields() {
		if ref, ok := expr.Expr.(*influxql.VarRef); ok && ref.Type == influxql.Tag {
			tagRef[*ref] = true
		}
		// remove the tag ref of the outSchema
		if ref, ok := expr.Expr.(*influxql.VarRef); ok && ref.Type != influxql.Tag {
			outSchema = append(outSchema, record.Field{Name: ref.Val, Type: record.ToModelTypes(ref.Type)})
		}
	}
	outSchema = append(outSchema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})

	// remove the tag ref of the ops
	removeIndex := make([]int, 0, len(tagRef))
	for i, v := range opsCopy {
		if tagRef[v.Ref] {
			removeIndex = append(removeIndex, i)
		}
	}
	for i, idx := range removeIndex {
		opsCopy = append(opsCopy[:idx-i], opsCopy[idx+1-i:]...)
	}

	c.r.SetSchema(inSchema, outSchema, opsCopy)
}

func (c *aggregateCursor) unreadRecord(record *record.Record) {
	c.bufRecord = record
}

func (c *aggregateCursor) peekRecord() (*record.Record, comm.SeriesInfoIntf, error) {
	inRecord, info, err := c.nextRecord()
	if err != nil {
		return nil, nil, err
	}
	c.unreadRecord(inRecord)
	c.sInfo = info
	return inRecord, info, nil
}

func (c *aggregateCursor) nextRecord() (*record.Record, comm.SeriesInfoIntf, error) {
	bufRecord := c.bufRecord
	if bufRecord != nil {
		c.bufRecord = nil
		return bufRecord, c.sInfo, nil
	}
	return c.input.Next()
}

func (c *aggregateCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if c.span != nil {
		start := time.Now()
		defer func() {
			c.span.Count(aggIterCount, 1)
			c.span.Count(aggIterCount, int64(time.Since(start)))
		}()
	}

	if !c.init {
		c.recordPool = record.NewCircularRecordPool(c.globalPool, aggCursorRecordNum, c.outSchema, c.initColMeta)
		c.intervalIndex = util.Bytes2Uint16Slice(bufferpool.Get())
		c.init = true
	}

	newRecord := c.recordPool.Get()
	for {
		inRecord, info, err := c.nextRecord()
		if err != nil {
			return nil, nil, err
		} else if inRecord == nil {
			if newRecord.RowNums() > 0 {
				return newRecord, info, err
			}
			return nil, nil, nil
		} else if inRecord.RowNums() == 0 {
			continue
		} else if newRecord.RowNums() >= c.maxRecordSize {
			c.unreadRecord(inRecord)
			return newRecord, info, nil
		}
		err = c.r.inNextWindow(inRecord)
		if err != nil {
			return nil, nil, err
		}
		c.r.reduce(inRecord, newRecord)
	}
}

func (c *aggregateCursor) reduce(inRecord, newRecord *record.Record) {
	c.getIntervalIndex(inRecord)
	c.setReducerParams()
	c.coProcessor.WorkOnRecord(inRecord, newRecord, c.reducerParams)
	c.deriveIntervalIndex(inRecord, newRecord)
	c.clear()
}

func (c *aggregateCursor) inNextWindow(currRecord *record.Record) error {
	nextRecord, _, err := c.peekRecord()
	if err != nil {
		return err
	}
	if nextRecord == nil || currRecord.RowNums() == 0 {
		c.inNextWin = false
		return nil
	}
	if nextRecord.RowNums() == 0 {
		c.inNextWin = true
		return nil
	}

	if !c.schema.Options().HasInterval() {
		c.inNextWin = true
		return nil
	}

	lastTime := currRecord.Times()[currRecord.RowNums()-1]
	startTime, endTime := c.schema.Options().Window(nextRecord.Times()[0])
	if startTime <= lastTime && lastTime < endTime {
		c.inNextWin = true
	} else {
		c.inNextWin = false
	}
	return nil
}

func (c *aggregateCursor) getIntervalIndex(record *record.Record) {
	var startTime, endTime int64
	if !c.schema.Options().HasInterval() {
		c.intervalIndex = append(c.intervalIndex, 0)
		return
	}
	times := record.Times()
	for i, t := range times {
		if i == 0 || t >= endTime || t < startTime {
			c.intervalIndex = append(c.intervalIndex, uint16(i))
			startTime, endTime = c.schema.Options().Window(t)
		}
	}
}

func (c *aggregateCursor) deriveIntervalIndex(inRecord, newRecord *record.Record) {
	if !c.multiCall {
		return
	}

	var addRecordLen int
	if c.inNextWin {
		addRecordLen = len(c.intervalIndex) - 1
	} else {
		addRecordLen = len(c.intervalIndex)
	}

	// the time of the first point in each time window is used as the aggregated time.
	times := inRecord.Times()
	for i := 0; i < addRecordLen; i++ {
		newRecord.ColVals[c.timeOrdinal].AppendInteger(times[c.intervalIndex[i]])
	}
}

func (c *aggregateCursor) setReducerParams() {
	c.reducerParams.sameWindow = c.inNextWin
	c.reducerParams.intervalIndex = c.intervalIndex
}

func (c *aggregateCursor) clear() {
	c.intervalIndex = c.intervalIndex[:0]
	c.inNextWin = false
}

func (c *aggregateCursor) GetSchema() record.Schemas {
	return c.outSchema
}

func (c *aggregateCursor) Close() error {
	if c.recordPool != nil {
		c.clear()
		c.recordPool.Put()
		bufferpool.Put(util.Uint16Slice2byte(c.intervalIndex))
	}
	return c.input.Close()
}

func (c *aggregateCursor) Name() string {
	return "aggregate_cursor"
}

func (c *aggregateCursor) StartSpan(span *tracing.Span) {
	if span != nil {
		c.span = span
		c.input.StartSpan(c.span)
	}
}

func (c *aggregateCursor) EndSpan() {
}
