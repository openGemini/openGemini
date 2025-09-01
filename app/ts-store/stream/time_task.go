// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package stream

import (
	"fmt"
	"math"
	"runtime/debug"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	atomic2 "github.com/openGemini/openGemini/lib/atomic"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	streamLib "github.com/openGemini/openGemini/lib/stream"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/stream"
	"go.uber.org/zap"
)

type TimeTask struct {
	values      []float64
	validValues []bool

	// store all ptIds for all window
	ptIds []int32
	// store all shardIds for all window
	shardIds     []int64
	windowOffset []int

	// pool
	windowCachePool *TaskCachePool
	*TaskDataPool

	// tmp data, reuse
	row *influx.Row
	*BaseTask
}

func (s *TimeTask) cleanPtInfo(ptID uint32) {
}

func (s *TimeTask) getSrcInfo() *meta2.StreamMeasurementInfo {
	return s.src
}

func (s *TimeTask) getDesInfo() *meta2.StreamMeasurementInfo {
	return s.des
}

func (s *TimeTask) Put(r ChanData) {
	s.TaskDataPool.Put(r)
}

func (s *TimeTask) getLoadStatus() map[uint32]*flushStatus {
	return map[uint32]*flushStatus{}
}

func (s *TimeTask) stop() error {
	close(s.abort)
	return s.err
}

func (s *TimeTask) getName() string {
	return s.name
}

func (s *TimeTask) run() error {
	err := s.initVar()
	if err != nil {
		s.err = err
		return err
	}
	s.info, err = s.cli.Measurement(s.des.Database, s.des.RetentionPolicy, s.des.Name)
	if err != nil {
		s.err = err
		return err
	}

	if s.window == 0 {
		return nil
	}

	go s.monitorRecover(s.consumeData)
	go s.cycleFlush()
	return nil
}

func (s *TimeTask) monitorRecover(f func()) {
	for {
		f()

		select {
		case <-s.abort:
			return
		default:
		}
	}
}

func (s *TimeTask) initVar() error {
	s.maxDuration = s.windowNum * s.window.Nanoseconds()
	s.abort = make(chan struct{})
	// chan len zero, make updateWindow cannot parallel execute with flush
	s.updateWindow = make(chan struct{})
	s.fieldCallsLen = len(s.fieldCalls)

	if s.window > 0 {
		s.values = make([]float64, s.fieldCallsLen*int(s.windowNum))
		s.validValues = make([]bool, s.fieldCallsLen*int(s.windowNum))
		for i := 0; i < int(s.windowNum); i++ {
			for j := 0; j < s.fieldCallsLen; j++ {
				id := i*s.fieldCallsLen + j
				initValue(s.values, id, s.fieldCalls[j].Call)
			}
		}
		s.windowOffset = make([]int, s.windowNum)
		for i := 0; i < int(s.windowNum); i++ {
			s.windowOffset[i] = i * s.fieldCallsLen
		}
	}
	s.rows = make([]influx.Row, 1)

	s.ptIds = make([]int32, maxWindowNum)
	s.shardIds = make([]int64, maxWindowNum)
	for i := 0; i < maxWindowNum; i++ {
		s.ptIds[i] = -1
		s.shardIds[i] = -1
	}
	s.startTimeStamp = s.start.UnixNano()
	s.endTimeStamp = s.end.UnixNano()
	s.maxTimeStamp = s.startTimeStamp + s.maxDuration
	return nil
}

// consume data from window cache, and update window metadata
func (s *TimeTask) consumeData() {
	defer func() {
		if r := recover(); r != nil {
			err := errno.NewError(errno.RecoverPanic, r)
			s.Logger.Error(err.Error(), zap.String("stack", string(debug.Stack())))
		}
	}()
	for {
		select {
		case _, open := <-s.updateWindow:
			if !open {
				return
			}
			s.start = s.end
			s.end = s.end.Add(s.window)
			s.startTimeStamp = s.start.UnixNano()
			s.endTimeStamp = s.end.UnixNano()
			s.maxTimeStamp = s.startTimeStamp + s.maxDuration
			lastWindowId := s.startWindowID
			atomic2.SetModInt64AndADD(&s.startWindowID, 1, int64(s.windowNum))
			s.stats.Reset()
			s.stats.StatWindowOutMinTime(s.startTimeStamp)
			s.stats.StatWindowOutMaxTime(s.maxTimeStamp)
			t := time.Now()
			s.walkUpdate(s.values, lastWindowId)
			s.stats.StatWindowUpdateCost(int64(time.Since(t)))
		case <-s.abort:
			ticker := time.NewTicker(30 * time.Second)
			for {
				select {
				case _, open := <-s.updateWindow:
					if !open {
						ticker.Stop()
						return
					}
					continue
				case <-s.cache:
					continue
				case <-ticker.C:
					ticker.Stop()
					return
				}
			}
		case cache := <-s.cache:
			if !stream.IsWriteStreamPointsEnabled() {
				switch c := cache.(type) {
				case *TaskCache:
					if c.release != nil {
						c.release()
					}
					c.rows = nil
					s.windowCachePool.Put(c)
				case *CacheRecord:
					c.Release()
				default:
					s.Logger.Info(fmt.Sprintf("unknown cache type: %T", cache))
				}
				continue
			}
			if s.window > 0 {
				err := s.calculate(cache)
				if err != nil {
					s.Logger.Error("calculate error", zap.String("window", s.name), zap.Error(err))
				}
			} else {
				err := s.calculateFilterOnly(cache)
				if err != nil {
					s.Logger.Error("calculate error", zap.String("window", s.name), zap.Error(err))
				}
			}
			s.IncreaseChan()
		}
	}
}

func (s *TimeTask) cycleFlush() {
	var err error
	defer func() {
		if r := recover(); r != nil {
			err = errno.NewError(errno.RecoverPanic, r)
			s.Logger.Error(err.Error(), zap.String("stack", string(debug.Stack())))
		}

		s.err = err
	}()
	reset := false
	now := time.Now()
	next := now.Truncate(s.window).Add(s.window).Add(s.maxDelay)
	ticker := time.NewTicker(next.Sub(now))
	for {
		select {
		case <-ticker.C:
			if !reset {
				reset = true
				ticker.Reset(s.window)
				continue
			}
			err = s.flush()
			if err != nil {
				s.Logger.Error("stream flush error", zap.Error(err))
			}
		case <-s.abort:
			return
		}
	}
}

func (s *TimeTask) walkUpdate(vv []float64, lastWindowId int64) bool {
	offset := int(lastWindowId) * s.fieldCallsLen
	vs := vv[offset : offset+s.fieldCallsLen]
	for j := 0; j < s.fieldCallsLen; j++ {
		initValue(vs, j, s.fieldCalls[j].Call)
		s.validValues[offset+j] = false
	}
	s.ptIds[lastWindowId] = -1
	s.shardIds[lastWindowId] = -1
	return true
}

func (s *TimeTask) calculateFilterOnly(data ChanData) error {
	if data == nil {
		return ErrEmptyCache
	}
	cache, ok := data.(*TaskCache)
	if !ok {
		return fmt.Errorf("not support type %T", cache)
	}

	// occur release func
	defer func() {
		if len(s.indexKeyPool) > 0 {
			s.indexKeyPool = s.indexKeyPool[:0]
		}
		if len(s.rows) > 0 {
			s.rows = s.rows[:0]
		}
		if cache.release != nil {
			cache.release()
		}
		cache.rows = nil
		s.windowCachePool.Put(cache)
	}()

	s.rows = s.rows[:0]
	matchedIndexes := filterRowsByExpr(cache.rows, s.condition)
	if len(matchedIndexes) == 0 {
		return nil
	}

	for _, idx := range matchedIndexes {
		rrow := &cache.rows[idx]
		var row *influx.Row
		if cap(s.rows) == len(s.rows) {
			s.rows = append(s.rows, influx.Row{})
		} else {
			s.rows = s.rows[:len(s.rows)+1]
		}
		row = &s.rows[len(s.rows)-1]
		row.Clone(rrow)
		row.Name = s.info.Name
		s.indexKeyPool = row.UnmarshalIndexKeys(s.indexKeyPool)

		if s.isSelectAll {
			// select *
			continue
		}

		fs := make(influx.Fields, 0, s.info.Schema.Len())
		for i := range row.Fields {
			field := &row.Fields[i]
			if _, ok = s.info.Schema.GetTyp(field.Key); ok {
				fs = append(fs, *field)
			}
		}
		if len(fs) == 0 {
			// TODO: Add the option syntax to the create stream statement. Users can choose whether to fill in 0 or discard data.
			s.rows = s.rows[:len(s.rows)-1] // point has no fields, so delete the current point
			continue
		}
		row.Fields = fs
	}
	if len(s.rows) == 0 {
		return nil
	}

	return s.doWrite(cache.shardId, cache.ptId, s.des.Database, s.des.RetentionPolicy, s.rows)
}

func isMatchCond(row *influx.Row, varRef *influxql.VarRef, value influxql.Expr, op influxql.Token) bool {
	idx, found := sort.Find(len(row.Fields), func(i int) int {
		return strings.Compare(varRef.Val, row.Fields[i].Key)
	})
	if !found {
		return false
	}
	field := &row.Fields[idx]

	switch v := value.(type) {
	case *influxql.IntegerLiteral:
		return isNumberFieldMatchCond(field, float64(v.Val), op)
	case *influxql.NumberLiteral:
		return isNumberFieldMatchCond(field, v.Val, op)
	case *influxql.BooleanLiteral:
		if field.Type != influx.Field_Type_Boolean {
			return false
		}
		var fieldValue bool
		if field.NumValue == 1 {
			fieldValue = true
		}
		switch op {
		case influxql.EQ:
			return fieldValue == v.Val
		case influxql.NEQ:
			return fieldValue != v.Val
		default:
			return false
		}
	case *influxql.StringLiteral:
		if field.Type != influx.Field_Type_String {
			return false
		}
		switch op {
		case influxql.EQ:
			return field.StrValue == v.Val
		case influxql.NEQ:
			return field.StrValue != v.Val
		default:
			return false
		}
	default:
		return false
	}
}

func isNumberFieldMatchCond(field *influx.Field, value float64, op influxql.Token) bool {
	switch op {
	case influxql.EQ:
		return field.NumValue == value
	case influxql.NEQ:
		return field.NumValue != value
	case influxql.GT:
		return field.NumValue > value
	case influxql.GTE:
		return field.NumValue >= value
	case influxql.LT:
		return field.NumValue < value
	case influxql.LTE:
		return field.NumValue <= value
	default:
		return false
	}
}

func filterRowsByExpr(rows []influx.Row, expr influxql.Expr) []int {
	switch cond := expr.(type) {
	case *influxql.BinaryExpr:
		var resIndex []int
		varRef, op, value, ok := getVarRefOpValue(cond)
		if ok {
			for i := range rows {
				row := &rows[i]
				if isMatchCond(row, varRef, value, op) {
					resIndex = append(resIndex, i)
				}
			}
			return resIndex
		}

		lIndexes := filterRowsByExpr(rows, cond.LHS)
		rIndexes := filterRowsByExpr(rows, cond.RHS)
		switch cond.Op {
		case influxql.AND:
			return util.IntersectSortedSliceInt(lIndexes, rIndexes)
		case influxql.OR:
			return util.UnionSortedSliceInt(lIndexes, rIndexes)
		default:
			return []int{}
		}
	case *influxql.ParenExpr:
		return filterRowsByExpr(rows, cond.Expr)
	default:
		return []int{}
	}
}

func getVarRefOpValue(expr *influxql.BinaryExpr) (*influxql.VarRef, influxql.Token, influxql.Expr, bool) {
	var varRef *influxql.VarRef
	var value influxql.Expr
	var op = expr.Op

	var ok bool
	if varRef, ok = expr.LHS.(*influxql.VarRef); ok {
		value = expr.RHS
	} else if varRef, ok = expr.RHS.(*influxql.VarRef); ok {
		value = expr.LHS
		switch op {
		case influxql.GT:
			op = influxql.LT
		case influxql.GTE:
			op = influxql.LTE
		case influxql.LT:
			op = influxql.GT
		case influxql.LTE:
			op = influxql.GTE
		}
	} else {
		return nil, 0, nil, false
	}
	return varRef, op, value, true
}

func (s *TimeTask) calculate(data ChanData) error {
	// occur release func
	if data == nil {
		return ErrEmptyCache
	}
	switch cache := data.(type) {
	case *TaskCache:
		defer func() {
			if cache.release != nil {
				cache.release()
			}
			cache.rows = nil
			s.windowCachePool.Put(cache)
		}()
		s.stats.AddWindowIn(int64(len(cache.rows)))
		s.stats.StatWindowStartTime(s.startTimeStamp)
		s.stats.StatWindowEndTime(s.maxTimeStamp)
		s.calculateRow(cache)
	case *CacheRecord:
		defer func() {
			cache.Release()
		}()
		s.stats.AddWindowIn(int64(cache.rec.RowNums()))
		s.stats.StatWindowStartTime(s.startTimeStamp)
		s.stats.StatWindowEndTime(s.maxTimeStamp)
		s.calculateRec(cache)
	default:
		return fmt.Errorf("not support type %T", cache)
	}

	return nil
}

func (s *TimeTask) calculateRec(cache *CacheRecord) {
	rec := cache.rec
	var skip int
	windowIDS := make([]int8, rec.RowNums())
	timeCol := rec.Column(rec.ColNums() - 1)
	times := timeCol.IntegerValues()
	for i, t := range times {
		if t < s.startTimeStamp || t >= s.maxTimeStamp {
			if t >= s.endTimeStamp {
				atomic2.CompareAndSwapMaxInt64(&s.stats.WindowOutMaxTime, t)
			} else {
				atomic2.CompareAndSwapMinInt64(&s.stats.WindowOutMinTime, t)
			}
			windowIDS[i] = -1
			skip++
			continue
		}
		windowIDS[i] = int8(s.windowId(t))
	}
	s.stats.AddWindowSkip(int64(skip))
	for c, call := range s.fieldCalls {
		id := rec.Schema.FieldIndex(call.Name)
		if id == -1 {
			id = rec.Schema.FieldIndex(call.Alias)
		}
		if id < 0 {
			continue
		}
		colVal := rec.Column(id)
		if colVal == nil {
			continue
		}
		if colVal.Length()+colVal.NilCount == 0 {
			continue
		}
		if rec.Schema.Field(id).Type == influx.Field_Type_Float {
			if call.Call == "count" {
				s.calculateCountValues(windowIDS, c, call, cache, colVal)
			} else {
				s.calculateFloatValues(windowIDS, c, call, cache, colVal)
			}
		} else if rec.Schema.Field(id).Type == influx.Field_Type_UInt || rec.Schema.Field(id).Type == influx.Field_Type_Int {
			if call.Call == "count" {
				s.calculateCountValues(windowIDS, c, call, cache, colVal)
			} else {
				s.calculateIntValues(windowIDS, c, call, cache, colVal)
			}
		}
	}
	s.stats.AddWindowProcess(int64(rec.RowNums() - skip))
}

func (s *TimeTask) calculateFloatValues(windowIDS []int8, c int, call *streamLib.FieldCall, cache *CacheRecord, colVal *record.ColVal) {
	var lastWindowID int8 = -1
	var lastIndex int = -1
	values := colVal.FloatValues()
	if colVal.NilCount == 0 {
		for i := 0; i < colVal.Length(); i++ {
			if lastWindowID == windowIDS[i] {
				s.values[lastIndex] = call.SingleThreadFunc(s.values[lastIndex], values[i])
				continue
			}
			lastWindowID = windowIDS[i]
			base := s.windowOffset[windowIDS[i]]
			index := base + c
			lastIndex = index
			s.values[index] = call.SingleThreadFunc(s.values[index], values[i])
			s.validValues[index] = true
			if s.shardIds[windowIDS[i]] < 0 {
				s.shardIds[windowIDS[i]] = int64(cache.shardID)
				s.ptIds[windowIDS[i]] = int32(cache.ptId)
			}
		}
		return
	}
	colIndex := 0
	for i := 0; i < colVal.Length(); i++ {
		if windowIDS[i] < 0 {
			continue
		}
		if colVal.IsNil(i) {
			continue
		}
		if lastWindowID == windowIDS[i] {
			s.values[lastIndex] = call.SingleThreadFunc(s.values[lastIndex], values[colIndex])
			colIndex++
			continue
		}
		colIndex++
		lastWindowID = windowIDS[i]
		base := s.windowOffset[windowIDS[i]]
		index := base + c
		lastIndex = index
		s.values[index] = call.SingleThreadFunc(s.values[index], values[colIndex])
		s.validValues[index] = true
		if s.shardIds[windowIDS[i]] < 0 {
			s.shardIds[windowIDS[i]] = int64(cache.shardID)
			s.ptIds[windowIDS[i]] = int32(cache.ptId)
		}
	}
}

func (s *TimeTask) calculateIntValues(windowIDS []int8, c int, call *streamLib.FieldCall, cache *CacheRecord, colVal *record.ColVal) {
	var lastWindowID int8 = -1
	var lastIndex int = -1
	values := colVal.IntegerValues()
	if colVal.NilCount == 0 {
		for i := 0; i < colVal.Length(); i++ {
			if lastWindowID == windowIDS[i] {
				s.values[lastIndex] = call.SingleThreadFunc(s.values[lastIndex], float64(values[i]))
				continue
			}
			lastWindowID = windowIDS[i]
			base := s.windowOffset[windowIDS[i]]
			index := base + c
			lastIndex = index
			s.values[index] = call.SingleThreadFunc(s.values[index], float64(values[i]))
			s.validValues[index] = true
			if s.shardIds[windowIDS[i]] < 0 {
				s.shardIds[windowIDS[i]] = int64(cache.shardID)
				s.ptIds[windowIDS[i]] = int32(cache.ptId)
			}
		}
		return
	}
	colIndex := 0
	for i := 0; i < colVal.Length(); i++ {
		if windowIDS[i] < 0 {
			continue
		}
		if colVal.IsNil(i) {
			continue
		}
		if lastWindowID == windowIDS[i] {
			s.values[lastIndex] = call.SingleThreadFunc(s.values[lastIndex], float64(values[colIndex]))
			colIndex++
			continue
		}
		colIndex++
		lastWindowID = windowIDS[i]
		base := s.windowOffset[windowIDS[i]]
		index := base + c
		lastIndex = index
		s.values[index] = call.SingleThreadFunc(s.values[index], float64(values[colIndex]))
		s.validValues[index] = true
		if s.shardIds[windowIDS[i]] < 0 {
			s.shardIds[windowIDS[i]] = int64(cache.shardID)
			s.ptIds[windowIDS[i]] = int32(cache.ptId)
		}
	}
}

func (s *TimeTask) calculateCountValues(windowIDS []int8, c int, call *streamLib.FieldCall, cache *CacheRecord, colVal *record.ColVal) {
	var lastWindowID int8 = -1
	var lastIndex int = -1
	if colVal.NilCount == 0 {
		for i := 0; i < colVal.Length(); i++ {
			if lastWindowID == windowIDS[i] {
				s.values[lastIndex] = call.SingleThreadFunc(s.values[lastIndex], 1)
				continue
			}
			lastWindowID = windowIDS[i]
			base := s.windowOffset[windowIDS[i]]
			index := base + c
			lastIndex = index
			s.values[index] = call.SingleThreadFunc(s.values[index], 1)
			s.validValues[index] = true
			if s.shardIds[windowIDS[i]] < 0 {
				s.shardIds[windowIDS[i]] = int64(cache.shardID)
				s.ptIds[windowIDS[i]] = int32(cache.ptId)
			}
		}
		return
	}
	for i := 0; i < colVal.Length(); i++ {
		if windowIDS[i] < 0 {
			continue
		}
		if colVal.IsNil(i) {
			continue
		}
		if lastWindowID == windowIDS[i] {
			s.values[lastIndex] = call.SingleThreadFunc(s.values[lastIndex], 1)
			continue
		}
		lastWindowID = windowIDS[i]
		base := s.windowOffset[windowIDS[i]]
		index := base + c
		lastIndex = index
		s.values[index] = call.SingleThreadFunc(s.values[index], 1)
		s.validValues[index] = true
		if s.shardIds[windowIDS[i]] < 0 {
			s.shardIds[windowIDS[i]] = int64(cache.shardID)
			s.ptIds[windowIDS[i]] = int32(cache.ptId)
		}
	}
}

func (s *TimeTask) calculateRow(cache *TaskCache) {
	var skip int
	var curVal float64
	for i := range cache.rows {
		row := &cache.rows[i]
		if s.skipRow(row) {
			skip++
			continue
		}
		windowId := s.windowId(row.Timestamp)
		base := s.windowOffset[windowId]
		for c, call := range s.fieldCalls {
			f := getFieldValue(row, call.Name, call.Alias)
			if f < 0 {
				continue
			}
			if call.Call == "count" && !row.StreamOnly {
				curVal = 1
			} else {
				curVal = row.Fields[f].NumValue
			}
			id := base + c
			s.values[id] = call.SingleThreadFunc(s.values[id], curVal)
			s.validValues[id] = true
		}
		if s.shardIds[windowId] < 0 {
			s.shardIds[windowId] = int64(cache.shardId)
			s.ptIds[windowId] = int32(cache.ptId)
		}
	}
	s.stats.AddWindowProcess(int64(len(cache.rows) - skip))
}

func (s *TimeTask) windowId(t int64) int64 {
	return ((t-s.startTimeStamp)/s.window.Nanoseconds() + atomic.LoadInt64(&s.startWindowID)) % s.windowNum
}

func (s *TimeTask) skipRow(row *influx.Row) bool {
	if row.Timestamp < s.startTimeStamp || row.Timestamp >= s.maxTimeStamp {
		if row.Timestamp >= s.endTimeStamp {
			atomic2.CompareAndSwapMaxInt64(&s.stats.WindowOutMaxTime, row.Timestamp)
		} else {
			atomic2.CompareAndSwapMinInt64(&s.stats.WindowOutMinTime, row.Timestamp)
		}
		s.stats.AddWindowSkip(1)
		return true
	}
	return false
}

func getFieldValue(row *influx.Row, name, alias string) int {
	for f := range row.Fields {
		if row.Fields[f].Key == alias || row.Fields[f].Key == name {
			return f
		}
	}
	return -1
}

func initValue(vs []float64, id int, call string) {
	if call == "min" {
		vs[id] = math.MaxFloat64
	} else if call == "max" {
		vs[id] = -math.MaxFloat64
	} else {
		vs[id] = 0
	}
}

// generateRows generate rows from map cache, prepare data for flush
func (s *TimeTask) generateRows() {
	s.offset = int(atomic.LoadInt64(&s.startWindowID)) * s.fieldCallsLen
	s.buildRow()
}

func (s *TimeTask) buildRow() bool {
	// window values only store float64 pointer type, no need to check
	if s.row == nil {
		s.row = &influx.Row{Name: s.info.Name}
	}
	s.row.ReFill()
	// reuse rows body
	fields := &s.row.Fields
	// once make, reuse every flush
	if fields.Len() < len(s.fieldCalls) {
		*fields = make([]influx.Field, len(s.fieldCalls))
		for i := 0; i < fields.Len(); i++ {
			(*fields)[i] = influx.Field{
				Key:  s.fieldCalls[i].Alias,
				Type: s.fieldCalls[i].OutFieldType,
			}
		}
	}
	validNum := 0
	for i := range s.fieldCalls {
		if !s.validValues[s.offset+i] {
			continue
		}
		(*fields)[validNum].NumValue = atomic2.LoadFloat64(&s.values[s.offset+i])
		validNum++
	}
	if validNum == 0 {
		return true
	}
	*fields = (*fields)[:validNum]
	s.row.Timestamp = s.startTimeStamp
	s.indexKeyPool = s.row.UnmarshalIndexKeys(s.indexKeyPool)
	s.validNum++
	return true
}

func (s *TimeTask) flush() error {
	var err error
	s.Logger.Info("stream start flush")
	t := time.Now()
	defer func() {
		s.indexKeyPool = s.indexKeyPool[:0]
		s.stats.StatWindowFlushCost(int64(time.Since(t)))
		s.stats.Push()
		select {
		case s.updateWindow <- struct{}{}:
			return
		case <-s.abort:
			return
		}
	}()

	atomic.StoreInt64(&s.curFlushTime, s.endTimeStamp)
	s.generateRows()
	if s.validNum == 0 {
		return err
	}

	s.rows[0] = *s.row
	err = s.WriteRowsToShard()
	s.Logger.Info("stream flush over")
	s.validNum = 0
	return err
}

func (s *TimeTask) WriteRowsToShard() error {
	windowId := atomic.LoadInt64(&s.startWindowID)
	ptId := uint32(s.ptIds[windowId])
	shardId := uint64(s.shardIds[windowId])

	return s.doWrite(shardId, ptId, s.des.Database, s.des.RetentionPolicy, s.rows)
}

func (s *TimeTask) doWrite(shardId uint64, ptId uint32, db, rp string, rows []influx.Row) error {
	pBuf := bufferpool.GetPoints()
	defer func() {
		bufferpool.PutPoints(pBuf)
	}()

	var err error
	// rows
	pBuf, err = influx.FastMarshalMultiRows(pBuf, s.rows)

	if err != nil {
		s.Logger.Error("stream FastMarshalMultiRows fail", zap.Error(err))
		return err
	}

	err = s.store.WriteRows(db, rp, ptId, shardId, s.rows, pBuf)
	if err != nil {
		s.Logger.Error("stream flush fail", zap.Error(err))
	}
	return nil
}

func (s *TimeTask) Drain() {
	for s.Len() != 0 {
	}
}

func (s *TimeTask) FilterRowsByCond(data ChanData) (bool, error) {
	if data == nil {
		return false, ErrEmptyCache
	}
	cache, ok := data.(*TaskCache)
	if !ok {
		return false, fmt.Errorf("not support type %T", cache)
	}

	// occur release func
	needAgg := s.window > 0
	defer func() {
		if needAgg {
			return
		}
		if cache.release != nil {
			cache.release()
		}
		s.windowCachePool.Put(cache)
	}()

	if s.condition == nil {
		if needAgg {
			s.Put(cache)
		}
		return needAgg, nil
	}

	matchedIndexes := filterRowsByExpr(cache.rows, s.condition)
	if len(matchedIndexes) == 0 {
		return false, nil
	}

	rows := splitMatchedRows(cache.rows, matchedIndexes, s.info, s.isSelectAll)
	if len(rows) == 0 {
		return false, nil
	}

	if needAgg {
		cache.rows = rows
		s.Put(cache)
	} else if cache.streamRows != nil {
		*cache.streamRows = append(*cache.streamRows, rows...)
	}
	return needAgg, nil
}

func splitMatchedRows(srcRows []influx.Row, matchedIndexes []int, dstMstInfo *meta2.MeasurementInfo, isSelectAll bool) []influx.Row {
	dstRows := make([]influx.Row, 0, len(matchedIndexes))
	indexKeyPool := make([]byte, 0)
	for _, idx := range matchedIndexes {
		rrow := &srcRows[idx]
		row := &influx.Row{}
		row.Clone(rrow)
		row.IndexKey = nil
		row.Name = dstMstInfo.Name
		indexKeyPool = row.UnmarshalIndexKeys(indexKeyPool)

		if isSelectAll {
			// select *
			dstRows = append(dstRows, *row)
			continue
		}

		fs := make(influx.Fields, 0, dstMstInfo.Schema.Len())
		for i := range row.Fields {
			field := &row.Fields[i]
			if _, ok := dstMstInfo.Schema.GetTyp(field.Key); ok {
				fs = append(fs, *field)
			}
		}
		if len(fs) == 0 {
			// TODO: Add the option syntax to the create stream statement. Users can choose whether to fill in 0 or discard data.
			continue
		}
		row.Fields = fs
		dstRows = append(dstRows, *row)
	}

	return dstRows
}
