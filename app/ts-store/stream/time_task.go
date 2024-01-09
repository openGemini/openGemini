/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package stream

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	numenc "github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	atomic2 "github.com/openGemini/openGemini/lib/atomic"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

type TimeTask struct {
	values      []float64
	validValues []bool
	// store startWindow id, for ring store structure
	startWindowID int64
	offset        int
	// current window start time
	start          time.Time
	startTimeStamp int64
	// current window end time
	end          time.Time
	endTimeStamp int64
	maxTimeStamp int64
	// store all ptIds for all window
	ptIds []int32
	// store all shardIds for all window
	shardIds     []int64
	windowOffset []int

	// metadata, not change
	src        *meta2.StreamMeasurementInfo
	des        *meta2.StreamMeasurementInfo
	info       *meta2.MeasurementInfo
	fieldCalls []*FieldCall

	// chan for process
	abort        chan struct{}
	err          error
	updateWindow chan struct{}

	// pool
	windowCachePool *WindowCachePool
	*WindowDataPool
	indexKeyPool []byte

	// config
	id        uint64
	name      string
	windowNum int64
	window    time.Duration
	maxDelay  time.Duration

	// tmp data, reuse
	fieldCallsLen int
	row           *influx.Row
	rows          []influx.Row
	validNum      int
	maxDuration   int64

	// tools
	stats  *statistics.StreamWindowStatItem
	store  Storage
	Logger Logger
	cli    MetaClient
}

func (s *TimeTask) getSrcInfo() *meta2.StreamMeasurementInfo {
	return s.src
}

func (s *TimeTask) getDesInfo() *meta2.StreamMeasurementInfo {
	return s.des
}

func (s *TimeTask) Put(r *WindowCache) {
	s.WindowDataPool.Put(r)
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
	s.info, err = s.cli.GetMeasurementInfoStore(s.des.Database, s.des.RetentionPolicy, s.des.Name)
	if err != nil {
		s.err = err
		return err
	}
	err = s.buildFieldCalls()
	if err != nil {
		return err
	}
	go s.consumeData()
	go s.cycleFlush()
	return nil
}

func (s *TimeTask) initVar() error {
	s.maxDuration = s.windowNum * s.window.Nanoseconds()
	s.abort = make(chan struct{})
	// chan len zero, make updateWindow cannot parallel execute with flush
	s.updateWindow = make(chan struct{})
	s.fieldCallsLen = len(s.fieldCalls)

	s.values = make([]float64, s.fieldCallsLen*int(s.windowNum))
	s.validValues = make([]bool, s.fieldCallsLen*int(s.windowNum))
	for i := 0; i < int(s.windowNum); i++ {
		for j := 0; j < s.fieldCallsLen; j++ {
			id := i*s.fieldCallsLen + j
			initValue(s.values, id, s.fieldCalls[j].call)
		}
	}
	s.windowOffset = make([]int, s.windowNum)
	for i := 0; i < int(s.windowNum); i++ {
		s.windowOffset[i] = i * s.fieldCallsLen
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

func (s *TimeTask) buildFieldCalls() error {
	for c := range s.fieldCalls {
		switch s.fieldCalls[c].call {
		case "min":
			s.fieldCalls[c].timeFunc = func(f float64, f2 float64) float64 {
				if f > f2 {
					return f2
				}
				return f
			}
		case "max":
			s.fieldCalls[c].timeFunc = func(f float64, f2 float64) float64 {
				if f < f2 {
					return f2
				}
				return f
			}
		case "sum":
			s.fieldCalls[c].timeFunc = func(f float64, f2 float64) float64 {
				return f + f2
			}
		case "count":
			s.fieldCalls[c].timeFunc = func(f float64, f2 float64) float64 {
				return f + f2
			}
		default:
			return fmt.Errorf("not support stream func %v", s.fieldCalls[c].call)
		}
	}
	return nil
}

// consume data from window cache, and update window metadata
func (s *TimeTask) consumeData() {
	defer func() {
		if r := recover(); r != nil {
			err := errno.NewError(errno.RecoverPanic, r)
			s.Logger.Error(err.Error())
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
			s.stats.StatWindowOutMinTime(s.start.UnixNano())
			s.stats.StatWindowOutMaxTime(s.end.UnixNano())
			t := time.Now()
			s.walkUpdate(s.values, lastWindowId)
			s.stats.StatWindowUpdateCost(int64(time.Since(t)))
		case <-s.abort:
			return
		case cache := <-s.cache:
			err := s.calculate(cache)
			if err != nil {
				s.Logger.Error("calculate error", zap.String("window", s.name), zap.Error(err))
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
			s.Logger.Error(err.Error())
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
		initValue(vs, j, s.fieldCalls[j].call)
		s.validValues[offset+j] = false
	}
	s.ptIds[lastWindowId] = -1
	s.shardIds[lastWindowId] = -1
	return true
}

func (s *TimeTask) calculate(cache *WindowCache) error {
	// occur release func
	if cache == nil {
		panic("cannot be here")
	}
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
	s.calculateRow2(cache)
	return nil
}

func (s *TimeTask) calculateRow2(cache *WindowCache) {
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
			f := getFieldValue(row, call.name)
			if f < 0 {
				continue
			}
			if call.call == "count" && !row.StreamOnly {
				curVal = 1
			} else {
				curVal = row.Fields[f].NumValue
			}
			id := base + c
			s.values[id] = call.timeFunc(s.values[id], curVal)
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

func getFieldValue(row *influx.Row, name string) int {
	for f := range row.Fields {
		if row.Fields[f].Key == name {
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
				Key:  s.fieldCalls[i].alias,
				Type: s.fieldCalls[i].outFieldType,
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
	s.row.Timestamp = s.start.UnixNano()
	s.row.UnmarshalIndexKeys(s.indexKeyPool)
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
	pBuf := bufferpool.GetPoints()
	defer func() {
		bufferpool.PutPoints(pBuf)
	}()
	windowId := atomic.LoadInt64(&s.startWindowID)
	ptId := uint32(s.ptIds[windowId])
	shardId := uint64(s.shardIds[windowId])

	var err error
	pBuf = append(pBuf[:0], netstorage.PackageTypeFast)
	// db
	pBuf = append(pBuf, uint8(len(s.des.Database)))
	pBuf = append(pBuf, s.des.Database...)
	// rp
	pBuf = append(pBuf, uint8(len(s.des.RetentionPolicy)))
	pBuf = append(pBuf, s.des.RetentionPolicy...)
	// ptid

	pBuf = numenc.MarshalUint32(pBuf, ptId)
	// shardId
	pBuf = numenc.MarshalUint64(pBuf, shardId)
	// rows

	pBuf, err = influx.FastMarshalMultiRows(pBuf, s.rows)

	if err != nil {
		s.Logger.Error("stream FastMarshalMultiRows fail", zap.Error(err))
		return err
	}

	err = s.store.WriteRows(s.des.Database, s.des.RetentionPolicy,
		ptId, shardId, s.rows, pBuf)
	if err != nil {
		s.Logger.Error("stream flush fail", zap.Error(err))
	}
	return nil
}

func (s *TimeTask) Drain() {
	for s.Len() != 0 {
	}
}
