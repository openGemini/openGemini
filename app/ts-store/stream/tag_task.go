// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	atomic2 "github.com/openGemini/openGemini/lib/atomic"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/stringinterner"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/util"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

var (
	maxWindowNum  = 5
	ErrEmptyCache = errors.New("empty window cache")
)

const (
	EmptyGroupKey = ""
	EmptyTagValue = ""
)

type TagTask struct {
	stringDict *stringinterner.StringDict
	// key tag values
	values   sync.Map
	lock     sync.Mutex
	calcLock sync.Mutex
	// store all shardIds for all window
	shardIds map[uint32][]*uint64
	// metadata, not change
	groupKeys []string

	nodePts []uint32
	// chan for process
	innerCache     chan ChanData
	innerRes       chan error
	cleanPreWindow chan struct{}

	// pool
	bp              *strings2.BuilderPool
	windowCachePool *TaskCachePool
	*TaskDataPool

	// config
	concurrency int

	// tools
	goPool *ants.Pool

	// replay
	replayValues   *sync.Map
	replayShardIds map[uint32]*sync.Map
	replayOver     bool
	replayCount    int32
	ptChanged      bool

	ptLoadStatus map[uint32]*flushStatus
	*BaseTask
}

type TaskCache struct {
	rows    []influx.Row
	shardId uint64
	ptId    uint32
	release func()

	streamRows *[]influx.Row // rows for flush directly
}

type ReplayData struct {
	fields  []*float64
	shardID uint64
}

func (s *TagTask) Put(r ChanData) {
	s.TaskDataPool.Put(r)
}

func (s *TagTask) stop() error {
	if s.abort != nil {
		close(s.abort)
	}
	return s.err
}

func (s *TagTask) getSrcInfo() *meta2.StreamMeasurementInfo {
	return s.src
}

func (s *TagTask) getDesInfo() *meta2.StreamMeasurementInfo {
	return s.des
}

func (s *TagTask) getName() string {
	return s.name
}

func (s *TagTask) getLoadStatus() map[uint32]*flushStatus {
	return s.ptLoadStatus
}

func (s *TagTask) run() error {
	var err error
	s.initVar()
	s.info, err = s.cli.Measurement(s.des.Database, s.des.RetentionPolicy, s.des.Name)
	if err != nil {
		s.err = err
		return err
	}
	go s.cycleFlush()
	go s.parallelCalculate()
	go s.cleanWindow()
	go s.consumeDataAndUpdateMeta()
	return nil
}

func (s *TagTask) initVar() {
	s.recoverStatus()
	s.maxDuration = int64(s.windowNum) * s.window.Nanoseconds()
	s.abort = make(chan struct{})
	// chan len zero, make updateWindow cannot parallel execute with flush
	s.updateWindow = make(chan struct{})
	s.cleanPreWindow = make(chan struct{})
	s.fieldCallsLen = len(s.fieldCalls)
	if s.concurrency == 0 {
		s.concurrency = 1
	}
	s.values = sync.Map{}
	s.stringDict = stringinterner.NewStringDict()
	s.innerCache = make(chan ChanData, s.concurrency)
	s.innerRes = make(chan error, s.concurrency)
	s.shardIds = make(map[uint32][]*uint64)
	for _, ptID := range s.nodePts {
		s.shardIds[ptID] = make([]*uint64, maxWindowNum)
		for i := range s.shardIds[ptID] {
			var shardId uint64
			s.shardIds[ptID][i] = &shardId
		}
	}
	s.startTimeStamp = s.start.UnixNano()
	s.endTimeStamp = s.end.UnixNano()
	s.maxTimeStamp = s.startTimeStamp + s.maxDuration
	s.initReplayVar()
	s.Logger.Info("initVar", zap.Any("ptLoadStatus", s.ptLoadStatus))
}

func (s *TagTask) initPtInfo(db string, ptID uint32) {
	if db == s.getSrcInfo().Database {
		s.ptChanged = true
	}
}

func (s *TagTask) initReplayVar() {
	minFlushTime := s.initTime.UnixNano()
	for _, st := range s.ptLoadStatus {
		if st.Timestamp < minFlushTime {
			minFlushTime = st.Timestamp
		}
	}
	atomic.StoreInt32(&s.replayCount, 0)
	s.replayShardIds = make(map[uint32]*sync.Map)
	s.replayValues = &sync.Map{}
	for _, ptID := range s.nodePts {
		if _, ok := s.replayShardIds[ptID]; !ok {
			s.replayShardIds[ptID] = &sync.Map{}
		}
	}
}

func (s *TagTask) updatePTShardInfo() {
	s.recoverStatus()
	s.initTime = time.Now().Truncate(s.window).Add(s.window)
	ptInitTime := s.initTime.UnixNano()
	minFlushTime := ptInitTime
	for _, v := range s.ptLoadStatus {
		if v.Timestamp < minFlushTime {
			minFlushTime = v.Timestamp
		}
	}

	s.replayShardIds = make(map[uint32]*sync.Map)
	s.replayValues.Clear()
	for _, ptID := range s.nodePts {
		if _, ok := s.replayShardIds[ptID]; !ok {
			s.replayShardIds[ptID] = &sync.Map{}
		}
		if !s.ptIDExist(ptID) {
			s.shardIds[ptID] = make([]*uint64, maxWindowNum)
			for i := range s.shardIds[ptID] {
				var shardId uint64
				s.shardIds[ptID][i] = &shardId
			}
		}
	}
}

// cycle flush data form cache, period is group time
func (s *TagTask) cycleFlush() {
	var err error
	defer func() {
		if r := recover(); r != nil {
			err := errno.NewError(errno.RecoverPanic, r)
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
			if reset && s.replayOver {
				err := s.flushReplayData()
				if err != nil {
					s.Logger.Error("stream flush replay data error", zap.Error(err))
				}
				s.cleanReplayData()
				s.replayOver = false
			}

			s.Logger.Info("stream replay data flush over")
			err = s.flush()
			if err != nil {
				s.Logger.Error("stream flush error", zap.Error(err))
			}
		case <-s.abort:
			return
		}
	}
}

// consume data from inner cache, inner cache size equal to concurrency
// TODO share calculate goroutine with other stream task
func (s *TagTask) parallelCalculate() {
	for i := 0; i < s.concurrency; i++ {
		go func() {
			for {
				select {
				case cache := <-s.innerCache:
					switch v := cache.(type) {
					case *TaskCache:
						err := s.calculateRow(v)
						if err != nil {
							s.Logger.Error("calculate error", zap.String("window", s.name), zap.Error(err))
						}
						select {
						case s.innerRes <- err:
						default:
							s.Logger.Error(fmt.Sprintf("stream innerRes is full, cap %d,size %d", cap(s.innerRes), len(s.innerRes)))
						}
					case *CacheRecord:
						err := s.calculateRec(v)
						if err != nil {
							s.Logger.Error("calculate error", zap.String("window", s.name), zap.Error(err))
						}
						select {
						case s.innerRes <- err:
						default:
							s.Logger.Error(fmt.Sprintf("stream innerRes is full, cap %d,size %d", cap(s.innerRes), len(s.innerRes)))
						}
					case *ReplayRow:
						s.stats.AddReplayCacheIn(1)
						atomic.AddInt32(&s.replayCount, 1)
						err := s.replayWalRow(v)
						atomic.AddInt32(&s.replayCount, -1)
						if err != nil {
							s.Logger.Error("calculate error", zap.String("window", s.name), zap.Error(err))
						}
						select {
						case s.innerRes <- err:
						default:
							s.Logger.Error(fmt.Sprintf("stream innerRes is full, cap %d,size %d", cap(s.innerRes), len(s.innerRes)))
						}

					case *LastReplayRow:
						for {
							time.Sleep(1 * time.Second)
							if atomic.LoadInt32(&s.replayCount) == 0 {
								s.replayOver = true
								select {
								case s.innerRes <- nil:
								default:
									s.Logger.Error(fmt.Sprintf("stream innerRes is full, cap %d,size %d", cap(s.innerRes), len(s.innerRes)))
								}
								break
							}
						}
					default:
						s.Logger.Error(fmt.Sprintf("not support type %T", cache))
					}
				case <-s.abort:
					return
				}
			}
		}()
	}
}

// clean old window values, set value nil
// TODO clean window unused key
func (s *TagTask) cleanWindow() {
	for {
		select {
		case _, open := <-s.cleanPreWindow:
			if !open {
				return
			}
			t := time.Now()
			offset := atomic2.LoadModInt64AndADD(&s.startWindowID, -1, int64(s.windowNum))
			s.values.Range(func(key, value any) bool {
				v, ok := value.(*sync.Map)
				if !ok {
					return false
				}
				v.Range(s.walkUpdate(offset))
				return true
			})
			s.stats.StatWindowUpdateCost(int64(time.Since(t)))
		case <-s.abort:
			return
		}
	}
}

func (s *TagTask) cleanPtInfo(ptID uint32) {
	s.values.Delete(ptID)
	delete(s.shardIds, ptID)
	delete(s.ptLoadStatus, ptID)
	for i, p := range s.nodePts {
		if ptID == p {
			s.nodePts = append(s.nodePts[:i], s.nodePts[i+1:]...)
			break
		}
	}
}

func (s *TagTask) cleanReplayData() {
	s.replayValues.Clear()
	s.replayShardIds = make(map[uint32]*sync.Map)
	s.Logger.Info("clean replay data")
}

// consume data from window cache, and update window metadata, calculate cannot parallel with update window
func (s *TagTask) consumeDataAndUpdateMeta() {
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
			atomic2.SetModInt64AndADD(&s.startWindowID, 1, int64(s.windowNum))
			s.stats.Reset()
			s.stats.StatWindowOutMinTime(s.startTimeStamp)
			s.stats.StatWindowOutMaxTime(s.maxTimeStamp)
			if s.ptChanged {
				s.ptChanged = false
				s.updatePTShardInfo()
			}
			select {
			case s.cleanPreWindow <- struct{}{}:
				continue
			case <-s.abort:
				return
			}
		case <-s.abort:
			return
		case cache := <-s.cache:
			s.stats.AddConsumeCacheIn(1)
			s.IncreaseChan()
			count := 0
			s.innerCache <- cache
			count++
			if count < s.concurrency {
				loop := true
				for loop {
					select {
					case c := <-s.cache:
						s.stats.AddConsumeCacheIn(1)
						s.IncreaseChan()
						s.innerCache <- c
						count++
						if count >= s.concurrency {
							loop = false
						}
					default:
						// currently no new data to calculate
						loop = false
					}
				}
			}
			for i := 0; i < count; i++ {
				<-s.innerRes
			}
		}
	}
}

func (s *TagTask) walkUpdate(offset int64) func(k, vv interface{}) bool {
	return func(k, vv interface{}) bool {
		// window values only store float64 pointer type, no need to check
		v, _ := vv.([]*float64)
		vs := v[int(offset)*s.fieldCallsLen : int(offset)*s.fieldCallsLen+s.fieldCallsLen]
		for i := range vs {
			vs[i] = nil
		}
		return true
	}
}

func (s *TagTask) getValue(ptID uint32) *sync.Map {
	v, ok := s.values.Load(ptID)
	if ok {
		return v.(*sync.Map)
	} else {
		s.lock.Lock()
		defer s.lock.Unlock()
		v, ok = s.values.Load(ptID)
		if ok {
			return v.(*sync.Map)
		}
		vv := &sync.Map{}
		s.values.Store(ptID, vv)
		return vv
	}
}

func (s *TagTask) genSyncMapValue(syncMap *sync.Map, key interface{}) *sync.Map {
	v, ok := syncMap.Load(key)
	if ok {
		return v.(*sync.Map)
	} else {
		s.lock.Lock()
		defer s.lock.Unlock()
		v, ok = syncMap.Load(key)
		if ok {
			return v.(*sync.Map)
		}
		vv := &sync.Map{}
		syncMap.Store(key, vv)
		return vv
	}
}

func (s *TagTask) calculateRec(cache *CacheRecord) error {
	if cache == nil {
		return ErrEmptyCache
	}
	defer func() {
		cache.Release()
	}()
	rec := cache.rec
	var skip int
	windowIDS := make([]int8, rec.RowNums())
	timeCol := rec.Column(rec.ColNums() - 1)
	times := timeCol.IntegerValues()
	columnIDs := s.generateRecGroupKeyIndex(s.groupKeys, rec.Schema)
	s.stats.AddWindowIn(int64(rec.RowNums()))
	s.stats.StatWindowStartTime(s.startTimeStamp)
	s.stats.StatWindowEndTime(s.endTimeStamp)
	var lastWindowID int = -1
	callIds := make([]int, len(s.fieldCalls))
	for c, call := range s.fieldCalls {
		id := rec.Schema.FieldIndex(call.Name)
		if id == -1 {
			id = rec.Schema.FieldIndex(call.Alias)
		}
		callIds[c] = id
	}

	if !s.ptIDExist(cache.ptId) {
		return fmt.Errorf("ptId not found,calculateRec")
	}
	for i := 0; i < rec.RowNums(); i++ {
		t := times[i]
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

		values := s.getValue(cache.ptId)
		key := s.generateRecGroupKeyUint(columnIDs, rec, i)
		vv, exist := values.Load(key)
		var vs []*float64
		if !exist {
			vs = make([]*float64, s.fieldCallsLen*int(s.windowNum))
			values.Store(key, vs)
			s.stats.AddWindowGroupKeyCount(1)
		} else {
			vs, _ = vv.([]*float64)
		}
		windowId := s.windowId(t)
		for c := range s.fieldCalls {
			var curVal float64
			// count op, if streamOnly, add value, else add 1
			if s.fieldCalls[c].Call == "count" {
				curVal = 1
			} else {
				val := rec.Column(callIds[c])
				if val == nil {
					continue
				}
				if rec.Schema.Field(callIds[c]).Type == influx.Field_Type_UInt || rec.Schema.Field(callIds[c]).Type == influx.Field_Type_Int {
					v, _ := val.IntegerValue(i)
					curVal = float64(v)
				} else if rec.Schema.Field(callIds[c]).Type == influx.Field_Type_Float {
					curVal, _ = val.FloatValue(i)
				} else {
					continue
				}
			}
			id := s.fieldCallsLen*windowId + c
			if vs[id] == nil {
				var v float64
				if s.fieldCalls[c].Call == "min" {
					v = math.MaxFloat64
				} else if s.fieldCalls[c].Call == "max" {
					v = -math.MaxFloat64
				}
				atomic2.SetAndSwapPointerFloat64(&vs[id], &v)
			}
			s.fieldCalls[c].ConcurrencyFunc(vs[id], curVal)
		}
		if windowId != lastWindowID {
			atomic.SwapUint64(s.shardIds[cache.ptId][windowId], cache.shardID)
			lastWindowID = windowId
		}

		s.stats.AddWindowProcess(1)
	}
	s.stats.AddWindowSkip(int64(skip))
	return nil
}

func (s *TagTask) ptIDExist(ptID uint32) bool {
	_, ok := s.shardIds[ptID]
	return ok
}

func (s *TagTask) windowId(t int64) int {
	return int(((t-s.startTimeStamp)/s.window.Nanoseconds() + atomic.LoadInt64(&s.startWindowID)) % s.windowNum)
}

func (s *TagTask) replayWalRow(cache *ReplayRow) error {
	// occur release func
	if cache == nil {
		return ErrEmptyCache
	}
	flushTime, ok1 := s.ptLoadStatus[cache.ptID]
	ptShardMap, ok2 := s.replayShardIds[cache.ptID]
	if !ok1 || !ok2 {
		return fmt.Errorf("replay ptId not found,ptID: %d,ptLoadStatus: %v，replayShardIds: %v", cache.ptID, s.ptLoadStatus, s.replayShardIds)
	}
	values := s.genSyncMapValue(s.replayValues, cache.ptID)

	rows := cache.rows
	s.stats.AddReplayIn(int64(len(rows)))
	for _, row := range rows {
		if row.Timestamp >= s.initTime.UnixNano() || row.Timestamp < flushTime.Timestamp {
			s.stats.AddReplaySkip(1)
			continue
		}

		windowNum := (row.Timestamp - flushTime.Timestamp) / s.window.Nanoseconds()
		rowTime := windowNum*s.window.Nanoseconds() + flushTime.Timestamp
		vs := s.genSyncMapValue(values, rowTime)

		key := s.generateGroupKeyUint(s.groupKeys, &row)
		vv, exist := vs.Load(key)
		var replayData *ReplayData
		var ok bool
		if !exist {
			s.lock.Lock()
			vv, exist = vs.Load(key)
			if !exist {
				replayData = &ReplayData{
					fields:  make([]*float64, len(s.fieldCalls)),
					shardID: cache.shardID,
				}
				vs.Store(key, replayData)
				ok = true
			} else {
				replayData, ok = vv.(*ReplayData)
				if !ok {
					continue
				}
			}
			s.lock.Unlock()
		} else {
			replayData, ok = vv.(*ReplayData)
			if !ok {
				continue
			}
		}
		for c := range s.fieldCalls {
			var curVal float64
			// count op, if streamOnly, add value, else add 1
			if s.fieldCalls[c].Call == "count" && !row.StreamOnly {
				curVal = 1
			} else {
				for f := range row.Fields {
					if row.Fields[f].Key == s.fieldCalls[c].Name || row.Fields[f].Key == s.fieldCalls[c].Alias {
						curVal = row.Fields[f].NumValue
						break
					}
				}
			}
			if replayData.fields[c] == nil {
				var t float64
				if s.fieldCalls[c].Call == "min" {
					t = math.MaxFloat64
				} else if s.fieldCalls[c].Call == "max" {
					t = -math.MaxFloat64
				}
				atomic2.SetAndSwapPointerFloat64(&replayData.fields[c], &t)
			}
			s.calcLock.Lock()
			s.fieldCalls[c].ConcurrencyFunc(replayData.fields[c], curVal)
			s.calcLock.Unlock()
		}
		ptShardMap.Store(rowTime, &cache.shardID)
		s.stats.AddReplayProcess(1)
	}
	return nil
}

func (s *TagTask) calculateRow(cache *TaskCache) error {
	// occur release func
	if cache == nil {
		return ErrEmptyCache
	}
	defer func() {
		cache.release()
		cache.rows = nil
		s.windowCachePool.Put(cache)
	}()
	rows := cache.rows
	s.stats.AddWindowIn(int64(len(rows)))
	s.stats.StatWindowStartTime(s.startTimeStamp)
	s.stats.StatWindowEndTime(s.endTimeStamp)

	values := s.getValue(cache.ptId)
	if !s.ptIDExist(cache.ptId) {
		return fmt.Errorf("ptId not found,calculateRow")
	}
	for i := range rows {
		row := rows[i]
		if row.Timestamp < s.startTimeStamp || row.Timestamp >= s.maxTimeStamp {
			if row.Timestamp >= s.endTimeStamp {
				atomic2.CompareAndSwapMaxInt64(&s.stats.WindowOutMaxTime, row.Timestamp)
			} else {
				atomic2.CompareAndSwapMinInt64(&s.stats.WindowOutMinTime, row.Timestamp)
			}
			s.stats.AddWindowSkip(1)
			continue
		}
		key := s.generateGroupKeyUint(s.groupKeys, &row)
		vv, exist := values.Load(key)
		var vs []*float64
		if !exist {
			s.lock.Lock()
			vv, exist = values.Load(key)
			if !exist {
				vs = make([]*float64, s.fieldCallsLen*int(s.windowNum))
				values.Store(key, vs)
				s.stats.AddWindowGroupKeyCount(1)
			} else {
				vs, _ = vv.([]*float64)
			}
			s.lock.Unlock()
		} else {
			vs, _ = vv.([]*float64)
		}
		windowId := int(((row.Timestamp-s.start.UnixNano())/s.window.Nanoseconds() + atomic.LoadInt64(&s.startWindowID)) % s.windowNum)
		for c := range s.fieldCalls {
			var curVal float64
			// count op, if streamOnly, add value, else add 1
			if s.fieldCalls[c].Call == "count" && !row.StreamOnly {
				curVal = 1
			} else {
				for f := range row.Fields {
					if row.Fields[f].Key == s.fieldCalls[c].Name || row.Fields[f].Key == s.fieldCalls[c].Alias {
						curVal = row.Fields[f].NumValue
						break
					}
				}
			}
			id := s.fieldCallsLen*windowId + c
			if vs[id] == nil {
				var t float64
				if s.fieldCalls[c].Call == "min" {
					t = math.MaxFloat64
				} else if s.fieldCalls[c].Call == "max" {
					t = -math.MaxFloat64
				}
				atomic2.SetAndSwapPointerFloat64(&vs[id], &t)
			}
			s.calcLock.Lock()
			s.fieldCalls[c].ConcurrencyFunc(vs[id], curVal)
			s.calcLock.Unlock()
		}
		atomic.SwapUint64(s.shardIds[cache.ptId][windowId], cache.shardId)
		s.stats.AddWindowProcess(1)
	}

	return nil
}

func (s *TagTask) buildRow() func(k, vv any) bool {
	return func(k any, vv any) bool {
		key, _ := k.(string)
		// window values only store float64 pointer type, no need to check
		v, _ := vv.([]*float64)
		if s.validNum >= len(s.rows) {
			s.rows = append(s.rows, influx.Row{Name: s.info.Name})
		}
		s.rows[s.validNum].ReFill()
		// reuse rows body
		fields := &s.rows[s.validNum].Fields
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
			if v[s.offset+i] == nil {
				continue
			}
			(*fields)[validNum].NumValue = atomic2.LoadFloat64(v[s.offset+i])
			validNum++
		}

		if validNum == 0 {
			return true
		}

		*fields = (*fields)[:validNum]
		tags := &s.rows[s.validNum].Tags
		if key == EmptyGroupKey {
			if len(s.groupKeys) != 0 {
				s.Logger.Error("buildRow fail", zap.Error(fmt.Errorf("cannot occur this groupkeys %v key %v", key, s.groupKeys)))
				return true
			}
		} else {
			var err error
			values := strings.Split(key, config.StreamGroupValueStrSeparator)
			if len(values) != len(s.groupKeys) {
				s.Logger.Error("buildRow fail", zap.Error(fmt.Errorf("cannot occur this values %v len %v groupkeys %v key %v", values, len(values), s.groupKeys, key)))
				return true
			}
			validNum = 0
			// once make, reuse every flush
			if tags.Len() < len(s.groupKeys) {
				*tags = make([]influx.Tag, len(s.groupKeys))
				for i := 0; i < tags.Len(); i++ {
					(*tags)[i] = influx.Tag{}
				}
			}
			for i := range s.groupKeys {
				value := values[i]
				value, err = s.unCompressDictKey(value)
				if err != nil {
					s.Logger.Error("unCompressDictKey fail", zap.Error(err))
					return true
				}
				// empty value, skip
				if value == EmptyTagValue {
					continue
				}
				(*tags)[validNum].Value = value
				(*tags)[validNum].Key = s.groupKeys[i]
				validNum++
			}
			*tags = (*tags)[:validNum]
		}
		s.Logger.Info("flush stream point", zap.String("name", s.rows[s.validNum].Name), zap.Int64("flushTime", s.startTimeStamp), zap.Any("fields", s.rows[s.validNum].Fields))
		s.rows[s.validNum].Timestamp = s.startTimeStamp
		s.indexKeyPool = s.rows[s.validNum].UnmarshalIndexKeys(s.indexKeyPool)
		s.validNum++
		return true
	}
}

func (s *TagTask) buildReplayRow(rowTime int64) func(k, vv any) bool {
	return func(k any, vv any) bool {
		key, _ := k.(string)
		replayData, _ := vv.(*ReplayData)
		v := replayData.fields
		if s.validNum >= len(s.rows) {
			s.rows = append(s.rows, influx.Row{Name: s.info.Name})
		}
		s.rows[s.validNum].ReFill()
		// reuse rows body
		fields := &s.rows[s.validNum].Fields
		// once make, reuse every flush
		if fields.Len() < len(s.fieldCalls) {
			*fields = make([]influx.Field, len(s.fieldCalls))
			for i := 0; i < len(s.fieldCalls); i++ {
				(*fields)[i] = influx.Field{
					Key:      s.fieldCalls[i].Alias,
					Type:     s.fieldCalls[i].OutFieldType,
					NumValue: atomic2.LoadFloat64(v[i]),
				}
			}
		}

		tags := &s.rows[s.validNum].Tags
		if key == EmptyGroupKey {
			if len(s.groupKeys) != 0 {
				s.Logger.Error("buildRow fail", zap.Error(fmt.Errorf("cannot occur this groupkeys %v key %v", key, s.groupKeys)))
				return true
			}
		} else {
			var err error
			values := strings.Split(key, config.StreamGroupValueStrSeparator)
			if len(values) != len(s.groupKeys) {
				s.Logger.Error("buildRow fail", zap.Error(fmt.Errorf("cannot occur this values %v len %v groupkeys %v key %v", values, len(values), s.groupKeys, key)))
				return true
			}
			validNum := 0
			// once make, reuse every flush
			if tags.Len() < len(s.groupKeys) {
				*tags = make([]influx.Tag, len(s.groupKeys))
				for i := 0; i < len(s.groupKeys); i++ {
					(*tags)[i] = influx.Tag{}
				}
			}
			for i := range s.groupKeys {
				value := values[i]
				value, err = s.unCompressDictKey(value)
				if err != nil {
					s.Logger.Error("unCompressDictKey fail", zap.Error(err))
					return true
				}
				// empty value, skip
				if value == EmptyTagValue {
					continue
				}
				(*tags)[validNum].Value = value
				(*tags)[validNum].Key = s.groupKeys[i]
				validNum++
			}
			*tags = (*tags)[:validNum]
		}
		s.Logger.Info("replay stream point", zap.String("name", s.rows[s.validNum].Name), zap.Int64("flushTime", rowTime), zap.Any("fields", s.rows[s.validNum].Fields))
		s.rows[s.validNum].Timestamp = rowTime
		s.indexKeyPool = s.rows[s.validNum].UnmarshalIndexKeys(s.indexKeyPool)
		s.validNum++
		return true
	}
}

func (s *TagTask) flushReplayData() error {
	var err error
	s.Logger.Info("stream replay data start flush", zap.Int64("endTime", s.endTimeStamp))
	s.indexKeyPool = bufferpool.GetPoints()
	s.rows = s.rows[:0]

	s.generateReplayRows()

	return err
}

func (s *TagTask) generateReplayRows() {
	s.replayValues.Range(func(key, value any) bool {
		v, ok := value.(*sync.Map)
		if !ok {
			return false
		}
		ptID, ok := key.(uint32)
		if !ok {
			return false
		}
		v.Range(func(k any, vv any) bool {
			timeV, ok := vv.(*sync.Map)
			if !ok {
				return false
			}
			rowTime, ok := k.(int64)
			if !ok {
				return false
			}
			timeV.Range(s.buildReplayRow(rowTime))
			s.WriteReplayRows(rowTime, ptID)
			return true
		})
		return true
	})
	for ptID := range s.ptLoadStatus {
		s.snapshot(ptID, true)
	}

}

func (s *TagTask) WriteReplayRows(rowTime int64, ptID uint32) {
	if s.validNum == 0 {
		return
	}

	s.Logger.Info("replay data point", zap.Any("rows", s.rows))
	shardID, ok := s.replayShardIds[ptID].Load(rowTime)
	if !ok {
		s.Logger.Error("WriteRowsToShard fail,replayShardIds not found", zap.Uint32("ptID", ptID), zap.Int64("rowTime", rowTime))
	}
	err := s.WriteRowsToShard(0, s.validNum, ptID, *shardID.(*uint64))
	if err != nil {
		s.Logger.Error("WriteRowsToShard fail", zap.Error(err))
	}
	s.rows = s.rows[:0]
	s.validNum = 0
}

func (s *TagTask) flush() error {
	var err error
	s.Logger.Info("stream start flush", zap.Int64("endTime", s.endTimeStamp))
	t := time.Now()
	s.indexKeyPool = bufferpool.GetPoints()
	defer func() {
		bufferpool.Put(s.indexKeyPool)
		s.indexKeyPool = nil
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
	s.Logger.Info("stream flush over", zap.Int64("flushTime", s.curFlushTime))
	return err
}

// generateRows generate rows from map cache, prepare data for flush
func (s *TagTask) generateRows() {
	s.offset = int(atomic.LoadInt64(&s.startWindowID)) * s.fieldCallsLen

	s.Logger.Info("generateRows")
	s.values.Range(func(key, value any) bool {
		v, ok := value.(*sync.Map)
		if !ok {
			return false
		}
		v.Range(s.buildRow())
		s.WriteRows(key.(uint32))
		return true
	})
	for ptID := range s.ptLoadStatus {
		s.snapshot(ptID, true)
	}
}

func (s *TagTask) WriteRows(ptID uint32) {
	var err error
	if s.validNum == 0 {
		return
	}
	if !s.ptIDExist(ptID) {
		return
	}
	shardID := atomic.LoadUint64(s.shardIds[ptID][s.startWindowID])
	err = s.WriteRowsToShard(0, s.validNum, ptID, shardID)
	if err != nil {
		s.Logger.Error("WriteRowsToShard fail", zap.Error(err))
	}
	s.rows = s.rows[:0]
	s.validNum = 0

}

func (s *TagTask) recoverStatus() {
	s.ptLoadStatus = map[uint32]*flushStatus{}
	pts := s.cli.GetNodePT(s.des.Database)
	s.nodePts = pts
	for _, ptID := range pts {
		s.recoverPTStatus(ptID)
	}
	s.setInit()
}

func (s *TagTask) recoverPTStatus(ptID uint32) {
	p := path.Join(s.dataPath, "data", s.des.Database, strconv.Itoa(int(ptID)), s.des.RetentionPolicy, strconv.FormatUint(s.id, 10))
	by, err := fileops.ReadFile(p)
	if err != nil {
		s.Logger.Error("recoverStatus read file error", zap.Error(err), zap.String("path", p))
		return
	}
	st := &flushStatus{}
	err = json.Unmarshal(by, st)
	if err != nil {
		s.Logger.Error("recoverStatus Unmarshal error", zap.Error(err))
		return
	}
	s.ptLoadStatus[ptID] = st
	s.Logger.Info("recoverPTStatus", zap.Uint32("ptID", ptID), zap.Int64("time", st.Timestamp))
}

func (s *TagTask) snapshot(ptID uint32, updatePT bool) {
	st := &flushStatus{Timestamp: atomic.LoadInt64(&s.curFlushTime)}
	by, err := json.Marshal(st)
	if err != nil {
		s.Logger.Error("Marshal flushStatus fail", zap.Error(err))
		return
	}

	p := path.Join(s.dataPath, "data", s.des.Database, strconv.Itoa(int(ptID)), s.des.RetentionPolicy)
	if _, err = fileops.Stat(p); err != nil {
		err = fileops.MkdirAll(p, 0700)
		if err != nil {
			s.Logger.Error("snapshot file mkdir failed", zap.Error(err))
		}
	}
	if err := fileops.WriteFile(path.Join(p, strconv.FormatUint(s.id, 10)), by, 0600); err != nil {
		s.Logger.Error("snapshot file write error", zap.Error(err))
	}
	if updatePT {
		s.ptLoadStatus[ptID] = st
	}

	s.Logger.Info("stream snapshot suc", zap.String("path", p), zap.Uint32("ptID", ptID), zap.Int64("flushTime", s.curFlushTime), zap.ByteString("byte", by))
}

type flushStatus struct {
	Timestamp int64 `json:"timestamp"`
}

func (s *TagTask) WriteRowsToShard(start, end int, ptID uint32, shardID uint64) error {
	pBuf := bufferpool.GetPoints()
	defer func() {
		bufferpool.PutPoints(pBuf)
	}()

	var err error
	// rows
	pBuf, err = influx.FastMarshalMultiRows(pBuf, s.rows[start:end])

	if err != nil {
		s.Logger.Error("stream FastMarshalMultiRows fail", zap.Error(err))
		return err
	}

	err = s.store.WriteRows(s.des.Database, s.des.RetentionPolicy, ptID, shardID, s.rows[start:end], pBuf)
	if err != nil {
		s.Logger.Error("stream flush fail", zap.Error(err))
	}

	return nil
}

// corpusIndexes array not need lock
func (s *TagTask) unCompressDictKey(key string) (string, error) {
	if key == EmptyTagValue {
		return EmptyTagValue, nil
	}
	intV, err := strconv.Atoi(key)
	if err != nil {
		s.Logger.Error(fmt.Sprintf("invalid corpus key %v", key))
		return "", err
	}
	return s.stringDict.LoadValue(intV), nil
}

func (s *TagTask) Drain() {
	for s.bp.Len() != s.bp.Size() {
	}
	for s.Len() != 0 {
	}
}

// generateGroupKeyUint not support fieldIndex
func (s *TagTask) generateRecGroupKeyIndex(keys []string, schema record.Schemas) []int {
	if len(keys) == 0 {
		return nil
	}

	columnIDs := make([]int, len(keys))
	tagIndex := 0
	for i := range keys {
		idx := util.Search(tagIndex, len(schema), func(j int) bool { return schema[j].Name >= keys[i] })
		if idx < len(schema) && schema[idx].Name == keys[i] {
			tagIndex = idx + 1
			if schema[idx].IsString() {
				columnIDs[i] = idx
			} else {
				columnIDs[i] = -1
			}
			continue
		}
		tagIndex = idx + 1
		columnIDs[i] = -1
	}
	return columnIDs
}

func (s *TagTask) generateRecGroupKeyUint(columnIDs []int, value *record.Record, row int) string {
	builder := s.bp.Get()
	defer func() {
		builder.Reset()
		s.bp.Put(builder)
	}()

	for i, id := range columnIDs {
		if id == -1 {
			if i < len(columnIDs)-1 {
				builder.AppendByte(config.StreamGroupValueSeparator)
			}
			continue
		}
		val := value.Column(id)
		if val == nil {
			if i < len(columnIDs)-1 {
				builder.AppendByte(config.StreamGroupValueSeparator)
			}
			continue
		}
		str, _ := val.StringValue(row)
		v := s.stringDict.LoadIndex(string(str))
		builder.AppendString(strconv.FormatUint(v, 10))
		if i < len(columnIDs)-1 {
			builder.AppendByte(config.StreamGroupValueSeparator)
		}
	}

	return builder.NewString()
}

// generateGroupKeyUint not support fieldIndex
func (s *TagTask) generateGroupKeyUint(keys []string, value *influx.Row) string {
	if len(keys) == 0 {
		return EmptyGroupKey
	}
	builder := s.bp.Get()
	defer func() {
		builder.Reset()
		s.bp.Put(builder)
	}()

	tagIndex := 0
	for i := range keys {
		idx := util.Search(tagIndex, len(value.Tags), func(j int) bool { return value.Tags[j].Key >= keys[i] })
		if idx < len(value.Tags) && value.Tags[idx].Key == keys[i] {
			v := s.stringDict.LoadIndex(value.Tags[idx].Value)
			builder.AppendString(strconv.FormatUint(v, 10))
			if i < len(keys)-1 {
				builder.AppendByte(config.StreamGroupValueSeparator)
			}
			tagIndex = idx + 1
			continue
		}
		if i < len(keys)-1 {
			builder.AppendByte(config.StreamGroupValueSeparator)
		}
		tagIndex = idx + 1
	}
	return builder.NewString()
}

func (s *TagTask) FilterRowsByCond(data ChanData) (bool, error) {
	cache, ok := data.(*TaskCache)
	if !ok {
		return false, fmt.Errorf("not support type %T", cache)
	}

	// occur release func
	if cache == nil {
		return false, ErrEmptyCache
	}
	if s.condition == nil {
		s.Put(cache)
		return true, nil
	}

	matchedIndexes := filterRowsByExpr(cache.rows, s.condition)
	if len(matchedIndexes) == 0 {
		return false, nil
	}
	rows := splitMatchedRows(cache.rows, matchedIndexes, s.info, s.isSelectAll)
	if len(rows) == 0 {
		return false, nil
	}

	cache.rows = rows
	s.Put(cache)
	return true, nil
}

func (s *TagTask) getCleanTimestamp() int64 {
	cleanTime := s.curFlushTime
	for _, pt := range s.ptLoadStatus {
		if cleanTime > pt.Timestamp {
			cleanTime = pt.Timestamp
		}
	}
	return cleanTime - int64(s.window) - int64(s.maxDelay)
}
