/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	Logger2 "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/stream"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type Engine interface {
	WriteRows(db, rp string, ptId uint32, shardID uint64, streamIdDstShardIdMap map[uint64]uint64, ww WritePointsWorkIF)
	RegisterTask(info *meta2.StreamInfo, fieldCalls []*FieldCall) error
	Drain()
	DeleteTask(id uint64)
	Run()
	Close()
}

type Storage interface {
	WriteRows(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte) error
}

type WritePointsWorkIF interface {
	GetRows() []influx.Row
	PutWritePointsWork()
}

type FieldCall struct {
	name         string
	alias        string
	call         string
	inFieldType  int32
	outFieldType int32
	tagFunc      func(*float64, float64) float64
	timeFunc     func(float64, float64) float64
}

func NewStream(store Storage, Logger Logger, cli MetaClient, conf stream.Config) (Engine, error) {
	cache := make(chan *CacheRow, conf.FilterCache)
	rowPool := NewCacheRowPool()
	bp := NewBuilderPool()
	windowCachePool := NewWindowCachePool()
	goPool, err := ants.NewPool(conf.FilterConcurrency)
	if err != nil {
		return nil, err
	}
	s := &Stream{
		cache:           cache,
		rowPool:         rowPool,
		bp:              bp,
		store:           store,
		stats:           statistics.NewStreamStatistics(),
		Logger:          Logger,
		windowCachePool: windowCachePool,
		goPool:          goPool,
		cli:             cli,
		conf:            conf,
	}
	for i := 0; i < conf.FilterConcurrency; i++ {
		go func() {
			for {
				select {
				case <-s.abort:
					return
				default:
					s.runFilter()
				}
			}
		}()
	}
	return s, nil
}

type Logger interface {
	Error(msg string, fields ...zap.Field)
	Info(msg string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
}

type MetaClient interface {
	GetStreamInfosStore() map[string]*meta2.StreamInfo
	GetMeasurementInfoStore(dbName string, rpName string, mstName string) (*meta2.MeasurementInfo, error)
}

type Stream struct {
	cache chan *CacheRow

	rowPool         *CacheRowPool
	bp              *BuilderPool
	windowCachePool *WindowCachePool
	goPool          *ants.Pool

	//key stream task id
	tasks sync.Map
	stats *statistics.StreamStatistics
	abort chan struct{}

	Logger Logger
	cli    MetaClient
	store  Storage

	conf stream.Config
}

type Task interface {
	run() error
	Drain()
	stop() error
	getName() string
	Put(r *WindowCache)
	getSrcInfo() *meta2.StreamMeasurementInfo
	getDesInfo() *meta2.StreamMeasurementInfo
}

type CacheRow struct {
	rows                  []influx.Row
	db, rp                string
	ptId                  uint32
	shardID               uint64
	refCount              int64
	streamIdDstShardIdMap map[uint64]uint64
	ww                    WritePointsWorkIF
}

func (s *Stream) Run() {
	s.Logger.Info("start stream")
	s.abort = make(chan struct{})
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-s.abort:
			s.Logger.Info("close stream")
			return
		case <-ticker.C:
			streams := s.cli.GetStreamInfosStore()
			if streams == nil {
				s.Logger.Info("get stream is nil")
				continue
			}
			for _, stream := range streams {
				_, exist := s.tasks.Load(stream.ID)
				if exist {
					continue
				}
				srcMst, err := s.cli.GetMeasurementInfoStore(stream.SrcMst.Database, stream.SrcMst.RetentionPolicy, stream.SrcMst.Name)
				if err != nil || srcMst == nil {
					if err != nil {
						s.Logger.Error(fmt.Sprintf("streamName: %s, get src measurement info failed and raise error (%s)", stream.Name, err.Error()))
					} else {
						s.Logger.Error(fmt.Sprintf("streamName: %s, srcMst exist: %v, get src measurement info failed ", stream.Name, srcMst != nil))
					}
					continue
				}
				dstMst, err := s.cli.GetMeasurementInfoStore(stream.DesMst.Database, stream.DesMst.RetentionPolicy, stream.DesMst.Name)
				if err != nil || dstMst == nil {
					if err != nil {
						s.Logger.Error(fmt.Sprintf("streamName: %s, get dst measurement info failed and raise error (%s)", stream.Name, err.Error()))
					} else {
						s.Logger.Error(fmt.Sprintf("streamName: %s, dstMst exist: %v, get dst measurement info failed ", stream.Name, dstMst != nil))
					}
					continue
				}
				canRegisterTask := true
				calls := make([]*FieldCall, len(stream.Calls))
				for i, v := range stream.Calls {
					inFieldType, ok := srcMst.Schema[v.Field]
					if !ok {
						s.Logger.Error(fmt.Sprintf("streamName: %s, srcMst: %s, inField: %s, get input field type failed", stream.Name, srcMst.Name, v.Field))
						canRegisterTask = false
						break
					}
					outFieldType, ok := dstMst.Schema[v.Alias]
					if !ok {
						s.Logger.Error(fmt.Sprintf("streamName: %s, dstMst: %s, outField: %s, get output field type failed", stream.Name, dstMst.Name, v.Alias))
						canRegisterTask = false
						break
					}
					calls[i] = &FieldCall{
						name:         v.Field,
						alias:        v.Alias,
						call:         v.Call,
						inFieldType:  inFieldType,
						outFieldType: outFieldType,
						tagFunc:      nil,
					}
				}
				//TODO detect src schema change
				for i := range stream.Dims {
					ty, ok := srcMst.Schema[stream.Dims[i]]
					if !ok || influx.Field_Type_Tag != ty {
						s.Logger.Error(fmt.Sprintf("streamName: %s, dstMst: %s, dim: %s check fail", stream.Name, dstMst.Name, stream.Dims[i]))
						canRegisterTask = false
						break
					}
				}
				if canRegisterTask {
					err = s.RegisterTask(stream, calls)
					if err != nil {
						s.Logger.Error("register stream task fail", zap.Error(err))
					}
				}
			}
			s.tasks.Range(func(key, value interface{}) bool {
				id, _ := key.(uint64)
				w, _ := value.(Task)
				_, exist := streams[w.getName()]
				if exist {
					return true
				}
				s.DeleteTask(id)
				return true
			})
		}
	}
}

// Close shutdown
func (s *Stream) Close() {
	close(s.abort)
	s.tasks.Range(func(key, value interface{}) bool {
		id, _ := key.(uint64)
		s.DeleteTask(id)
		return true
	})
}

func (s *Stream) DeleteTask(id uint64) {
	s.Logger.Info("delete stream task", zap.String("streamId", strconv.FormatUint(id, 10)))
	v, exist := s.tasks.Load(id)
	if exist {
		w, _ := v.(Task)
		err := w.stop()
		if err != nil {
			s.Logger.Error("task stop fail", zap.String("name", w.getName()),
				zap.Error(err))
		}
		s.tasks.Delete(id)
	}
}

func (s *Stream) RegisterTask(info *meta2.StreamInfo, fieldCalls []*FieldCall) error {
	s.Logger.Info("register stream task", zap.String("streamName", info.Name), zap.String("streamId", strconv.FormatUint(info.ID, 10)))
	start := time.Now().Truncate(info.Interval).Add(info.Interval)
	var logger Logger
	l, ok := s.Logger.(*Logger2.Logger)
	if ok {
		logger = l.With(zap.String("windowName", info.Name))
	} else {
		logger = s.Logger
	}
	//base windowNum is 2, one window for current window, other window for delay data
	windowNum := info.Delay/info.Interval + 2
	if int64(windowNum) > int64(maxWindowNum) {
		return errors.New("maxDelay too big, exceed the maxWindowNum")
	}
	var task Task
	if len(info.Dims) == 0 {
		task = &TimeTask{
			windowNum:       int64(maxWindowNum),
			id:              info.ID,
			src:             info.SrcMst,
			des:             info.DesMst,
			start:           start,
			end:             start.Add(info.Interval),
			window:          info.Interval,
			WindowDataPool:  NewWindowDataPool(),
			fieldCalls:      fieldCalls,
			windowCachePool: s.windowCachePool,
			store:           s.store,
			maxDelay:        info.Delay,
			Logger:          logger,
			name:            info.Name,
			stats:           statistics.NewStreamWindowStatItem(info.ID),
			cli:             s.cli,
		}
	} else {
		task = &TagTask{
			windowNum:       maxWindowNum,
			id:              info.ID,
			values:          sync.Map{},
			src:             info.SrcMst,
			des:             info.DesMst,
			start:           start,
			end:             start.Add(info.Interval),
			window:          info.Interval,
			WindowDataPool:  NewWindowDataPool(),
			goPool:          s.goPool,
			groupKeys:       info.Dims,
			fieldCalls:      fieldCalls,
			bp:              s.bp,
			windowCachePool: s.windowCachePool,
			store:           s.store,
			maxDelay:        info.Delay,
			rows:            []influx.Row{},
			Logger:          logger,
			name:            info.Name,
			concurrency:     s.conf.WindowConcurrency,
			stats:           statistics.NewStreamWindowStatItem(info.ID),
			cli:             s.cli,
		}
	}
	s.tasks.Store(info.ID, task)
	go func() {
		err := task.run()
		if err != nil {
			s.Logger.Error("task run fail", zap.String("name", task.getName()),
				zap.Error(err))
		}
	}()
	return nil
}

func (s *Stream) runFilter() {
	defer func() {
		if err := recover(); err != nil {
			s.Logger.Error("runtime panic", zap.String("stream filter raise stack:", string(debug.Stack())),
				zap.Error(errno.NewError(errno.RecoverPanic, err)))
		}
	}()
	s.filter()
}

func (s *Stream) filter() {
	for {
		var r *CacheRow
		select {
		case r = <-s.cache:
		case <-s.abort:
			return
		}
		s.stats.AddStreamFilter(1)

		release := func() bool {
			cur := atomic.AddInt64(&r.refCount, -1)
			if cur == 0 {
				r.ww.PutWritePointsWork()
				s.rowPool.Put(r)
				return true
			}
			return false
		}
		ref, indexes := s.rangeWindow(r)
		if !ref {
			r.ww.PutWritePointsWork()
			s.rowPool.Put(r)
			continue
		}
		for _, vs := range indexes {
			for j := 0; j < len(vs); j = j + 2 {
				atomic.AddInt64(&r.refCount, 1)
			}
		}
		for i, vs := range indexes {
			for j := 0; j < len(vs); j = j + 2 {
				cache := s.windowCachePool.Get()
				cache.ptId = r.ptId
				cache.shardId = r.streamIdDstShardIdMap[i]
				cache.rows = r.rows[vs[j]:vs[j+1]]
				cache.release = release
				v, ok := s.tasks.Load(i)
				if ok {
					w, _ := v.(Task)
					w.Put(cache)
				}
			}
		}
	}
}

func (s *Stream) rangeWindow(r *CacheRow) (bool, map[uint64][]int) {
	ref := false
	indexs := make(map[uint64][]int)
	s.tasks.Range(func(key, value interface{}) bool {
		i, _ := key.(uint64)
		v, _ := value.(Task)
		if r.db != v.getSrcInfo().Database || r.rp != v.getSrcInfo().RetentionPolicy {
			return true
		}
		s.stats.AddStreamFilterNum(int64(len(r.rows)))
		index, exist := indexs[i]
		if !exist {
			index = []int{}
			indexs[i] = index
		}
		con := false
		startIndex := 0

		for j := range r.rows {
			name := influx.GetOriginMstName(r.rows[j].Name)
			if (name == v.getSrcInfo().Name || name == v.getDesInfo().Name) && util.Include(r.rows[j].StreamId, i) {
				if !con {
					startIndex = j
					con = true
				}
			} else {
				if !con {
					continue
				}
				ref = true
				con = false
				index = append(index, startIndex, j)
			}
		}
		if con {
			ref = true
			index = append(index, startIndex, len(r.rows))
		}
		indexs[i] = index
		return true
	})
	return ref, indexs
}

// Drain is for test case, to check whether there exist resource leakage
func (s *Stream) Drain() {
	//wait rowPool put back all
	for s.rowPool.Len() != s.rowPool.Size() {
	}
	//wait window cache pool empty
	for s.windowCachePool.Count() != 0 {
	}
	s.tasks.Range(func(key, value interface{}) bool {
		w, _ := value.(Task)
		w.Drain()
		return true
	})
}

func (s *Stream) WriteRows(db, rp string, ptId uint32, shardID uint64, streamIdDstShardIdMap map[uint64]uint64,
	ww WritePointsWorkIF,
) {
	r := s.rowPool.Get()
	r.rows = ww.GetRows()
	r.ptId = ptId
	r.shardID = shardID
	r.db = db
	r.rp = rp
	r.streamIdDstShardIdMap = streamIdDstShardIdMap
	r.ww = ww
	r.refCount = 0

	s.stats.AddStreamIn(1)
	s.stats.AddStreamInNum(int64(len(r.rows)))
	s.cache <- r
}
