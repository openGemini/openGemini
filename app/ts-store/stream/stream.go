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

package stream

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	Logger2 "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	streamLib "github.com/openGemini/openGemini/lib/stream"
	"github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/stream"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type Engine interface {
	WriteRows(db, rp string, ptId uint32, shardID uint64, streamIdDstShardIdMap map[uint64]uint64, ww WritePointsWorkIF)
	WriteRec(db, rp, mst string, ptId uint32, shardID uint64, rec *record.Record, binaryRec []byte) error
	RegisterTask(info *meta.StreamInfo, fieldCalls []*streamLib.FieldCall) error
	Drain()
	DeleteTask(id uint64)
	Run()
	Close()
}

type Storage interface {
	WriteRows(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte) error
	WriteRec(db, rp, mst string, ptId uint32, shardID uint64, rec *record.Record, binaryRec []byte) error
}

type WritePointsWorkIF interface {
	GetRows() []influx.Row
	PutWritePointsWork()
}

func NewStream(store Storage, Logger Logger, cli MetaClient, conf stream.Config) (Engine, error) {
	cache := make(chan ChanData, conf.FilterCache)
	rowPool := NewCacheRowPool()
	bp := strings.NewBuilderPool()
	windowCachePool := NewTaskCachePool()
	goPool, err := ants.NewPool(conf.FilterConcurrency)
	if err != nil {
		return nil, err
	}
	s := &Stream{
		cache:           cache,
		abort:           make(chan struct{}),
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
	GetStreamInfos() map[string]*meta.StreamInfo
	Measurement(dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error)
}

type Stream struct {
	cache           chan ChanData
	rowPool         *CacheRowPool
	bp              *strings.BuilderPool
	windowCachePool *TaskCachePool
	goPool          *ants.Pool

	//key stream task id
	tasks sync.Map
	stats *statistics.StreamStatistics
	abort chan struct{}

	Logger Logger
	cli    MetaClient
	store  Storage
	conf   stream.Config
}

type Task interface {
	run() error
	Drain()
	stop() error
	getName() string
	Put(r ChanData)
	getSrcInfo() *meta.StreamMeasurementInfo
	getDesInfo() *meta.StreamMeasurementInfo
}

type ChanData interface {
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

type CacheRecord struct {
	rec         *record.Record
	db, rp, mst string
	ptId        uint32
	shardID     uint64
	wg          sync.WaitGroup
}

func (r *CacheRecord) Wait() {
	r.wg.Wait()
}

func (r *CacheRecord) Retain() {
	r.wg.Add(1)
}

func (r *CacheRecord) RetainNum(num int) {
	r.wg.Add(num)
}

func (r *CacheRecord) Release() {
	r.wg.Done()
}

func (s *Stream) Run() {
	s.Logger.Info("start stream")
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-s.abort:
			s.Logger.Info("close stream")
			return
		case <-ticker.C:
			streams := s.cli.GetStreamInfos()
			if streams == nil {
				continue
			}
			for _, info := range streams {
				_, exist := s.tasks.Load(info.ID)
				if exist {
					continue
				}
				srcMst, err := s.cli.Measurement(info.SrcMst.Database, info.SrcMst.RetentionPolicy, info.SrcMst.Name)
				if err != nil || srcMst == nil {
					if err != nil {
						s.Logger.Error(fmt.Sprintf("streamName: %s, get src measurement info failed and raise error (%s)", info.Name, err.Error()))
					} else {
						s.Logger.Error(fmt.Sprintf("streamName: %s, srcMst exist: %v, get src measurement info failed ", info.Name, srcMst != nil))
					}
					continue
				}
				dstMst, err := s.cli.Measurement(info.DesMst.Database, info.DesMst.RetentionPolicy, info.DesMst.Name)
				if err != nil || dstMst == nil {
					if err != nil {
						s.Logger.Error(fmt.Sprintf("streamName: %s, get dst measurement info failed and raise error (%s)", info.Name, err.Error()))
					} else {
						s.Logger.Error(fmt.Sprintf("streamName: %s, dstMst exist: %v, get dst measurement info failed ", info.Name, dstMst != nil))
					}
					continue
				}
				srcSchema := srcMst.CloneSchema()
				dstSchema := dstMst.CloneSchema()
				canRegisterTask := true
				calls := make([]*streamLib.FieldCall, len(info.Calls))
				for i, v := range info.Calls {
					inFieldType, ok := srcSchema.GetTyp(v.Field)
					if !ok {
						s.Logger.Error(fmt.Sprintf("streamName: %s, srcMst: %s, inField: %s, get input field type failed", info.Name, srcMst.Name, v.Field))
						canRegisterTask = false
						break
					}
					outFieldType, ok := dstSchema.GetTyp(v.Alias)
					if !ok {
						s.Logger.Error(fmt.Sprintf("streamName: %s, dstMst: %s, outField: %s, get output field type failed", info.Name, dstMst.Name, v.Alias))
						canRegisterTask = false
						break
					}
					calls[i], err = streamLib.NewFieldCall(inFieldType, outFieldType, v.Field, v.Alias, v.Call, len(info.Dims) != 0)
					if err != nil {
						s.Logger.Error(fmt.Sprintf("streamName: %s, dstMst: %s, outField: %s, new stream call failed", info.Name, dstMst.Name, v.Alias), zap.Error(err))
						canRegisterTask = false
						break
					}
				}
				//TODO detect src schema change
				for i := range info.Dims {
					ty, ok := srcSchema.GetTyp(info.Dims[i])
					if !ok || influx.Field_Type_Tag != ty {
						s.Logger.Error(fmt.Sprintf("streamName: %s, dstMst: %s, dim: %s check fail", info.Name, dstMst.Name, info.Dims[i]))
						canRegisterTask = false
						break
					}
				}
				if canRegisterTask {
					sort.Sort(streamLib.FieldCalls(calls))
					err = s.RegisterTask(info, calls)
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

func (s *Stream) RegisterTask(info *meta.StreamInfo, fieldCalls []*streamLib.FieldCall) error {
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
			TaskDataPool:    NewTaskDataPool(),
			windowCachePool: s.windowCachePool,
			BaseTask: &BaseTask{
				windowNum:  int64(maxWindowNum),
				id:         info.ID,
				src:        info.SrcMst,
				des:        info.DesMst,
				start:      start,
				end:        start.Add(info.Interval),
				window:     info.Interval,
				fieldCalls: fieldCalls,
				store:      s.store,
				maxDelay:   info.Delay,
				Logger:     logger,
				name:       info.Name,
				stats:      statistics.NewStreamWindowStatItem(info.ID),
				cli:        s.cli,
			},
		}
	} else {
		task = &TagTask{
			values:          sync.Map{},
			TaskDataPool:    NewTaskDataPool(),
			goPool:          s.goPool,
			groupKeys:       info.Dims,
			bp:              s.bp,
			windowCachePool: s.windowCachePool,
			concurrency:     s.conf.WindowConcurrency,
			BaseTask: &BaseTask{
				windowNum:  int64(maxWindowNum),
				id:         info.ID,
				src:        info.SrcMst,
				des:        info.DesMst,
				start:      start,
				end:        start.Add(info.Interval),
				window:     info.Interval,
				fieldCalls: fieldCalls,
				store:      s.store,
				maxDelay:   info.Delay,
				rows:       []influx.Row{},
				Logger:     logger,
				name:       info.Name,
				stats:      statistics.NewStreamWindowStatItem(info.ID),
				cli:        s.cli,
			},
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
		select {
		case c := <-s.cache:
			switch r := c.(type) {
			case *CacheRow:
				s.stats.AddStreamFilter(1)
				release := func() {
					if atomic.AddInt64(&r.refCount, -1) == 0 {
						r.ww.PutWritePointsWork()
						s.rowPool.Put(r)
						return
					}
				}
				ref, indexes := s.rowsRangeTask(r)
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
			case *CacheRecord:
				s.stats.AddStreamFilter(1)
				ref, indexes := s.recordRangeTask(r)
				if !ref {
					continue
				}
				r.RetainNum(len(indexes))
				for i := range indexes {
					v, ok := s.tasks.Load(i)
					if ok {
						w, _ := v.(Task)
						w.Put(r)
					}
				}
			default:
				continue
			}
		case <-s.abort:
			return
		}
	}
}

func (s *Stream) rowsRangeTask(r *CacheRow) (bool, map[uint64][]int) {
	ref := false
	indexes := make(map[uint64][]int)
	s.tasks.Range(func(key, value interface{}) bool {
		i, _ := key.(uint64)
		v, _ := value.(Task)
		if r.db != v.getSrcInfo().Database || r.rp != v.getSrcInfo().RetentionPolicy {
			return true
		}
		s.stats.AddStreamFilterNum(int64(len(r.rows)))
		index, exist := indexes[i]
		if !exist {
			index = []int{}
			indexes[i] = index
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
		indexes[i] = index
		return true
	})
	return ref, indexes
}

func (s *Stream) recordRangeTask(r *CacheRecord) (bool, map[uint64]struct{}) {
	ref := false
	indexes := make(map[uint64]struct{})
	s.tasks.Range(func(key, value interface{}) bool {
		i, _ := key.(uint64)
		v, _ := value.(Task)
		name := influx.GetOriginMstName(r.mst)
		if r.db != v.getSrcInfo().Database || r.rp != v.getSrcInfo().RetentionPolicy || name != v.getSrcInfo().Name {
			return true
		}
		s.stats.AddStreamFilterNum(int64(r.rec.RowNums()))
		indexes[i] = struct{}{}
		ref = true
		return true
	})
	return ref, indexes
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

func (s *Stream) WriteRec(db, rp, mst string, ptId uint32, shardID uint64, rec *record.Record, binaryRec []byte) error {
	err := s.store.WriteRec(db, rp, mst, ptId, shardID, rec, binaryRec)
	if err == nil {
		s.stats.AddStreamIn(1)
		s.stats.AddStreamInNum(int64(rec.RowNums()))
		r := &CacheRecord{
			rec:     rec,
			db:      db,
			rp:      rp,
			mst:     mst,
			ptId:    ptId,
			shardID: shardID,
		}
		s.cache <- r
		r.Wait()
		return nil
	}
	return err
}
