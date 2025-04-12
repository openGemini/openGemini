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
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/errno"
	Logger2 "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	streamLib "github.com/openGemini/openGemini/lib/stream"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/stream"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

const WalFilePathReg = "/wal/(\\w+)/(\\d+)/(\\w+)/(\\d+)_(\\d+)_(\\d+)_(\\d+)/(.*)"

type Engine interface {
	WriteRows(writeCtx *WriteStreamRowsCtx) (bool, error)
	WriteRec(db, rp, mst string, ptId uint32, shardID uint64, rec *record.Record, binaryRec []byte) error
	WriteReplayRows(db, rp string, ptId uint32, shardID uint64, ww WritePointsWorkIF)
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
	Ref()
	UnRef() int64
}

func NewStream(store Storage, Logger Logger, cli MetaClient, conf stream.Config, dataPath, walPath string, ptNumPerNode uint32) (Engine, error) {
	stream.SetWriteStreamPointsEnabled(conf.WriteEnabled)
	cache := make(chan ChanData, conf.FilterCache)
	rowPool := NewCacheRowPool()
	bp := strings2.NewBuilderPool()
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
		dataPath:        dataPath,
		walPath:         walPath,
		ptNumPerNode:    ptNumPerNode,
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
	GetNodePT(database string) []uint32
}

type Stream struct {
	cache           chan ChanData
	rowPool         *CacheRowPool
	bp              *strings2.BuilderPool
	windowCachePool *TaskCachePool
	goPool          *ants.Pool

	//key stream task id
	tasks   sync.Map
	taskNum int32
	stats   *statistics.StreamStatistics
	abort   chan struct{}

	Logger       Logger
	cli          MetaClient
	store        Storage
	conf         stream.Config
	dataPath     string
	walPath      string
	ptNumPerNode uint32
}

type Task interface {
	run() error
	Drain()
	stop() error
	getName() string
	Put(r ChanData)
	getSrcInfo() *meta.StreamMeasurementInfo
	getDesInfo() *meta.StreamMeasurementInfo
	getCurrentTimestamp() int64
	getLoadStatus() map[uint32]*flushStatus
	IsInit() bool
	FilterRowsByCond(cache ChanData) (bool, error)
}

type ChanData interface {
}

type Rows interface {
	GetRows() []influx.Row
	GetDB() string
	GetRP() string
	IsStreamRow(name string, v Task, row influx.Row, key uint64) bool
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

func (r *CacheRow) GetRows() []influx.Row {
	return r.rows
}

func (r *CacheRow) GetDB() string {
	return r.db
}

func (r *CacheRow) GetRP() string {
	return r.rp
}

func (r *CacheRow) IsStreamRow(name string, v Task, row influx.Row, key uint64) bool {
	return (name == v.getSrcInfo().Name || name == v.getDesInfo().Name) && util.Include(row.StreamId, key)
}

type ReplayRow struct {
	rows    []influx.Row
	db, rp  string
	ptID    uint32
	shardID uint64
}

func (r *ReplayRow) GetRows() []influx.Row {
	return r.rows
}

func (r *ReplayRow) GetDB() string {
	return r.db
}

func (r *ReplayRow) GetRP() string {
	return r.rp
}

func (r *ReplayRow) IsStreamRow(name string, v Task, row influx.Row, key uint64) bool {
	return name == v.getSrcInfo().Name || name == v.getDesInfo().Name
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

func (s *Stream) updateTask() {
	streams := s.cli.GetStreamInfos()
	if streams == nil {
		return
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
			if len(info.Dims) == 0 && info.Interval == 0 {
				// for filter only
				calls[i] = &streamLib.FieldCall{
					InFieldType:  inFieldType,
					OutFieldType: outFieldType,
					Name:         v.Field,
					Alias:        v.Alias,
				}
			} else {
				calls[i], err = streamLib.NewFieldCall(inFieldType, outFieldType, v.Field, v.Alias, v.Call, len(info.Dims) != 0)
				if err != nil {
					s.Logger.Error(fmt.Sprintf("streamName: %s, dstMst: %s, outField: %s, new stream call failed", info.Name, dstMst.Name, v.Alias), zap.Error(err))
					canRegisterTask = false
					break
				}
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
	var cnt int32
	s.tasks.Range(func(key, value interface{}) bool {
		cnt++
		id, _ := key.(uint64)
		w, _ := value.(Task)
		_, exist := streams[w.getName()]
		if exist {
			return true
		}
		s.DeleteTask(id)
		return true
	})
	atomic.StoreInt32(&s.taskNum, cnt)
}

func (s *Stream) Run() {
	s.Logger.Info("start stream")
	s.updateTask()
	go s.cleanStreamWAl()
	go s.detectRelay()

	d := 10 * time.Second
	util.TickerRun(d, s.abort, s.updateTask, s.AbortFunc)
}

func (s *Stream) detectRelay() {
	ticker := time.NewTimer(5 * time.Second)
	defer ticker.Stop()
	index := 0
	engine.NewStreamWalManager().InitStreamHandler(s.StreamHandler)
	for {
		select {
		case <-s.abort:
			s.AbortFunc()
			return
		case <-ticker.C:
			ctx := context.Background()
			init := true
			initTime := map[uint32]int64{}
			s.tasks.Range(func(key, value any) bool {
				w, _ := value.(Task)
				if w.IsInit() {
					lastFlushes := w.getLoadStatus()
					if lastFlushes == nil {
						return true
					}
					for u, status := range lastFlushes {
						v, ok := initTime[u]
						if !ok {
							initTime[u] = status.Timestamp
						} else {
							if v > status.Timestamp {
								initTime[u] = status.Timestamp
							}
						}
					}
					return true
				}
				init = false
				return false
			})
			if !init {
				continue
			}
			s.Logger.Info("stream replay task init")
			// TODO: replay function support get data based pt
			for ptID, ti := range initTime {
				var err error
				index, err = engine.NewStreamWalManager().Replay(ctx, ti, ptID, index)
				if err != nil {
					s.Logger.Error("replay task init error", zap.Error(err))
				}
			}
			s.Logger.Info("stream replay task init over")
			return
		}
	}
}

func (s *Stream) StreamHandler(rows influx.Rows, fileNames []string) error {
	s.Logger.Info("row data", zap.Int("len", rows.Len()))

	if len(rows) == 0 || len(fileNames) == 0 {
		return nil
	}

	db, rp, ptID, shardID, err := ParseFileName(fileNames[0], s.walPath)
	if err != nil {
		return err
	}

	r := &ReplayRow{}
	r.rows = rows
	r.ptID = ptID
	r.shardID = shardID
	r.db = db
	r.rp = rp

	s.stats.AddStreamIn(1)
	s.stats.AddStreamInNum(int64(len(r.rows)))
	s.cache <- r

	return nil
}

func ParseFileName(name, walPath string) (db, rp string, ptID uint32, shardID uint64, err error) {
	var re *regexp.Regexp
	re, err = regexp.Compile(fmt.Sprintf("%s%s", walPath, WalFilePathReg))
	if err != nil {
		return
	}
	info := re.FindStringSubmatch(name)
	if len(info) != 9 {
		err = fmt.Errorf("filePath non-conformance to specifications")
		return
	}
	db, rp = info[1], info[3]
	pt, err := strconv.ParseUint(info[2], 10, 32)
	if err != nil {
		return
	}
	ptID = uint32(pt)
	shardID, err = strconv.ParseUint(info[4], 10, 64)

	return
}

func (s *Stream) cleanStreamWAl() {
	var flushWindowTime = int64(math.MinInt64)
	d := 30 * time.Second
	util.TickerRun(d, s.abort, func() {
		if atomic.LoadInt32(&s.taskNum) == 0 {
			flushWindowTime = time.Now().UnixNano()
		} else {
			flushWindowTime = int64(math.MaxInt64)
			s.tasks.Range(func(key, value any) bool {
				w, _ := value.(Task)
				if flushWindowTime > w.getCurrentTimestamp() {
					flushWindowTime = w.getCurrentTimestamp()
				}
				return true
			})
		}
		engine.NewStreamWalManager().Free(flushWindowTime)
		s.Logger.Info("clean stream wal", zap.Int64("flushWindowTime", flushWindowTime))
	}, s.AbortFunc)
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

	if info.Interval > 0 {
		//base windowNum is 2, one window for current window, other window for delay data
		windowNum := info.Delay/info.Interval + 2
		if int64(windowNum) > int64(maxWindowNum) {
			return errors.New("maxDelay too big, exceed the maxWindowNum")
		}
	}

	var cond *influxql.BinaryExpr
	if info.Cond != "" && info.Interval == 0 && len(info.Dims) == 0 {
		expr, err := influxql.ParseExpr(info.Cond)
		if err != nil {
			return err
		}
		if binaryExpr, ok := expr.(*influxql.BinaryExpr); ok {
			cond = binaryExpr
		} else {
			return fmt.Errorf("invalid condition, expr: %s", expr.String())
		}
	}

	baseTask := &BaseTask{
		windowNum:    int64(maxWindowNum),
		id:           info.ID,
		src:          info.SrcMst,
		des:          info.DesMst,
		initTime:     start,
		start:        start,
		end:          start.Add(info.Interval),
		window:       info.Interval,
		fieldCalls:   fieldCalls,
		store:        s.store,
		maxDelay:     info.Delay,
		rows:         []influx.Row{},
		Logger:       logger,
		name:         info.Name,
		stats:        statistics.NewStreamWindowStatItem(info.ID),
		cli:          s.cli,
		dataPath:     s.dataPath,
		ptNumPerNode: s.ptNumPerNode,
		condition:    cond,
		isSelectAll:  info.IsSelectAll,
	}
	var task Task
	if len(info.Dims) == 0 {
		task = &TimeTask{
			TaskDataPool:    NewTaskDataPool(),
			windowCachePool: s.windowCachePool,
			BaseTask:        baseTask,
		}
	} else {
		task = &TagTask{
			TaskDataPool:    NewTaskDataPool(),
			goPool:          s.goPool,
			groupKeys:       info.Dims,
			bp:              s.bp,
			windowCachePool: s.windowCachePool,
			concurrency:     s.conf.WindowConcurrency,
			BaseTask:        baseTask,
		}
	}
	go func() {
		err := task.run()
		if err != nil {
			s.Logger.Error("task run fail", zap.String("name", task.getName()),
				zap.Error(err))
		}
	}()
	s.tasks.Store(info.ID, task)
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
			case *ReplayRow:
				_, indexes := s.rowsRangeTask(r)

				for i, vs := range indexes {
					for j := 0; j+1 < len(vs); j = j + 2 {
						newRows := &ReplayRow{}
						newRows.ptID = r.ptID
						newRows.shardID = r.shardID
						newRows.rows = r.rows[vs[j]:vs[j+1]]
						v, ok := s.tasks.Load(i)
						if ok {
							w, _ := v.(Task)
							w.Put(newRows)
						}
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

func (s *Stream) rowsRangeTask(r Rows) (bool, map[uint64][]int) {
	ref := false
	indexes := make(map[uint64][]int)
	db := r.GetDB()
	rp := r.GetRP()
	rows := r.GetRows()
	s.tasks.Range(func(key, value interface{}) bool {
		i, _ := key.(uint64)
		v, _ := value.(Task)
		if db != v.getSrcInfo().Database || rp != v.getSrcInfo().RetentionPolicy {
			return true
		}
		s.stats.AddStreamFilterNum(int64(len(rows)))
		index, exist := indexes[i]
		if !exist {
			index = []int{}
			indexes[i] = index
		}
		con := false
		startIndex := 0

		for j := range rows {
			name := influx.GetOriginMstName(rows[j].Name)
			if r.IsStreamRow(name, v, rows[j], i) {
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
			index = append(index, startIndex, len(rows))
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

type WriteStreamRowsCtx struct {
	DB                    string
	RP                    string
	PtId                  uint32
	ShardId               uint64
	StreamIdDstShardIdMap map[uint64]uint64
	WW                    WritePointsWorkIF
	StreamRows            *[]influx.Row
}

func (s *Stream) WriteRows(writeCtx *WriteStreamRowsCtx) (bool, error) {
	r := s.allocCacheRow(writeCtx)

	s.stats.AddStreamIn(1)
	s.stats.AddStreamInNum(int64(len(r.rows)))
	ref, indexes := s.rowsRangeTask(r)
	if !ref {
		s.rowPool.Put(r)
		return false, nil
	}
	for _, vs := range indexes {
		for j := 0; j < len(vs); j = j + 2 {
			atomic.AddInt64(&r.refCount, 1)
			r.ww.Ref()
		}
	}

	release := func() {
		wwRef := r.ww.UnRef()
		if wwRef == 0 {
			r.ww.PutWritePointsWork()
		}

		cur := atomic.AddInt64(&r.refCount, -1)
		if cur == 0 {
			s.rowPool.Put(r)
		}
	}

	var inUse bool
	for i, vs := range indexes {
		for j := 0; j < len(vs); j = j + 2 {
			cache := s.windowCachePool.Get()
			cache.ptId = r.ptId
			cache.shardId = r.streamIdDstShardIdMap[i]
			cache.rows = r.rows[vs[j]:vs[j+1]]
			cache.release = release
			cache.streamRows = writeCtx.StreamRows

			v, ok := s.tasks.Load(i)
			if !ok {
				// If no task is found, the cache is reclaimed.
				s.windowCachePool.Put(cache)
				continue
			}

			// If a task is found, the cache is reclaimed by invoking the release method inside the task.
			w, _ := v.(Task)
			needAgg, err := w.FilterRowsByCond(cache)
			if err != nil {
				return needAgg, err
			}
			inUse = inUse || needAgg
		}
	}
	return inUse, nil
}

func (s *Stream) allocCacheRow(ctx *WriteStreamRowsCtx) *CacheRow {
	r := s.rowPool.Get()
	r.rows = ctx.WW.GetRows()
	r.ptId = ctx.PtId
	r.shardID = ctx.ShardId
	r.db = ctx.DB
	r.rp = ctx.RP
	r.streamIdDstShardIdMap = ctx.StreamIdDstShardIdMap
	r.ww = ctx.WW
	r.refCount = 0
	return r
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

func (s *Stream) WriteReplayRows(db, rp string, ptId uint32, shardID uint64, ww WritePointsWorkIF) {
	r := &ReplayRow{
		rows:    ww.GetRows(),
		db:      db,
		rp:      rp,
		ptID:    ptId,
		shardID: shardID,
	}

	s.stats.AddStreamIn(1)
	s.stats.AddStreamInNum(int64(len(r.rows)))
	s.cache <- r
}

func (s *Stream) AbortFunc() {
	s.Logger.Info("close stream")
}
