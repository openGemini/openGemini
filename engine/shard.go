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

package engine

import (
	"context"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/ski"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/bucket"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

type Shard interface {
	WriteRows(rows []influx.Row, binaryRows []byte) error

	ForceFlush()

	MaxTime() int64

	Count() uint64

	SeriesCount() int

	Close() error

	Ref()

	UnRef()

	CreateLogicalPlan(ctx context.Context, sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error)

	LogicalPlanCost(sources influxql.Sources, opt query.ProcessorOptions) (hybridqp.LogicalPlanCost, error)

	GetSplitPoints(idxes []int64) ([]string, error)

	Expired() bool

	TierDurationExpired() (tier uint64, expired bool)

	RPName() string

	GetIndexBuild() *tsi.IndexBuilder

	GetID() uint64

	Open() error

	DataPath() string

	WalPath() string

	TableStore() immutable.TablesStore

	Ident() *meta.ShardIdentifier

	Duration() *meta.DurationDescriptor

	ChangeShardTierToWarm()

	SetWriteColdDuration(duration time.Duration)

	SetMutableSizeLimit(size int64)

	DropMeasurement(ctx context.Context, name string) error

	Statistics(buffer []byte) ([]byte, error)

	NewShardKeyIdx(shardType, dataPath string) error
}

type shard struct {
	mu       sync.RWMutex
	wg       sync.WaitGroup
	closed   *interruptsignal.InterruptSignal
	dataPath string
	tsspPath string
	walPath  string
	ident    *meta.ShardIdentifier

	cacheClosed  int32
	wal          *WAL
	snapshotLock sync.RWMutex
	activeTbl    *mutable.MemTable
	snapshotTbl  *mutable.MemTable
	snapshotWg   sync.WaitGroup
	immTables    immutable.TablesStore
	indexBuilder *tsi.IndexBuilder
	skIdx        *ski.ShardKeyIndex
	count        int64
	tmLock       sync.RWMutex
	maxTime      int64
	startTime    time.Time
	endTime      time.Time
	durationInfo *meta.DurationDescriptor
	log          *logger.Logger

	tier uint64

	lastWriteTime uint64

	writeColdDuration time.Duration

	mutableSizeLimit int64

	forceFlush  bool
	forceChan   chan struct{}
	defaultTags map[string]string
	fileStat    *statistics.FileStatistics
}

type nodeMemBucket struct {
	once      sync.Once
	memBucket bucket.ResourceBucket
}

var nodeMutableLimit nodeMemBucket

func (nodeLimit *nodeMemBucket) initNodeMemBucket(timeOut time.Duration, memThreshold int64) {
	nodeLimit.once.Do(func() {
		log.Info("New node mem limit bucket", zap.Int64("node mutable size limit", memThreshold),
			zap.Duration("max write hang duration", timeOut))
		nodeLimit.memBucket = bucket.NewInt64Bucket(timeOut, memThreshold)
	})
}

func (nodeLimit *nodeMemBucket) allocResource(r int64) error {
	return nodeLimit.memBucket.GetResource(r)
}

func (nodeLimit *nodeMemBucket) freeResource(r int64) {
	nodeLimit.memBucket.ReleaseResource(r)
}

func getWalPartitionNum() int {
	cpuNum := cpu.GetCpuNum()
	if cpuNum <= 16 {
		return cpuNum
	}
	return 16
}

func NewShard(dataPath string, walPath string, ident *meta.ShardIdentifier, indexBuilder *tsi.IndexBuilder, durationInfo *meta.DurationDescriptor, tr *meta.TimeRangeInfo,
	options netstorage.EngineOptions) *shard {
	db, rp := decodeShardPath(dataPath)
	tsspPath := path.Join(dataPath, immutable.TsspDirName)
	lock := fileops.FileLockOption("")
	err := fileops.MkdirAll(tsspPath, 0750, lock)
	if err != nil {
		panic(err)
	}
	err = fileops.MkdirAll(walPath, 0750, lock)
	if err != nil {
		panic(err)
	}

	nodeMutableLimit.initNodeMemBucket(options.MaxWriteHangTime, options.NodeMutableSizeLimit)

	s := &shard{
		closed:            interruptsignal.NewInterruptSignal(),
		dataPath:          dataPath,
		walPath:           walPath,
		tsspPath:          tsspPath,
		ident:             ident,
		wal:               NewWAL(walPath, options.WalSyncInterval, options.WalEnabled, options.WalReplayParallel, getWalPartitionNum()),
		activeTbl:         mutable.NewMemTable(mutable.NewConfig(), dataPath),
		indexBuilder:      indexBuilder,
		maxTime:           0,
		lastWriteTime:     fasttime.UnixTimestamp(),
		startTime:         tr.StartTime,
		endTime:           tr.EndTime,
		writeColdDuration: options.WriteColdDuration,
		forceChan:         make(chan struct{}, 1),
		defaultTags: map[string]string{
			"path":            dataPath,
			"id":              fmt.Sprintf("%d", ident.ShardID),
			"database":        db,
			"retentionPolicy": rp,
		},
		fileStat: statistics.NewFileStatistics(),
	}
	s.log = logger.NewLogger(errno.ModuleShard)
	s.SetMutableSizeLimit(options.ShardMutableSizeLimit)
	s.durationInfo = durationInfo
	tier, expired := s.TierDurationExpired()
	s.tier = tier
	if expired {
		if tier == meta.Hot {
			s.tier = meta.Warm
		} else {
			s.tier = meta.Cold
		}
	}
	s.immTables = immutable.NewTableStore(tsspPath, &s.tier, options.CompactRecovery, immutable.NewConfig())
	s.wg.Add(1)
	go s.Snapshot()
	return s
}

func (s *shard) NewShardKeyIdx(shardType, dataPath string) error {
	if shardType != influxql.RANGE {
		return nil
	}
	skToSidIdx, err := ski.NewShardKeyIndex(dataPath)
	if err != nil {
		return err
	}
	s.skIdx = skToSidIdx
	s.activeTbl.SetIdx(skToSidIdx)
	return nil
}

func (s *shard) Ref() {
	s.wg.Add(1)
}

func (s *shard) UnRef() {
	s.wg.Done()
}

func (s *shard) SetWriteColdDuration(duration time.Duration) {
	s.writeColdDuration = duration
}

func (s *shard) SetMutableSizeLimit(size int64) {
	s.activeTbl.GetConf().SetShardMutableSizeLimit(size)
	s.mutableSizeLimit = size
}

func (s *shard) GetValuesInMutableAndSnapshot(msName string, id uint64, tr record.TimeRange, schema record.Schemas, ascending bool) *record.Record {
	s.snapshotLock.RLock()
	activeTbl := s.activeTbl
	snapshotTbl := s.snapshotTbl
	activeTbl.Ref()
	if snapshotTbl != nil {
		snapshotTbl.Ref()
	}
	s.snapshotLock.RUnlock()

	activeRec := activeTbl.Values(msName, id, tr, schema, ascending)
	activeTbl.UnRef()

	var snapshotRec *record.Record
	if snapshotTbl != nil {
		snapshotRec = snapshotTbl.Values(msName, id, tr, schema, ascending)
		snapshotTbl.UnRef()
	}

	if activeRec == nil {
		return snapshotRec
	} else if snapshotRec == nil {
		return activeRec
	}
	var mergeRecord record.Record
	if ascending {
		mergeRecord.MergeRecord(activeRec, snapshotRec)
	} else {
		mergeRecord.MergeRecordDescend(activeRec, snapshotRec)
	}
	return &mergeRecord
}

func (s *shard) WriteRows(rows []influx.Row, binaryRows []byte) error {
	if atomic.LoadInt32(&s.cacheClosed) > 0 {
		return ErrShardClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	atomic.StoreUint64(&s.lastWriteTime, fasttime.UnixTimestamp())

	if err := s.writeRowsToTable(rows, binaryRows); err != nil {
		log.Error("write buffer failed", zap.Error(err))
		atomic.AddInt64(&statistics.PerfStat.WriteReqErrors, 1)
		return err
	}

	atomic.AddInt64(&statistics.PerfStat.WriteRowsBatch, 1)
	atomic.AddInt64(&statistics.PerfStat.WriteRowsCount, int64(len(rows)))
	return nil
}

func (s *shard) shouldSnapshot() bool {
	s.snapshotLock.RLock()
	defer s.snapshotLock.RUnlock()

	if s.activeTbl == nil || s.snapshotTbl != nil || s.forceFlushing() {
		return false
	}

	if s.activeTbl != nil && s.activeTbl.GetMemSize() > 0 {
		if s.activeTbl.NeedFlush() {
			s.prepareSnapshot()
			return true
		}

		// check time
		if fasttime.UnixTimestamp() >= (atomic.LoadUint64(&s.lastWriteTime) + uint64(s.writeColdDuration.Seconds())) {
			s.prepareSnapshot()
			return true
		}
	}

	return false
}
func (s *shard) EnableCompaction() {
	s.immTables.CompactionEnable()
}

func (s *shard) DisableCompaction() {
	s.immTables.CompactionDisable()
}

func (s *shard) Snapshot() {
	timer := time.NewTicker(time.Millisecond * 100)
	defer func() {
		s.wg.Done()
		timer.Stop()
	}()
	for {
		select {
		case <-s.closed.Signal():
			return
		case <-timer.C:
			if !s.shouldSnapshot() {
				continue
			}
			s.writeSnapshot()
			s.endSnapshot()
		}
	}
}

type mstWriteCtx struct {
	rowsPool sync.Pool
	mstMap   dictpool.Dict
}

func (mw *mstWriteCtx) getMstMap() *dictpool.Dict {
	return &mw.mstMap
}

func (mw *mstWriteCtx) getRowsPool() []influx.Row {
	v := mw.rowsPool.Get()
	if v == nil {
		return []influx.Row{}
	}
	rp := v.([]influx.Row)

	return rp
}

//nolint
func (mw *mstWriteCtx) putRowsPool(rp []influx.Row) {
	for _, r := range rp {
		r.Reset()
	}
	rp = rp[:0]
	mw.rowsPool.Put(rp)
}

func (mw *mstWriteCtx) Reset() {
	mw.mstMap.Reset()
}

var mstWriteCtxPool sync.Pool

func getMstWriteCtx() *mstWriteCtx {
	v := mstWriteCtxPool.Get()
	if v == nil {
		return &mstWriteCtx{}
	}
	return v.(*mstWriteCtx)
}

func putMstWriteCtx(mw *mstWriteCtx) {
	for _, mapp := range mw.mstMap.D {
		rows, ok := mapp.Value.(*[]influx.Row)
		if !ok {
			panic("can't map mmPoints")
		}
		mw.putRowsPool(*rows)
	}

	mw.Reset()
	mstWriteCtxPool.Put(mw)
}

func (s *shard) getLastFlushTime(msName string, sid uint64) int64 {
	tm := int64(math.MinInt64)
	if s.snapshotTbl != nil {
		tm = s.snapshotTbl.GetMaxTimeBySidNoLock(msName, sid)
	}
	if tm == math.MinInt64 {
		tm = s.immTables.GetLastFlushTimeBySid(msName, sid)
	}
	return tm
}

func (s *shard) addRowCountsBySid(msName string, sid uint64, rowCounts int64) {
	s.immTables.AddRowCountsBySid(msName, sid, rowCounts)
}

func (s *shard) getRowCountsBySid(msName string, sid uint64) int64 {
	return s.immTables.GetRowCountsBySid(msName, sid)
}

func (s *shard) addRowCounts(rowCounts int64) {
	atomic.AddInt64(&s.count, rowCounts)
}

func calculateMemSize(rows influx.Rows) int64 {
	var memCost int64
	for i := range rows {
		// calculate tag mem cost, sid is 8 bytes
		memCost += int64(record.Uint64SizeBytes * len(rows[i].Tags))

		// calculate field mem cost
		for j := 0; j < len(rows[i].Fields); j++ {
			memCost += int64(len(rows[i].Fields[j].Key))
			if rows[i].Fields[j].Type == influx.Field_Type_Float {
				memCost += int64(record.Float64SizeBytes)
			} else if rows[i].Fields[j].Type == influx.Field_Type_String {
				memCost += int64(len(rows[i].Fields[j].StrValue))
			} else if rows[i].Fields[j].Type == influx.Field_Type_Boolean {
				memCost += int64(record.BooleanSizeBytes)
			} else if rows[i].Fields[j].Type == influx.Field_Type_Int {
				memCost += int64(record.Uint64SizeBytes)
			}
		}
	}
	return memCost
}

func (s *shard) writeRowsToTable(rows influx.Rows, binaryRows []byte) error {
	s.wg.Add(1)
	defer s.wg.Done()
	var err error

	start := time.Now()
	curSize := calculateMemSize(rows)
	err = nodeMutableLimit.allocResource(curSize)
	atomic.AddInt64(&statistics.PerfStat.WriteGetTokenDurationNs, time.Since(start).Nanoseconds())
	if err != nil {
		log.Info("Alloc resource failed, need retry", zap.Int64("current mem size", curSize))
		return err
	}

	mw := getMstWriteCtx()
	defer putMstWriteCtx(mw)
	mmPoints := mw.getMstMap()

	start = time.Now()
	if !sort.IsSorted(rows) {
		sort.Stable(rows)
	}
	atomic.AddInt64(&statistics.PerfStat.WriteSortIndexDurationNs, time.Since(start).Nanoseconds())

	var writeIndexRequired bool
	start = time.Now()

	tm := int64(math.MinInt64)
	primaryIndex := s.indexBuilder.GetPrimaryIndex()
	mergetIndex := primaryIndex.(*tsi.MergeSetIndex)
	for i := 0; i < len(rows); i++ {
		if s.closed.Closed() {
			return ErrShardClosed
		}

		if !mmPoints.Has(rows[i].Name) {
			rp := mw.getRowsPool()
			mmPoints.Set(rows[i].Name, &rp)
		}
		rowsPool := mmPoints.Get(rows[i].Name)
		rp, ok := rowsPool.(*[]influx.Row)
		if !ok {
			return fmt.Errorf("MstMap error")
		}

		if cap(*rp) > len(*rp) {
			*rp = (*rp)[:len(*rp)+1]
		} else {
			*rp = append(*rp, influx.Row{})
		}
		ri := &(*rp)[len(*rp)-1]
		ri.Clone(&rows[i])

		if rows[i].Timestamp > tm {
			tm = rows[i].Timestamp
		}

		if !writeIndexRequired {
			ri.SeriesId, err = mergetIndex.GetSeriesIdBySeriesKey(rows[i].IndexKey, record.Str2bytes(rows[i].Name))
			if err != nil {
				return err
			}

			if ri.SeriesId == 0 {
				writeIndexRequired = true
			}
		}

		atomic.AddInt64(&statistics.PerfStat.WriteFieldsCount, int64(rows[i].Fields.Len()))
	}

	s.setMaxTime(tm)

	if writeIndexRequired {

		failpoint.Inject("SlowDownCreateIndex", nil)

		if err = s.indexBuilder.CreateIndexIfNotExists(mmPoints); err != nil {
			return err
		}
	} else {
		if err = s.indexBuilder.CreateIndexIfPrimaryKeyExists(mmPoints, false); err != nil {
			return err
		}
	}
	atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(start).Nanoseconds())

	s.snapshotLock.RLock()

	start = time.Now()

	failpoint.Inject("SlowDownActiveTblWrite", nil)

	if err = s.activeTbl.WriteRows(mmPoints, func(msName string, sid uint64) int64 {
		return s.getLastFlushTime(msName, sid)
	}, func(msName string, sid uint64, rowCounts int64) {
		s.addRowCountsBySid(msName, sid, rowCounts)
	}); err != nil {
		s.activeTbl.AddMemSize(curSize)
		s.snapshotLock.RUnlock()
		log.Error("write rows to memory table fail", zap.Uint64("shard", s.ident.ShardID), zap.Error(err))
		return err
	}
	s.activeTbl.AddMemSize(curSize)
	atomic.AddInt64(&statistics.PerfStat.WriteRowsDurationNs, time.Since(start).Nanoseconds())

	start = time.Now()

	failpoint.Inject("SlowDownWalWrite", nil)

	if err := s.wal.Write(binaryRows); err != nil {
		s.snapshotLock.RUnlock()
		log.Error("write rows to wal fail", zap.Uint64("shard", s.ident.ShardID), zap.Error(err))
		return err
	}
	atomic.AddInt64(&statistics.PerfStat.WriteWalDurationNs, time.Since(start).Nanoseconds())
	s.snapshotLock.RUnlock()
	s.addRowCounts(int64(len(rows)))
	return nil
}

func (s *shard) enableForceFlush() {
	s.forceChan <- struct{}{}
	s.snapshotLock.Lock()
	s.forceFlush = true
	s.snapshotLock.Unlock()
}

func (s *shard) disableForceFlush() {
	s.snapshotLock.Lock()
	s.forceFlush = false
	s.snapshotLock.Unlock()
	<-s.forceChan
}

func (s *shard) forceFlushing() bool {
	return s.forceFlush
}

func (s *shard) ForceFlush() {
	s.enableForceFlush()
	defer s.disableForceFlush()

	s.waitSnapshot()
	s.prepareSnapshot()
	s.writeSnapshot()
	s.endSnapshot()
}

func flushChunkImp(dataPath, msName string, totalChunks int, tbStore immutable.TablesStore, chunk *mutable.WriteChunk,
	orderMs, unOrderMs *immutable.MsBuilder, finish bool) (*immutable.MsBuilder, *immutable.MsBuilder) {
	orderRec := chunk.OrderWriteRec.GetRecord()
	unOrderRec := chunk.UnOrderWriteRec.GetRecord()
	conf := immutable.NewConfig()
	var err error
	if orderRec.RowNums() != 0 {
		if orderMs == nil {
			orderFileName := immutable.NewTSSPFileName(tbStore.NextSequence(), 0, 0, 0, true)
			orderMs = immutable.AllocMsBuilder(dataPath, msName, conf, totalChunks,
				orderFileName, tbStore.Tier(), tbStore.Sequencer(), orderRec.Len())
		}

		orderMs, err = orderMs.WriteRecord(chunk.Sid, orderRec, func(fn immutable.TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
			return tbStore.NextSequence(), 0, 0, 0
		})
		if err != nil {
			panic(err)
		}
		atomic.AddInt64(&statistics.PerfStat.FlushRowsCount, int64(orderRec.RowNums()))
		atomic.AddInt64(&statistics.PerfStat.FlushOrderRowsCount, int64(orderRec.RowNums()))
	}

	if unOrderRec.RowNums() != 0 {
		if unOrderMs == nil {
			disorderFileName := immutable.NewTSSPFileName(tbStore.NextSequence(), 0, 0, 0, false)
			unOrderMs = immutable.AllocMsBuilder(dataPath, msName, conf,
				totalChunks, disorderFileName, tbStore.Tier(), tbStore.Sequencer(), unOrderRec.Len())
		}

		unOrderMs, err = unOrderMs.WriteRecord(chunk.Sid, unOrderRec, func(fn immutable.TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
			return tbStore.NextSequence(), 0, 0, 0
		})
		if err != nil {
			panic(err)
		}

		atomic.AddInt64(&statistics.PerfStat.FlushRowsCount, int64(unOrderRec.RowNums()))
		atomic.AddInt64(&statistics.PerfStat.FlushUnOrderRowsCount, int64(unOrderRec.RowNums()))
	}

	if finish {
		if orderMs != nil {
			f, err := orderMs.NewTSSPFile(true)
			if err != nil {
				panic(err)
			}
			if f != nil {
				orderMs.Files = append(orderMs.Files, f)
			}

			if err = immutable.RenameTmpFiles(orderMs.Files); err != nil {
				panic(err)
			}
			tbStore.AddTSSPFiles(orderMs.Name(), true, orderMs.Files...)
			immutable.PutMsBuilder(orderMs)
			orderMs = nil
		}

		if unOrderMs != nil {
			f, err := unOrderMs.NewTSSPFile(true)
			if err != nil {
				panic(err)
			}
			if f != nil {
				unOrderMs.Files = append(unOrderMs.Files, f)
			}
			if err = immutable.RenameTmpFiles(unOrderMs.Files); err != nil {
				panic(err)
			}
			tbStore.AddTSSPFiles(unOrderMs.Name(), false, unOrderMs.Files...)
			immutable.PutMsBuilder(unOrderMs)
			unOrderMs = nil
		}
	}

	return orderMs, unOrderMs
}

func (s *shard) commitSnapshot(snapshot *mutable.MemTable) {
	snapshot.ApplyConcurrency(func(msName string, sids []uint64) {
		start := time.Now()
		t := start
		snapshot.SortAndDedup(msName, sids)
		atomic.AddInt64(&statistics.PerfStat.SnapshotSortChunksNs, time.Since(t).Nanoseconds())

		t = time.Now()
		snapshot.FlushChunks(s.tsspPath, msName, s.immTables, sids, flushChunkImp)
		atomic.AddInt64(&statistics.PerfStat.SnapshotFlushChunksNs, time.Since(t).Nanoseconds())

		atomic.AddInt64(&statistics.PerfStat.SnapshotHandleChunksNs, time.Since(start).Nanoseconds())
	})
}

func (s *shard) prepareSnapshot() {
	s.snapshotWg.Add(1)
}

func (s *shard) endSnapshot() {
	s.snapshotWg.Done()
}

func (s *shard) waitSnapshot() {
	s.snapshotWg.Wait()
}

func (s *shard) writeSnapshot() {
	s.snapshotLock.Lock()
	if s.activeTbl == nil {
		s.snapshotLock.Unlock()
		return
	}
	walFiles, err := s.wal.Switch()
	if err != nil {
		s.snapshotLock.Unlock()
		panic("wal switch failed")
	}

	s.snapshotTbl = s.activeTbl
	curSize := s.snapshotTbl.GetMemSize()
	statistics.MutableStat.AddMutableSize(s.tsspPath, -curSize)

	s.activeTbl = mutable.GetMemTable(s.tsspPath)
	s.activeTbl.SetIdx(s.skIdx)
	s.activeTbl.GetConf().SetShardMutableSizeLimit(s.mutableSizeLimit)
	s.snapshotLock.Unlock()

	start := time.Now()
	s.indexBuilder.Flush()

	s.commitSnapshot(s.snapshotTbl)
	nodeMutableLimit.freeResource(curSize)

	err = s.wal.Remove(walFiles)
	if err != nil {
		panic("wal remove files failed")
	}
	s.snapshotLock.Lock()
	s.snapshotTbl.PutMemTable()
	s.snapshotTbl = nil
	s.snapshotLock.Unlock()

	atomic.AddInt64(&statistics.PerfStat.FlushSnapshotDurationNs, time.Since(start).Nanoseconds())
	atomic.AddInt64(&statistics.PerfStat.FlushSnapshotCount, 1)
}

func (s *shard) MaxTime() int64 {
	s.tmLock.RLock()
	tm := s.maxTime
	s.tmLock.RUnlock()
	return tm
}

func (s *shard) SeriesCount() int {
	if s.skIdx == nil {
		return 0
	}
	return s.skIdx.GetShardSeriesCount()
}

func (s *shard) Count() uint64 {
	return uint64(atomic.LoadInt64(&s.count))
}

func (s *shard) GetSplitPoints(idxes []int64) ([]string, error) {
	if atomic.LoadInt32(&s.cacheClosed) > 0 {
		return nil, ErrShardClosed
	}
	return s.getSplitPointsByRowCount(idxes)
}

func (s *shard) getSplitPointsByRowCount(idxes []int64) ([]string, error) {
	return s.skIdx.GetSplitPointsByRowCount(idxes, func(name string, sid uint64) int64 {
		return s.getRowCountsBySid(name, sid)
	})
}

func (s *shard) GetIndexBuild() *tsi.IndexBuilder {
	if atomic.LoadInt32(&s.cacheClosed) > 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.indexBuilder
}

func (s *shard) Close() error {
	// prevent multi goroutines close shard the same time
	if atomic.AddInt32(&s.cacheClosed, 1) != 1 {
		return ErrShardClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	compWorker.UnregisterShard(s.ident.ShardID)

	if s.skIdx != nil {
		if err := s.skIdx.Close(); err != nil {
			return err
		}
	}

	s.closed.Close()

	log.Info("start close shard...", zap.Uint64("id", s.ident.ShardID))
	if err := s.wal.Close(); err != nil {
		log.Error("close wal fail", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		return err
	}

	// release mem table resource
	s.snapshotLock.Lock()
	curMemSize := int64(0)
	if s.activeTbl != nil {
		curMemSize = s.activeTbl.GetMemSize()
		s.activeTbl = nil
	}
	s.snapshotLock.Unlock()
	nodeMutableLimit.freeResource(curMemSize)

	log.Info("close immutables")
	if err := s.immTables.Close(); err != nil {
		log.Error("close table store fail", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		return err
	}

	log.Info("success close immutables")

	s.wg.Wait()
	return nil
}

func (s *shard) setMaxTime(tm int64) {
	s.tmLock.Lock()
	if tm > s.maxTime {
		s.maxTime = tm
	}
	s.tmLock.Unlock()
}

func (s *shard) writeWalBuffer(binary []byte) error {
	var rows []influx.Row
	var tagPools []influx.Tag
	var fieldPools []influx.Field
	var indexKeyPools []byte
	var indexOptionPools []influx.IndexOption
	var err error

	rows, _, _, _, _, err = influx.FastUnmarshalMultiRows(binary, rows, tagPools, fieldPools, indexOptionPools, indexKeyPools)
	if err != nil {
		logger.GetLogger().Warn(errno.NewError(errno.WalRecordUnmarshalFailed, "unmarshal rows fail", s.ident.ShardID, err).Error())
		return nil
	}

	return s.writeRowsToTable(rows, nil)
}

func (s *shard) replayWal() error {
	walFileNames, err := s.wal.Replay(
		func(binary []byte) error {
			return s.writeWalBuffer(binary)
		},
	)
	if err != nil {
		return err
	}

	s.ForceFlush()
	err = s.wal.Remove(walFileNames)
	if err != nil {
		return err
	}

	return nil
}

func (s *shard) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.immTables.CompactionEnable()

	compWorker.RegisterShard(s)

	start := time.Now()
	logger.GetLogger().Info("open shard start...", zap.Uint64("id", s.ident.ShardID))

	maxTime, totalRows, err := s.immTables.Open()
	if err != nil {
		logger.GetLogger().Error("open shard failed", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		return err
	}
	s.addRowCounts(totalRows)
	s.setMaxTime(maxTime)
	logger.GetLogger().Info("open immutable done", zap.Uint64("id", s.ident.ShardID), zap.Duration("time used", time.Since(start)))

	// replay wal files
	wStart := time.Now()
	err = s.replayWal()
	if err != nil {
		logger.GetLogger().Error("replay wal failed", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		return err
	}
	logger.GetLogger().Info("replay wal done", zap.Uint64("id", s.ident.ShardID), zap.Duration("time used", time.Since(wStart)))

	logger.GetLogger().Info("open shard done", zap.Uint64("id", s.ident.ShardID), zap.Duration("time used", time.Since(start)),
		zap.Int64("totalRows", totalRows), zap.Int64("maxTime", maxTime))

	return nil
}

func (s *shard) TableStore() immutable.TablesStore {
	return s.immTables
}

func (s *shard) Expired() bool {
	now := time.Now().UTC()
	if s.durationInfo.Duration != 0 && s.endTime.Add(s.durationInfo.Duration).Before(now) {
		return true
	}
	return false
}

func (s *shard) TierDurationExpired() (tier uint64, expired bool) {
	now := time.Now().UTC()
	if s.durationInfo.TierDuration == 0 {
		return s.durationInfo.Tier, false
	}
	if s.durationInfo.Tier == meta.Hot && s.endTime.Add(s.durationInfo.TierDuration).Before(now) {
		return meta.Hot, true
	}

	if s.durationInfo.Tier == meta.Warm && s.endTime.Add(s.durationInfo.TierDuration).Before(now) {
		return meta.Warm, true
	}
	return s.durationInfo.Tier, false
}

func (s *shard) ChangeShardTierToWarm() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tier == meta.Warm {
		return
	}
	s.immTables.FreeAllMemReader()
	s.tier = meta.Warm
}

func (s *shard) RPName() string {
	return s.ident.Policy
}

func (s *shard) GetID() uint64 {
	return s.ident.ShardID
}

func (s *shard) Ident() *meta.ShardIdentifier {
	return s.ident
}

func (s *shard) Duration() *meta.DurationDescriptor {
	return s.durationInfo
}

func (s *shard) DataPath() string {
	return s.dataPath
}

func (s *shard) WalPath() string {
	return s.walPath
}

func (s *shard) LastWriteTime() uint64 {
	return atomic.LoadUint64(&s.lastWriteTime)
}

// DropMeasurement drop measurement name from shard
func (s *shard) DropMeasurement(ctx context.Context, name string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// flush measurement data in mem
	s.ForceFlush()

	// drop measurement from immutable
	return s.immTables.DropMeasurement(ctx, name)
}

func (s *shard) Statistics(buffer []byte) ([]byte, error) {
	s.mu.RLock()
	if s.closed.Closed() {
		s.mu.RUnlock()
		return buffer, nil
	}
	fileStat := s.immTables.GetMstFileStat()
	s.mu.RUnlock()

	return s.fileStat.Collect(buffer, s.defaultTags, fileStat)
}

var (
	_ Shard = (*shard)(nil)
)

func decodeShardPath(shardPath string) (database, retentionPolicy string) {
	// shardPath format: /opt/tsdb/data/data/db0/1/autogen/6_1650844800000000000_1650931200000000000_6/

	// Discard the last part of the path, remain: /opt/tsdb/data/data/db0/1/autogen/
	path, _ := filepath.Split(filepath.Clean(shardPath))

	// remain: /opt/tsdb/data/data/db0/1/
	// rp: autogen
	path, rp := filepath.Split(filepath.Clean(path))

	// remain: /opt/tsdb/data/data/db0/
	path, _ = filepath.Split(filepath.Clean(path))

	// remain: /opt/tsdb/data/data
	// db: db0
	_, db := filepath.Split(filepath.Clean(path))

	return db, rp
}
