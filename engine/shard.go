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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/ski"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/bucket"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

const (
	MaxRetryUpdateOnShardNum    = 4
	minDownSampleConcurrencyNum = 1
	cpuDownSampleRatio          = 16
	CRCLen                      = 4
	BufferSize                  = 1024 * 1024
)

var (
	DownSampleWriteDrop  = true
	downSampleLogSeq     = uint64(time.Now().UnixNano())
	downSampleInorder    = false
	maxDownSampleTaskNum int
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

	StartDownSample(taskID uint64, level int, sdsp *meta.ShardDownSamplePolicyInfo, meta interface {
		UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
	}) error

	GetTSSPFiles(mm string, isOrder bool) (*immutable.TSSPFiles, bool)

	GetIndexBuild() *tsi.IndexBuilder

	GetID() uint64

	Open(client metaclient.MetaClient) error

	DataPath() string

	WalPath() string

	TableStore() immutable.TablesStore

	Ident() *meta.ShardIdentifier

	Duration() *meta.DurationDescriptor

	ChangeShardTierToWarm()

	SetWriteColdDuration(duration time.Duration)

	SetMutableSizeLimit(size int64)

	IsOutOfOrderFilesExist() bool

	DropMeasurement(ctx context.Context, name string) error

	Statistics(buffer []byte) ([]byte, error)

	NewShardKeyIdx(shardType, dataPath string, lockPath *string) error

	GetShardDownSamplePolicy(policy *meta.DownSamplePolicyInfo) *meta.ShardDownSamplePolicyInfo

	SetShardDownSampleLevel(i int)
	UpdateDownSampleOnShard(id uint64, level int)
	NewDownSampleTask(sdsp *meta.ShardDownSamplePolicyInfo, schema []hybridqp.Catalog, log *zap.Logger)
	UpdateShardReadOnly(meta interface {
		UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
	}) error
	DisableDownSample()
	EnableDownSample()
	downSampleEnabled() bool
	CanDoDownSample() bool

	CompactionEnabled() bool
	DisableCompAndMerge()
	EnableCompAndMerge()
	Compact() error
	WaitWriteFinish()

	SetIndexBuilder(builder *tsi.IndexBuilder)
	CloseIndexBuilder() error
	Scan(span *tracing.Span, schema *executor.QuerySchema, callBack func(num int64) error) (tsi.GroupSeries, int64, error)
	CreateCursor(ctx context.Context, schema *executor.QuerySchema) ([]comm.KeyCursor, error)
}

type shard struct {
	mu       sync.RWMutex
	wg       sync.WaitGroup
	writeWg  sync.WaitGroup
	opId     uint64
	closed   *interruptsignal.InterruptSignal
	dataPath string
	tsspPath string
	walPath  string
	lock     *string
	ident    *meta.ShardIdentifier

	cacheClosed        int32
	isAsyncReplayWal   bool               // async replay wal switch
	cancelLock         sync.RWMutex       // lock for cancelFn
	cancelFn           context.CancelFunc // to cancel wal replay
	loadWalDone        bool               // is replay wal done
	wal                *WAL               // for cases: 1. write 2. replay
	snapshotLock       sync.RWMutex
	memDataReadEnabled bool
	activeTbl          *mutable.MemTable
	snapshotTbl        *mutable.MemTable
	snapshotWg         sync.WaitGroup
	immTables          immutable.TablesStore
	indexBuilder       *tsi.IndexBuilder
	skIdx              *ski.ShardKeyIndex
	count              int64
	tmLock             sync.RWMutex
	maxTime            int64
	startTime          time.Time
	endTime            time.Time
	durationInfo       *meta.DurationDescriptor
	log                *logger.Logger

	tier uint64

	lastWriteTime uint64

	writeColdDuration time.Duration

	mutableSizeLimit int64

	forceFlush  bool
	forceChan   chan struct{}
	defaultTags map[string]string
	fileStat    *statistics.FileStatistics

	shardDownSampleTaskInfo *shardDownSampleTaskInfo

	stopDownSample chan struct{}
	dswg           sync.WaitGroup
	downSampleEn   int32
}

type shardDownSampleTaskInfo struct {
	sdsp   *meta.ShardDownSamplePolicyInfo
	schema []hybridqp.Catalog
	log    *zap.Logger
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
		nodeLimit.memBucket = bucket.NewInt64Bucket(timeOut, memThreshold, false)
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

func initMaxDownSampleParallelism(parallelism int) {
	if parallelism <= 0 {
		parallelism = hybridqp.MaxInt(cpu.GetCpuNum()/cpuDownSampleRatio, minDownSampleConcurrencyNum)
	}
	maxDownSampleTaskNum = parallelism
}

func NewShard(dataPath, walPath string, lockPath *string, ident *meta.ShardIdentifier, durationInfo *meta.DurationDescriptor, tr *meta.TimeRangeInfo,
	options netstorage.EngineOptions) *shard {
	db, rp := decodeShardPath(dataPath)
	tsspPath := path.Join(dataPath, immutable.TsspDirName)
	lock := fileops.FileLockOption(*lockPath)
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
		closed:             interruptsignal.NewInterruptSignal(),
		dataPath:           dataPath,
		walPath:            walPath,
		tsspPath:           tsspPath,
		lock:               lockPath,
		ident:              ident,
		isAsyncReplayWal:   options.WalReplayAsync,
		wal:                NewWAL(walPath, lockPath, options.WalSyncInterval, options.WalEnabled, options.WalReplayParallel, getWalPartitionNum()),
		activeTbl:          mutable.NewMemTable(mutable.NewConfig(), dataPath),
		memDataReadEnabled: options.MemDataReadEnabled,
		maxTime:            0,
		lastWriteTime:      fasttime.UnixTimestamp(),
		startTime:          tr.StartTime,
		endTime:            tr.EndTime,
		writeColdDuration:  options.WriteColdDuration,
		forceChan:          make(chan struct{}, 1),
		defaultTags: map[string]string{
			"path":            dataPath,
			"id":              fmt.Sprintf("%d", ident.ShardID),
			"database":        db,
			"retentionPolicy": rp,
		},
		fileStat:       statistics.NewFileStatistics(),
		stopDownSample: make(chan struct{}),
		downSampleEn:   0,
	}
	DownSampleWriteDrop = options.DownSampleWriteDrop
	initMaxDownSampleParallelism(options.MaxDownSampleTaskConcurrency)

	s.log = logger.NewLogger(errno.ModuleShard)
	s.SetMutableSizeLimit(options.ShardMutableSizeLimit)
	s.durationInfo = durationInfo
	tier, expired := s.TierDurationExpired()
	s.tier = tier
	if expired {
		if tier == util.Hot {
			s.tier = util.Warm
		} else {
			s.tier = util.Cold
		}
	}
	s.immTables = immutable.NewTableStore(tsspPath, s.lock, &s.tier, options.CompactRecovery, immutable.NewConfig())
	s.immTables.SetAddFunc(s.addRowCounts)
	return s
}

func (s *shard) NewShardKeyIdx(shardType, dataPath string, lockPath *string) error {
	if shardType != influxql.RANGE {
		return nil
	}
	skToSidIdx, err := ski.NewShardKeyIndex(dataPath, lockPath)
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

func (s *shard) WriteRows(rows []influx.Row, binaryRows []byte) error {
	if atomic.LoadInt32(&s.cacheClosed) > 0 {
		return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
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

func (s *shard) WaitWriteFinish() {
	s.writeWg.Wait()
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

func (s *shard) CompactionEnabled() bool {
	return s.immTables.CompactionEnabled()
}

func (s *shard) DisableCompAndMerge() {
	s.immTables.DisableCompAndMerge()
}

func (s *shard) EnableCompAndMerge() {
	s.immTables.EnableCompAndMerge()
}

func (s *shard) IsDownsampled() bool {
	return s.ident.DownSampleLevel != 0
}

func (s *shard) Compact() error {
	if s.IsDownsampled() {
		return nil
	}

	id := s.GetID()
	select {
	case <-s.closed.Signal():
		log.Info("closed", zap.Uint64("shardId", id))
		return nil
	default:
		if !s.immTables.CompactionEnabled() {
			return nil
		}
		nowTime := fasttime.UnixTimestamp()
		lastWrite := s.LastWriteTime()
		d := nowTime - lastWrite
		if d >= atomic.LoadUint64(&fullCompColdDuration) {
			if err := s.immTables.FullCompact(id); err != nil {
				log.Error("full compact error", zap.Uint64("shid", id), zap.Error(err))
			}
			return nil
		}

		for _, level := range immutable.LevelCompactRule {
			if err := s.immTables.LevelCompact(level, id); err != nil {
				log.Error("level compact error", zap.Uint64("shid", id), zap.Error(err))
				continue
			}
		}
	}
	return nil
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
	return *(v.(*[]influx.Row))
}

// nolint
func (mw *mstWriteCtx) putRowsPool(rp *[]influx.Row) {
	for _, r := range *rp {
		r.Reset()
	}
	*rp = (*rp)[:0]
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
		mw.putRowsPool(rows)
	}

	mw.Reset()
	mstWriteCtxPool.Put(mw)
}

func (s *shard) getLastFlushTime(msName string, sid uint64) (int64, error) {
	tm, err := s.immTables.GetLastFlushTimeBySid(msName, sid)
	if err != nil {
		return 0, err
	}

	if tm == math.MaxInt64 || s.snapshotTbl == nil {
		return tm, nil
	}

	snapshotTm := s.snapshotTbl.GetMaxTimeBySidNoLock(msName, sid)
	if snapshotTm > tm {
		tm = snapshotTm
	}

	return tm, nil
}

func (s *shard) addRowCountsBySid(msName string, sid uint64, rowCounts int64) {
	s.immTables.AddRowCountsBySid(msName, sid, rowCounts)
}

func (s *shard) getRowCountsBySid(msName string, sid uint64) (int64, error) {
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
	s.writeWg.Add(1)
	defer s.wg.Done()
	defer s.writeWg.Done()
	var err error

	if s.ident.ReadOnly {
		e := errors.New("can not write rows to downSampled shard")
		log.Error("write into shard failed", zap.Error(e))
		if !DownSampleWriteDrop {
			return e
		}
		return nil
	}
	start := time.Now()
	curSize := calculateMemSize(rows)
	err = nodeMutableLimit.allocResource(curSize)
	atomic.AddInt64(&statistics.PerfStat.WriteGetTokenDurationNs, time.Since(start).Nanoseconds())
	if err != nil {
		s.log.Info("Alloc resource failed, need retry", zap.Int64("current mem size", curSize))
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
			return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
		}
		//skip StreamOnly data
		if rows[i].StreamOnly {
			continue
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
			// PrimaryId is equal to SeriesId by default.
			ri.PrimaryId = ri.SeriesId

			if ri.SeriesId == 0 {
				writeIndexRequired = true
			}
		}
		atomic.AddInt64(&statistics.PerfStat.WriteFieldsCount, int64(rows[i].Fields.Len()))
	}

	s.setMaxTime(tm)

	failpoint.Inject("SlowDownCreateIndex", nil)

	if writeIndexRequired {
		if err = s.indexBuilder.CreateIndexIfNotExists(mmPoints); err != nil {
			return err
		}
	} else {
		if err = s.indexBuilder.CreateSecondaryIndexIfNotExist(mmPoints); err != nil {
			return err
		}
	}
	atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(start).Nanoseconds())

	s.snapshotLock.RLock()

	start = time.Now()

	failpoint.Inject("SlowDownActiveTblWrite", nil)

	if err = s.activeTbl.WriteRows(mmPoints, s.getLastFlushTime, s.addRowCountsBySid); err != nil {
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
	if s.indexBuilder == nil {
		return
	}
	s.enableForceFlush()
	defer s.disableForceFlush()

	s.waitSnapshot()
	s.prepareSnapshot()
	s.writeSnapshot()
	s.endSnapshot()
}

func flushChunkImp(dataPath, msName string, lockPath *string, totalChunks int, tbStore immutable.TablesStore, chunk *mutable.WriteChunk,
	orderMs, unOrderMs *immutable.MsBuilder, finish bool) (*immutable.MsBuilder, *immutable.MsBuilder) {
	orderRec := chunk.OrderWriteRec.GetRecord()
	unOrderRec := chunk.UnOrderWriteRec.GetRecord()
	conf := immutable.NewConfig()
	var err error
	if orderRec.RowNums() != 0 {
		if orderMs == nil {
			orderFileName := immutable.NewTSSPFileName(tbStore.NextSequence(), 0, 0, 0, true, lockPath)
			orderMs = immutable.AllocMsBuilder(dataPath, msName, lockPath, conf, totalChunks,
				orderFileName, tbStore.Tier(), tbStore.Sequencer(), orderRec.Len())
		}

		orderMs, err = orderMs.WriteRecord(chunk.Sid, orderRec, func(fn immutable.TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
			return tbStore.NextSequence(), 0, 0, 0
		})
		if err != nil {
			tbStore.UnRefSequencer()
			panic(err)
		}
		atomic.AddInt64(&statistics.PerfStat.FlushRowsCount, int64(orderRec.RowNums()))
		atomic.AddInt64(&statistics.PerfStat.FlushOrderRowsCount, int64(orderRec.RowNums()))
	}

	if unOrderRec.RowNums() != 0 {
		if unOrderMs == nil {
			disorderFileName := immutable.NewTSSPFileName(tbStore.NextSequence(), 0, 0, 0, false, lockPath)
			unOrderMs = immutable.AllocMsBuilder(dataPath, msName, lockPath, conf,
				totalChunks, disorderFileName, tbStore.Tier(), tbStore.Sequencer(), unOrderRec.Len())
		}

		unOrderMs, err = unOrderMs.WriteRecord(chunk.Sid, unOrderRec, func(fn immutable.TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
			return tbStore.NextSequence(), 0, 0, 0
		})
		if err != nil {
			tbStore.UnRefSequencer()
			panic(err)
		}

		atomic.AddInt64(&statistics.PerfStat.FlushRowsCount, int64(unOrderRec.RowNums()))
		atomic.AddInt64(&statistics.PerfStat.FlushUnOrderRowsCount, int64(unOrderRec.RowNums()))
	}

	if finish {
		if orderMs != nil {
			f, err := orderMs.NewTSSPFile(true)
			if err != nil {
				tbStore.UnRefSequencer()
				panic(err)
			}
			if f != nil {
				orderMs.Files = append(orderMs.Files, f)
			}

			if err = immutable.RenameTmpFiles(orderMs.Files); err != nil {
				if os.IsNotExist(err) {
					orderMs = nil
					unOrderMs = nil
					tbStore.UnRefSequencer()
					logger.GetLogger().Error("rename init file failed", zap.String("mstName", msName), zap.Error(err))
					return orderMs, unOrderMs
				}
				panic(err)
			}
			tbStore.AddTSSPFiles(orderMs.Name(), true, orderMs.Files...)
			orderMs = nil
			tbStore.UnRefSequencer()
		}

		if unOrderMs != nil {
			f, err := unOrderMs.NewTSSPFile(true)
			if err != nil {
				tbStore.UnRefSequencer()
				panic(err)
			}
			if f != nil {
				unOrderMs.Files = append(unOrderMs.Files, f)
			}
			if err = immutable.RenameTmpFiles(unOrderMs.Files); err != nil {
				if os.IsNotExist(err) {
					orderMs = nil
					unOrderMs = nil
					tbStore.UnRefSequencer()
					logger.GetLogger().Error("rename init file failed", zap.String("mstName", msName), zap.Error(err))
					return orderMs, unOrderMs
				}
				panic(err)
			}
			tbStore.AddTSSPFiles(unOrderMs.Name(), false, unOrderMs.Files...)
			unOrderMs = nil
			tbStore.UnRefSequencer()
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
		snapshot.FlushChunks(s.tsspPath, msName, s.lock, s.immTables, sids, flushChunkImp)
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
		panic("wal remove files failed: " + err.Error())
	}

	//This fail point is used in scenarios where "s.snapshotTbl" is not recycled
	failpoint.Inject("snapshot-table-reset-delay", func() {
		time.Sleep(2 * time.Second)
	})

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
		return nil, errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
	}
	return s.getSplitPointsByRowCount(idxes)
}

func (s *shard) getSplitPointsByRowCount(idxes []int64) ([]string, error) {
	return s.skIdx.GetSplitPointsByRowCount(idxes, func(name string, sid uint64) (int64, error) {
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
		return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
	}

	s.DisableDownSample()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.indexBuilder != nil {
		compWorker.UnregisterShard(s.ident.ShardID)
	}

	if s.skIdx != nil {
		if err := s.skIdx.Close(); err != nil {
			return err
		}
	}

	s.closed.Close()

	log.Info("start close shard...", zap.Uint64("id", s.ident.ShardID))
	s.cancelWalReplay()
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

	log.Info("close immutables", zap.Uint64("id", s.ident.ShardID))
	if err := s.immTables.Close(); err != nil {
		log.Error("close table store fail", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		return err
	}

	log.Info("success close immutables", zap.Uint64("id", s.ident.ShardID))

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
		e := errno.NewError(errno.WalRecordUnmarshalFailed, s.ident.ShardID, err.Error())
		s.log.Error("unmarshal rows fail", zap.Error(e))
		return nil
	}

	return s.writeRowsToTable(rows, nil)
}

func (s *shard) setCancelWalFunc(cancel context.CancelFunc) {
	s.cancelLock.Lock()
	s.cancelFn = cancel
	s.cancelLock.Unlock()
}

func (s *shard) replayWal() error {
	// make sure the wal files exist in the disk
	s.wal.restoreLogs()

	ctx, cancel := context.WithCancel(context.Background())
	s.setCancelWalFunc(cancel)
	s.wal.ref()
	if !s.isAsyncReplayWal {
		err := s.syncReplayWal(ctx)
		s.setCancelWalFunc(nil)
		s.loadWalDone = true
		s.wal.unref()
		return err
	}
	go func() {
		defer replayWalLimit.Release()
		replayWalLimit <- struct{}{}
		err := s.syncReplayWal(ctx)
		if err != nil {
			s.log.Error("async replay wal failed", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		}
		s.setCancelWalFunc(nil)
		s.loadWalDone = true
		s.wal.unref()
	}()
	return nil

}

func (s *shard) syncReplayWal(ctx context.Context) error {
	wStart := time.Now()

	walFileNames, err := s.wal.Replay(ctx,
		func(binary []byte) error {
			return s.writeWalBuffer(binary)
		},
	)
	if err != nil {
		return err
	}
	s.log.Info("replay wal files ok", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId))

	s.ForceFlush()
	s.log.Info("force flush shard ok", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId))
	err = s.wal.Remove(walFileNames)
	if err != nil {
		return err
	}
	s.log.Info("replay wal done", zap.Uint64("id", s.ident.ShardID),
		zap.Duration("time used", time.Since(wStart)), zap.Uint64("opId", s.opId))
	return nil
}

func (s *shard) Open(client metaclient.MetaClient) error {
	start := time.Now()
	s.log.Info("open shard start...", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId))
	var err error
	if e := s.DownSampleRecover(client); e != nil {
		s.log.Error("down sample recover failed", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId), zap.Error(e))
		return e
	}
	statistics.ShardStepDuration(s.GetID(), s.opId, "RecoverDownSample", time.Since(start).Nanoseconds(), false)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.immTables.SetOpId(s.GetID(), s.opId)
	maxTime, err := s.immTables.Open()
	if err != nil {
		s.log.Error("open shard failed", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId), zap.Error(err))
		return err
	}
	s.setMaxTime(maxTime)
	s.log.Info("open immutable done", zap.Uint64("id", s.ident.ShardID), zap.Duration("time used", time.Since(start)),
		zap.Int64("maxTime", maxTime), zap.Uint64("opId", s.opId))
	return nil
}

func (s *shard) DownSampleRecover(client metaclient.MetaClient) error {
	shardDir := filepath.Dir(s.tsspPath)
	dirs, err := fileops.ReadDir(shardDir)
	if err != nil {
		return err
	}
	for i := range dirs {
		dn := dirs[i].Name()
		if dn != immutable.DownSampleLogDir {
			continue
		}
		logDir := filepath.Join(shardDir, immutable.DownSampleLogDir)
		downSampleLogDirs, err := fileops.ReadDir(logDir)
		if err != nil {
			return err
		}
		logInfo := &DownSampleFilesInfo{}
		for _, v := range downSampleLogDirs {
			logName := v.Name()
			logFile := filepath.Join(logDir, logName)
			logInfo.reset()
			err = readDownSampleLogFile(logFile, logInfo)
			if err != nil {
				return err
			}
			s.mu.Lock()
			err = s.DownSampleRecoverReplaceFiles(logInfo, shardDir)
			s.mu.Unlock()
			if err != nil {
				return err
			}
			s.UpdateDownSampleOnShard(logInfo.taskID, logInfo.level)
			if e := client.UpdateShardDownSampleInfo(s.ident); e != nil {
				return e
			}
			lock := fileops.FileLockOption(*s.lock)
			if err = fileops.Remove(logFile, lock); err != nil {
				log.Error("remove downSample log file error", zap.Uint64("shardID", s.ident.ShardID), zap.String("dir", shardDir), zap.String("log", logFile), zap.Error(err))
			}
		}
	}
	return nil
}

func (s *shard) DownSampleRecoverReplaceFiles(logInfo *DownSampleFilesInfo, shardDir string) error {
	for k, v := range logInfo.Names {
		mstPath := filepath.Join(shardDir, immutable.TsspDirName, v)
		dir, err := fileops.ReadDir(mstPath)
		if err != nil {
			return err
		}
		err = renameFiles(mstPath, logInfo.NewFiles[k], dir, s.lock)
		if err != nil {
			return err
		}
	}

	for k, v := range logInfo.Names {
		mstPath := filepath.Join(shardDir, immutable.TsspDirName, v)
		dir, err := fileops.ReadDir(mstPath)
		if err != nil {
			return err
		}
		err = deleteFiles(mstPath, logInfo.OldFiles[k], dir, s.lock)
		if err != nil {
			return err
		}
	}
	return nil
}

func deleteFiles(mmDir string, files []string, dirs []os.FileInfo, lockPath *string) error {
	for _, v := range files {
		for j := range dirs {
			name := dirs[j].Name()
			tmp := v + immutable.GetTmpTsspFileSuffix()
			if name == v || tmp == name {
				fName := filepath.Join(mmDir, v)
				if _, err := fileops.Stat(fName); os.IsNotExist(err) {
					continue
				}
				lock := fileops.FileLockOption(*lockPath)
				if err := fileops.Remove(fName, lock); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func renameFiles(mmDir string, files []string, dirs []os.FileInfo, lockPath *string) error {
	for i := range dirs {
		name := dirs[i].Name()
		for k := range files {
			nameInLog := files[k]
			if nameInLog == name {
				lock := fileops.FileLockOption(*lockPath)
				normalName := nameInLog[:len(nameInLog)-len(immutable.GetTmpTsspFileSuffix())]
				oldName := filepath.Join(mmDir, nameInLog)
				newName := filepath.Join(mmDir, normalName)
				if err := fileops.RenameFile(oldName, newName, lock); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func readDownSampleLogFile(name string, info *DownSampleFilesInfo) error {
	fi, err := fileops.Stat(name)
	if err != nil {
		return fmt.Errorf("stat downsample log file fail")
	}

	fSize := fi.Size()
	if fSize < CRCLen {
		return fmt.Errorf("too small downsample log file(%v) size %v", name, fSize)
	}

	buf := make([]byte, int(fSize))
	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(name, os.O_RDONLY, 0640, lock, pri)
	if err != nil {
		return fmt.Errorf("read downSample log file fail")
	}
	defer util.MustClose(fd)

	n, err := fd.Read(buf)
	if err != nil || n != len(buf) {
		return fmt.Errorf("read downSample log file(%v) fail, file size:%v, read size:%v", name, fSize, n)
	}
	crcValue := buf[fSize-CRCLen:]
	currCrcValue := crc32.ChecksumIEEE(buf[0 : fSize-CRCLen])
	currBytes := make([]byte, CRCLen)
	binary.BigEndian.PutUint32(currBytes, currCrcValue)
	if !bytes.Equal(crcValue, currBytes) {
		return fmt.Errorf("invalid downsample log file(%v) crc", name)
	}

	if _, err = info.unmarshal(buf); err != nil {
		return fmt.Errorf("unmarshal downSample log %v fail", name)
	}

	return nil
}

func (s *shard) OpenAndEnable(client metaclient.MetaClient) error {
	var err error
	err = s.Open(client)
	if err != nil {
		return err
	}
	// replay wal files
	s.log.Info("replay wal start", zap.Uint64("id", s.ident.ShardID), zap.Uint64("opId", s.opId), zap.Bool("async replay", s.isAsyncReplayWal))
	wStart := time.Now()
	err = s.replayWal()
	if err != nil {
		s.log.Error("replay wal failed", zap.Uint64("id", s.ident.ShardID), zap.Error(err))
		return err
	}
	statistics.ShardStepDuration(s.GetID(), s.opId, "ReplayWalDone", time.Since(wStart).Nanoseconds(), false)
	s.log.Info("call replayWal method ok", zap.Uint64("id", s.ident.ShardID),
		zap.Duration("time used", time.Since(wStart)), zap.Uint64("opId", s.opId))

	// Must shard open successfully,
	// then add this shard to compaction worker.
	s.immTables.CompactionEnable()
	s.immTables.MergeEnable()
	compWorker.RegisterShard(s)
	s.EnableDownSample()
	s.wg.Add(1)
	go s.Snapshot()
	return nil
}

func (s *shard) TableStore() immutable.TablesStore {
	return s.immTables
}

func (s *shard) IsOutOfOrderFilesExist() bool {
	if s.immTables == nil {
		return false
	}
	return s.immTables.IsOutOfOrderFilesExist()
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
	if s.durationInfo.Tier == util.Hot && s.endTime.Add(s.durationInfo.TierDuration).Before(now) {
		return util.Hot, true
	}

	if s.durationInfo.Tier == util.Warm && s.endTime.Add(s.durationInfo.TierDuration).Before(now) {
		return util.Warm, true
	}
	return s.durationInfo.Tier, false
}

func (s *shard) ChangeShardTierToWarm() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tier == util.Warm {
		return
	}
	s.immTables.FreeAllMemReader()
	s.tier = util.Warm
}

func (s *shard) RPName() string {
	return s.ident.Policy
}

func (s *shard) DisableDownSample() {
	if atomic.CompareAndSwapInt32(&s.downSampleEn, 1, 0) {
		close(s.stopDownSample)
	}
	s.dswg.Wait()
}

func (s *shard) EnableDownSample() {
	if atomic.CompareAndSwapInt32(&s.downSampleEn, 0, 1) {
		s.stopDownSample = make(chan struct{})
	}
}

func (s *shard) CanDoDownSample() bool {
	if atomic.LoadInt32(&s.cacheClosed) > 0 || !s.downSampleEnabled() {
		return false
	}
	return true
}

func (s *shard) downSampleEnabled() bool {
	return atomic.LoadInt32(&s.downSampleEn) == 1
}

// notify async goroutines to cancel the wal replay
func (s *shard) cancelWalReplay() {
	s.cancelLock.RLock()
	if s.cancelFn == nil {
		s.cancelLock.RUnlock()
		return
	}
	s.cancelFn()
	s.cancelLock.RUnlock()
	s.wal.wait()
}

func (s *shard) StartDownSample(taskID uint64, level int, sdsp *meta.ShardDownSamplePolicyInfo, meta interface {
	UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, schemas, dlog := s.shardDownSampleTaskInfo.sdsp, s.shardDownSampleTaskInfo.schema, s.shardDownSampleTaskInfo.log
	var err error

	s.dswg.Add(1)
	defer s.dswg.Done()

	if !s.downSampleEnabled() {
		return nil
	}
	s.DisableCompAndMerge()

	lcLog := logger.NewLogger(errno.ModuleDownSample).SetZapLogger(dlog)
	taskNum := len(schemas)
	parallelism := maxDownSampleTaskNum
	filesMap := make(map[int]*immutable.TSSPFiles, taskNum)
	allDownSampleFiles := make(map[int][]immutable.TSSPFile, taskNum)
	dlog.Info("DownSample Start", zap.Any("shardId", info.ShardId))
	for i := 0; i < taskNum; i += parallelism {
		var num int
		if i+parallelism <= taskNum {
			num = parallelism
		} else {
			num = taskNum - i
		}
		err = s.StartDownSampleTaskBySchema(i, filesMap, allDownSampleFiles, schemas[i:i+num], info, dlog)
		if err != nil {
			break
		}
	}

	if !s.downSampleEnabled() {
		err = fmt.Errorf("downsample cancel")
	}
	if err == nil {
		mstNames := make([]string, 0)
		originFiles := make([][]immutable.TSSPFile, 0)
		newFiles := make([][]immutable.TSSPFile, 0)
		for k, v := range allDownSampleFiles {
			nameWithVer := schemas[k].Options().OptionsName()
			files := filesMap[k]
			var filesSlice []immutable.TSSPFile
			for _, f := range files.Files() {
				filesSlice = append(filesSlice, f)
				f.Unref()
				f.UnrefFileReader()
			}
			mstNames = append(mstNames, nameWithVer)
			originFiles = append(originFiles, filesSlice)
			newFiles = append(newFiles, v)
		}
		if e := s.ReplaceDownSampleFiles(mstNames, originFiles, newFiles, lcLog, taskID, level, sdsp, meta); e != nil {
			s.DeleteDownSampleFiles(allDownSampleFiles)
			return e
		}
		dlog.Info("DownSample Success", zap.Any("shardId", info.ShardId))
	} else {
		for _, v := range filesMap {
			for _, f := range v.Files() {
				f.Unref()
				f.UnrefFileReader()
			}
		}
		s.DeleteDownSampleFiles(allDownSampleFiles)
	}
	return err
}

func (s *shard) DeleteDownSampleFiles(allDownSampleFiles map[int][]immutable.TSSPFile) {
	for _, mstFiles := range allDownSampleFiles {
		for _, file := range mstFiles {
			fname := file.Path()
			if e := file.Remove(); e != nil {
				log.Error("remove downSample fail error", zap.String("name", fname), zap.Error(e))
			}
		}
	}
}

func (s *shard) ReplaceDownSampleFiles(mstNames []string, originFiles [][]immutable.TSSPFile, newFiles [][]immutable.TSSPFile,
	log *logger.Logger, taskID uint64, level int, sdsp *meta.ShardDownSamplePolicyInfo,
	meta interface {
		UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
	}) (err error) {
	if len(mstNames) == 0 || len(originFiles) == 0 || len(newFiles) == 0 {
		s.UpdateDownSampleOnShard(sdsp.TaskID, sdsp.DownSamplePolicyLevel)
		s.updateShardIentOnMeta(meta)
		return nil
	}
	if len(mstNames) != len(originFiles) || len(mstNames) != len(newFiles) {
		return fmt.Errorf("length of mst are not equal with files")
	}
	var logFile string
	logFile, err = s.writeDownSampleInfo(mstNames, originFiles, newFiles, taskID, level)
	if err != nil {
		if len(logFile) > 0 {
			lock := fileops.FileLockOption(*s.lock)
			_ = fileops.Remove(logFile, lock)
		}
		return err
	}
	s.updateDownSampleStat(originFiles, newFiles)
	if err = s.immTables.ReplaceDownSampleFiles(mstNames, originFiles, newFiles, true, func() {
		s.UpdateDownSampleOnShard(sdsp.TaskID, sdsp.DownSamplePolicyLevel)
	}); err != nil {
		return err
	}
	s.updateShardIentOnMeta(meta)
	lock := fileops.FileLockOption(*s.lock)
	if err = fileops.Remove(logFile, lock); err != nil {
		return err
	}
	return nil
}

func (s *shard) updateDownSampleStat(originFIles, newFiles [][]immutable.TSSPFile) {
	downSampleStatItem := statistics.NewDownSampleStatItem(s.ident.ShardID, s.ident.DownSampleID)
	downSampleStatItem.Level = int64(s.ident.DownSampleLevel)
	for i := range newFiles {
		downSampleStatItem.OriginalFileCount += int64(len(originFIles[i]))
		downSampleStatItem.DownSampleFileCount += int64(len(newFiles[i]))
		downSampleStatItem.OriginalFileSize += immutable.SumFilesSize(originFIles[i])
		downSampleStatItem.DownSampleFileSize += immutable.SumFilesSize(newFiles[i])
	}
	downSampleStatItem.Push()
}

func (s *shard) updateShardIentOnMeta(meta interface {
	UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
}) {
	var err error
	var retryCount int32
	for retryCount <= MaxRetryUpdateOnShardNum {
		retryCount++
		err = meta.UpdateShardDownSampleInfo(s.ident)
		if err == nil {
			return
		}
		time.Sleep(time.Second)
	}
	panic("update shard Ident to meta fail")
}

func (s *shard) writeDownSampleInfo(mstNames []string, originFiles [][]immutable.TSSPFile, newFiles [][]immutable.TSSPFile, taskID uint64, level int) (string, error) {
	shardDir := filepath.Dir(s.tsspPath)
	info := &DownSampleFilesInfo{
		taskID:   taskID,
		level:    level,
		Names:    mstNames,
		OldFiles: make([][]string, len(originFiles)),
		NewFiles: make([][]string, len(newFiles)),
	}
	for k := range originFiles {
		info.OldFiles[k] = make([]string, len(originFiles[k]))
		for mk, f := range originFiles[k] {
			info.OldFiles[k][mk] = filepath.Base(f.Path())
		}
	}
	for k := range newFiles {
		info.NewFiles[k] = make([]string, len(newFiles[k]))
		for mk, f := range newFiles[k] {
			info.NewFiles[k][mk] = filepath.Base(f.Path())
		}
	}
	fDir := filepath.Join(shardDir, immutable.DownSampleLogDir)
	lock := fileops.FileLockOption(*s.lock)
	if err := fileops.MkdirAll(fDir, 0750, lock); err != nil {
		return "", err
	}
	fName := filepath.Join(fDir, immutable.GenLogFileName(&downSampleLogSeq))

	buf := bufferpool.Get()
	defer bufferpool.Put(buf)

	buf = info.marshal(buf[:0])

	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(fName, os.O_CREATE|os.O_WRONLY, 0640, lock, pri)
	if err != nil {
		return "", err
	}

	newWriter := bufio.NewWriterSize(fd, BufferSize)

	sBuf, err := newWriter.Write(buf)
	if err != nil || sBuf != len(buf) {
		panic(err)
	}
	if err = newWriter.Flush(); err != nil {
		return "", err
	}

	if err = fd.Sync(); err != nil {
		panic(err)
	}

	return fName, fd.Close()
}

func (s *shard) StartDownSampleTaskBySchema(start int, filesMap map[int]*immutable.TSSPFiles, allDownSampleFiles map[int][]immutable.TSSPFile, schemas []hybridqp.Catalog, info *meta.ShardDownSamplePolicyInfo, logger *zap.Logger) error {
	var mstTaskNum int32
	var err error
	ch := executor.NewDownSampleStatePort(nil)
	defer ch.Close()
	downSampleStatItem := statistics.NewDownSampleStatistics()
	s.dswg.Add(len(schemas))
	downSampleStatItem.AddActive(int64(len(schemas)))
	for i := range schemas {
		mstName := schemas[i].Options().OptionsName()
		files, isExist := s.GetTSSPFiles(mstName, true)
		if !isExist || files.Len() == 0 {
			s.dswg.Done()
			downSampleStatItem.AddActive(-1)
			continue
		}
		e := s.StartDownSampleTask(start+i, mstName, files, ch, schemas[i].(*executor.QuerySchema), info.DbName, info.RpName)
		if e != nil {
			for _, v := range files.Files() {
				v.Unref()
				v.UnrefFileReader()
			}
			err = e
			logger.Warn(e.Error(), zap.Any("shardId", info.ShardId))
			s.dswg.Done()
			downSampleStatItem.AddActive(-1)
			downSampleStatItem.AddErrors(1)
			continue
		}
		logger.Info("DownSample Measurement Start", zap.Any("Measurement", mstName))
		mstTaskNum += 1
		filesMap[start+i] = files
	}
	if mstTaskNum == 0 {
		return err
	}
	for {
		state := <-ch.State

		mstTaskNum -= 1
		s.dswg.Done()
		downSampleStatItem.AddActive(-1)
		taskID := state.GetTaskID()
		allDownSampleFiles[taskID] = state.GetNewFiles()
		if state.GetErr() != nil {
			err = state.GetErr()
			downSampleStatItem.AddErrors(1)
			if mstTaskNum == 0 {
				return err
			}
			continue
		}
		logger.Info("DownSample Measurement Success", zap.Any("Measurement", schemas[taskID-start].Options().OptionsName()))
		if mstTaskNum == 0 {
			return err
		}
	}
}

func (s *shard) GetNewFilesSeqs(files []immutable.TSSPFile) []uint64 {
	var curSeq uint64
	var curNewSeq uint64
	var newSeqs []uint64
	for k, v := range files {
		_, seq := v.LevelAndSequence()
		if k == 0 || seq != curSeq {
			newSeq := s.immTables.NextSequence()
			curNewSeq = newSeq
			curSeq = seq
		}
		newSeqs = append(newSeqs, curNewSeq)
	}
	return newSeqs
}

func (s *shard) StartDownSampleTask(taskID int, mstName string, files *immutable.TSSPFiles, port *executor.DownSampleStatePort, querySchema *executor.QuerySchema, db string, rpName string) error {
	node := executor.NewLogicalTSSPScan(querySchema)
	newSeqs := s.GetNewFilesSeqs(files.Files())
	node.SetNewSeqs(newSeqs)
	node.SetFiles(files)
	node2 := executor.NewLogicalWriteIntoStorage(node, querySchema)
	var mmsTables *immutable.MmsTables
	var ok bool
	if mmsTables, ok = s.TableStore().(*immutable.MmsTables); !ok {
		return fmt.Errorf("Get MmsTables error")
	}
	node2.SetMmsTables(mmsTables)
	source := influxql.Sources{&influxql.Measurement{Database: db, RetentionPolicy: rpName, Name: mstName}}
	sidSequenceReader := NewTsspSequenceReader(nil, nil, nil, source, querySchema, files, newSeqs, s.stopDownSample)
	writeIntoStorage := NewWriteIntoStorageTransform(nil, nil, nil, source, querySchema, immutable.NewConfig(), mmsTables, s.ident.DownSampleLevel == 0)
	fileSequenceAgg := NewFileSequenceAggregator(querySchema, s.ident.DownSampleLevel == 0, s.startTime.UnixNano(), s.endTime.UnixNano())
	sidSequenceReader.GetOutputs()[0].Connect(fileSequenceAgg.GetInputs()[0])
	fileSequenceAgg.GetOutputs()[0].Connect(writeIntoStorage.GetInputs()[0])
	port.ConnectStateReserve(writeIntoStorage.GetOutputs()[0])
	writeIntoStorage.(*WriteIntoStorageTransform).SetTaskId(taskID)
	ctx := context.Background()
	go sidSequenceReader.Work(ctx)
	go fileSequenceAgg.Work(ctx)
	go writeIntoStorage.Work(ctx)
	return nil
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
	if !s.loadWalDone {
		return fmt.Errorf("async replay wal not finish")
	}
	s.DisableDownSample()
	defer s.EnableDownSample()
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

func (s *shard) GetShardDownSamplePolicy(policy *meta.DownSamplePolicyInfo) *meta.ShardDownSamplePolicyInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getShardDownSamplePolicyHelper(policy)
}

func (s *shard) getShardDownSamplePolicyHelper(policy *meta.DownSamplePolicyInfo) *meta.ShardDownSamplePolicyInfo {
	now := time.Now().UTC()
	if !downSampleInorder {
		for i := len(policy.DownSamplePolicies) - 1; i >= 0; i-- {
			if s.checkDownSample(policy.TaskID, policy.DownSamplePolicies[i], i, now) {
				return &meta.ShardDownSamplePolicyInfo{
					RpName:                s.RPName(),
					DownSamplePolicyLevel: i + 1,
					TaskID:                policy.TaskID,
				}
			}
		}
	} else {
		for i := 0; i < len(policy.DownSamplePolicies); i++ {
			if s.checkDownSample(policy.TaskID, policy.DownSamplePolicies[i], i, now) {
				return &meta.ShardDownSamplePolicyInfo{
					RpName:                s.RPName(),
					DownSamplePolicyLevel: i + 1,
					TaskID:                policy.TaskID,
				}
			}
		}
	}

	return nil
}

func (s *shard) checkDownSample(id uint64, p *meta.DownSamplePolicy, index int, now time.Time) bool {
	if !s.checkDownSampleID(id) {
		return false
	}
	if !s.endTime.Add(p.SampleInterval).Before(now) {
		return false
	}
	if index <= s.ident.DownSampleLevel-1 {
		return false
	}
	return true
}

func (s *shard) checkDownSampleID(id uint64) bool {
	if s.ident.DownSampleID == 0 {
		return true
	}
	return s.ident.DownSampleID == id
}

func (s *shard) SetShardDownSampleLevel(i int) {
	s.ident.DownSampleLevel = i
}

func (s *shard) UpdateDownSampleOnShard(id uint64, level int) {
	s.Ident().DownSampleLevel = level
	s.Ident().DownSampleID = id
}

func (s *shard) NewDownSampleTask(sdsp *meta.ShardDownSamplePolicyInfo, schema []hybridqp.Catalog, log *zap.Logger) {
	s.shardDownSampleTaskInfo = &shardDownSampleTaskInfo{
		sdsp:   sdsp,
		schema: schema,
		log:    log,
	}
}

func (s *shard) UpdateShardReadOnly(meta interface {
	UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
}) error {
	s.ident.ReadOnly = true
	var retryNum int
	var success bool
	for retryNum < MaxRetryUpdateOnShardNum {
		retryNum++
		if e := meta.UpdateShardDownSampleInfo(s.ident); e == nil {
			success = true
			break
		}
		time.Sleep(time.Second)
	}
	if success {
		return nil
	}
	return errno.NewError(errno.UpdateShardIdentFail)
}

func (s *shard) SetIndexBuilder(builder *tsi.IndexBuilder) {
	s.indexBuilder = builder
}

func (s *shard) CloseIndexBuilder() error {
	return s.indexBuilder.Close()
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
