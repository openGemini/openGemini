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
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

type columnstoreImpl struct {
	mu                  sync.RWMutex
	wg                  sync.WaitGroup
	client              metaclient.MetaClient
	snapshotContainer   []*mutable.MemTable
	snapshotInUsed      []bool
	lastSnapShotTime    uint64
	flushManager        map[string]mutable.FlushManager // mst -> flush detached or attached
	accumulateMetaIndex *sync.Map                       //mst -> immutable.AccumulateMetaIndex, record metaIndex for detached store
	mstsInfo            *sync.Map                       // map[cpu-001]meta.MeasurementInfo
	strategy            shardMoveStrategy               // config to determine which strategy
}

func newColumnstoreImpl(snapshotTblNum int) *columnstoreImpl {
	return &columnstoreImpl{
		lastSnapShotTime:    fasttime.UnixTimestamp(),
		snapshotContainer:   make([]*mutable.MemTable, snapshotTblNum),
		snapshotInUsed:      make([]bool, snapshotTblNum),
		mstsInfo:            &sync.Map{},
		flushManager:        make(map[string]mutable.FlushManager),
		accumulateMetaIndex: &sync.Map{},
		strategy:            newShardMoveStrategy(config.GetStoreConfig().ShardMoveLayoutSwitchEnabled),
	}
}

func (storage *columnstoreImpl) writeSnapshot(s *shard) {
	if s.SnapShotter != nil {
		atomic.StoreUint32(&s.SnapShotter.RaftFlag, 0)
	}
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

	idx := storage.getFreeSnapShotTbl()
	if idx == -1 {
		s.snapshotLock.Unlock()
		panic("error: there is not free snapShotTbl")
	}
	//set flushManager and accumulateMetaIndex
	s.activeTbl.MTable.SetFlushManagerInfo(storage.flushManager, storage.accumulateMetaIndex)
	storage.snapshotContainer[idx] = s.activeTbl
	storage.snapshotInUsed[idx] = true
	curSize := storage.snapshotContainer[idx].GetMemSize()

	s.activeTbl = s.memTablePool.Get(s.engineType)
	s.activeTbl.MTable.SetClient(storage.client)
	s.activeTbl.SetIdx(s.skIdx)
	s.snapshotLock.Unlock()
	// update last snapshot time
	atomic.StoreUint64(&storage.lastSnapShotTime, fasttime.UnixTimestamp())
	if s.SnapShotter != nil {
		s.SnapShotter.RaftFlushC <- true
		atomic.StoreUint32(&s.SnapShotter.RaftFlag, 1)
	}

	start := time.Now()
	s.indexBuilder.Flush()

	storage.wg.Add(1)
	go func() {
		defer storage.wg.Done()
		storage.flush(s, idx, curSize, walFiles, start)
	}()
}

func (storage *columnstoreImpl) flush(s *shard, idx int, curSize int64, walFiles []string, start time.Time) {
	s.commitSnapshot(storage.snapshotContainer[idx])
	nodeMutableLimit.freeResource(curSize)
	err := s.wal.Remove(walFiles)
	if err != nil {
		panic("wal remove files failed: " + err.Error())
	}

	//This fail point is used in scenarios where "s.snapshotTbl" is not recycled
	failpoint.Inject("snapshot-table-reset-delay", func() {
		time.Sleep(2 * time.Second)
	})

	s.snapshotLock.Lock()
	storage.snapshotContainer[idx].UnRef()
	storage.snapshotContainer[idx] = nil
	storage.snapshotInUsed[idx] = false
	s.snapshotLock.Unlock()

	atomic.AddInt64(&statistics.PerfStat.FlushSnapshotDurationNs, time.Since(start).Nanoseconds())
	atomic.AddInt64(&statistics.PerfStat.FlushSnapshotCount, 1)
}

func (storage *columnstoreImpl) WriteRows(s *shard, mw *mstWriteCtx) error {
	mmPoints := mw.getMstMap()
	mw.initWriteRowsCtx(nil, storage.mstsInfo)
	mw.writeRowsCtx.MsRowCount = s.msRowCount
	return s.activeTbl.MTable.WriteRows(s.activeTbl, mmPoints, mw.writeRowsCtx)
}

func (storage *columnstoreImpl) waitSnapshot() {
	storage.wg.Wait()
}

func (storage *columnstoreImpl) WriteRowsToTable(s *shard, rows influx.Rows, mw *mstWriteCtx, binaryRows []byte) error {
	var indexErr error
	var indexWg sync.WaitGroup
	indexWg.Add(1)
	err := storage.updateMstMap(s, rows, mw)
	if err != nil {
		return err
	}

	// alloc token
	// Token is released during the snapshot process, the number of tokens needs to be recorded before data is written.
	start := time.Now()
	curSize := calculateMemSize(rows)
	err = nodeMutableLimit.allocResource(curSize, mw.timer)
	atomic.AddInt64(&statistics.PerfStat.WriteGetTokenDurationNs, time.Since(start).Nanoseconds())
	if err != nil {
		s.log.Info("Alloc resource failed, need retry", zap.Int64("current mem size", curSize))
		return err
	}

	go func() {
		writeIndexStart := time.Now()
		indexErr = storage.WriteIndex(s, &rows, mw)
		if indexErr != nil {
			nodeMutableLimit.freeResource(curSize)
		}
		indexWg.Done()
		atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(writeIndexStart).Nanoseconds())
	}()
	err = s.writeRows(mw, binaryRows, curSize)
	indexWg.Wait()
	if err != nil {
		return err
	}

	return indexErr
}

func (storage *columnstoreImpl) UpdateMstsInfo(s *shard, msName, db, rp string) error {
	mst := stringinterner.InternSafe(msName)
	_, ok := storage.mstsInfo.Load(mst)
	if !ok {
		mInfo, err := storage.client.Measurement(db, rp, influx.GetOriginMstName(mst))
		if err != nil {
			return err
		}
		err = storage.checkMstInfo(mInfo)
		if err != nil {
			return err
		}
		storage.SetMstInfo(s, mst, mInfo)
	}
	return nil
}

func (storage *columnstoreImpl) updateMstMap(s *shard, rows influx.Rows, mw *mstWriteCtx) error {
	mmPoints := mw.getMstMap()
	tm := int64(math.MinInt64)
	for i := 0; i < len(rows); i++ {
		if s.closed.Closed() {
			return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
		}
		//skip StreamOnly data
		if rows[i].StreamOnly {
			continue
		}

		//update mstsInfo
		err := storage.UpdateMstsInfo(s, rows[i].Name, s.ident.OwnerDb, s.ident.Policy)
		if err != nil {
			return err
		}
		ri := cloneRowToDict(mmPoints, mw, &rows[i])
		if ri.Timestamp > tm {
			tm = ri.Timestamp
		}
		atomic.AddInt64(&statistics.PerfStat.WriteFieldsCount, int64(rows[i].Fields.Len())+int64(rows[i].Tags.Len()))
	}
	s.setMaxTime(tm)
	return nil
}

func (storage *columnstoreImpl) SetMstInfo(s *shard, name string, mstInfo *meta.MeasurementInfo) {
	storage.mstsInfo.Store(name, mstInfo)
	s.immTables.SetMstInfo(name, mstInfo)
}

func (storage *columnstoreImpl) SetAccumulateMetaIndex(name string, aMetaIndex *immutable.AccumulateMetaIndex) {
	storage.accumulateMetaIndex.Store(name, aMetaIndex)
}

func (storage *columnstoreImpl) shouldSnapshot(s *shard) bool {
	if s.activeTbl == nil || !storage.isSnapShotTblFree() || s.forceFlushing() {
		return false
	}
	return true
}

func (storage *columnstoreImpl) timeToSnapshot(s *shard) bool {
	return fasttime.UnixTimestamp() >= (atomic.LoadUint64(&s.lastWriteTime)+s.writeColdDuration) ||
		fasttime.UnixTimestamp() >= (atomic.LoadUint64(&storage.lastSnapShotTime)+s.forceSnapShotDuration)
}

func (storage *columnstoreImpl) isSnapShotTblFree() bool {
	for i := range storage.snapshotContainer {
		if storage.snapshotContainer[i] == nil {
			return true
		}
	}
	return false
}

func (storage *columnstoreImpl) ForceFlush(s *shard) {
	if s.indexBuilder == nil {
		return
	}
	s.enableForceFlush()
	defer s.disableForceFlush()

	s.waitSnapshot()
	idx := storage.getFreeSnapShotTbl()
	if idx == -1 {
		log.Debug("there is no free snapshot table", zap.Uint64("shard id", s.ident.ShardID))
		return
	}
	s.prepareSnapshot()
	s.storage.writeSnapshot(s)
	s.endSnapshot()
}

func (storage *columnstoreImpl) getFreeSnapShotTbl() int {
	for i := range storage.snapshotInUsed {
		if !storage.snapshotInUsed[i] {
			return i
		}
	}
	return -1
}

func (storage *columnstoreImpl) WriteCols(s *shard, cols *record.Record, mst string, binaryCols []byte) error {
	if cols == nil {
		return errors.New("write rec can not be nil")
	}
	s.wg.Add(1)
	s.writeWg.Add(1)
	defer func() {
		s.wg.Done()
		s.writeWg.Done()
	}()

	if s.ident.ReadOnly {
		err := errors.New("can not write cols to downSampled shard")
		log.Error("write into shard failed", zap.Error(err))
		if !getDownSampleWriteDrop() {
			return err
		}
		if !syscontrol.IsWriteColdShardEnabled() {
			err = errors.New("forbid by shard moving")
			log.Error("write into shard failed", zap.Error(err))
			return err
		}
		return nil
	}
	atomic.StoreUint64(&s.lastWriteTime, fasttime.UnixTimestamp())
	mw := getMstWriteRecordCtx(nodeMutableLimit.timeOut, s.engineType)
	defer putMstWriteRecordCtx(mw)

	// alloc token
	start := time.Now()
	curSize := int64(cols.Size())
	err := nodeMutableLimit.allocResource(curSize, mw.timer)
	atomic.AddInt64(&statistics.PerfStat.WriteGetTokenDurationNs, time.Since(start).Nanoseconds())
	if err != nil {
		s.log.Info("Alloc resource failed, need retry", zap.Int64("current mem size", curSize))
		return err
	}

	var indexErr error
	var indexWg sync.WaitGroup
	indexWg.Add(1)

	//update mstsInfo
	err = storage.UpdateMstsInfo(s, mst, s.ident.OwnerDb, s.ident.Policy)
	if err != nil {
		return err
	}

	//write index
	go func() {
		writeIndexStart := time.Now()
		indexErr = storage.WriteIndexForCols(s, cols, mst)
		indexWg.Done()
		atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(writeIndexStart).Nanoseconds())
	}()

	// write data and wal
	err = s.writeCols(cols, binaryCols, mst)
	indexWg.Wait()
	if err != nil {
		return err
	}
	s.activeTbl.AddMemSize(curSize)
	return indexErr
}

func (storage *columnstoreImpl) writeCols(s *shard, cols *record.Record, mst string) error {
	// update the row count for each mst
	storage.mu.Lock()
	mutable.UpdateMstRowCount(s.msRowCount, mst, int64(cols.RowNums()))
	storage.mu.Unlock()
	return s.activeTbl.MTable.WriteCols(s.activeTbl, cols, storage.mstsInfo, mst)
}

func (storage *columnstoreImpl) WriteIndex(s *shard, rows *influx.Rows, mw *mstWriteCtx) error {
	mmPoints := mw.getMstMap()
	return s.indexBuilder.CreateIndexIfNotExists(mmPoints, false)
}

func (storage *columnstoreImpl) WriteIndexForCols(s *shard, cols *record.Record, mst string) error {
	if s.closed.Closed() {
		return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
	}
	if config.IsLogKeeper() {
		return nil
	}
	mst = stringinterner.InternSafe(mst)
	msInfo, ok := mutable.GetMsInfo(mst, storage.mstsInfo)
	if !ok {
		s.log.Info("mstInfo is nil", zap.String("mst name", mst))
		return errors.New("measurement info is not found")
	}
	msInfo.SchemaLock.RLock()
	tagIndex := findTagIndex(cols.Schemas(), msInfo.Schema)
	msInfo.SchemaLock.RUnlock()
	// write index
	return s.indexBuilder.CreateIndexIfNotExistsByCol(cols, tagIndex, mst)
}

func (storage *columnstoreImpl) SetClient(client metaclient.MetaClient) {
	storage.client = client
}

func (storage *columnstoreImpl) checkMstInfo(mstInfo *meta.MeasurementInfo) error {
	if mstInfo == nil {
		return errors.New("mstInfo is nil")
	}
	return nil
}

func (storage *columnstoreImpl) getAllFiles(s *shard, mstName string) ([]immutable.TSSPFile, []string, error) {
	// get all order tssp files, since full compact is completed
	csFiles, existCsFiles := s.immTables.GetCSFiles(mstName)
	csFiles.RLock()
	defer csFiles.RUnlock()
	immutable.UnrefFilesReader(csFiles.Files()...)
	immutable.UnrefFiles(csFiles.Files()...)
	// has no both order and out of order files
	if !existCsFiles {
		return nil, nil, nil
	}

	files := make([]immutable.TSSPFile, 0, csFiles.Len())
	coldTmpFilesPath := make([]string, 0, csFiles.Len())
	return genAllFiles(s, csFiles.Files(), files, coldTmpFilesPath)
}

func (storage *columnstoreImpl) executeShardMove(s *shard) error {
	return storage.strategy.doShardMove(s)
}

type shardMoveStrategy interface {
	doShardMove(s *shard) error
}

func newShardMoveStrategy(layoutSwitchEnabled bool) shardMoveStrategy {
	if layoutSwitchEnabled {
		return &compactStrategy{}
	}
	return &writeStrategy{}
}

type compactStrategy struct {
}

func (c *compactStrategy) doShardMove(s *shard) error {
	return nil
}

type writeStrategy struct {
}

func (w *writeStrategy) doShardMove(s *shard) error {
	return s.doShardMove()
}
