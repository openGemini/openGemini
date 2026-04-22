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
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

type ColumnStoreImpl struct {
	db, rp              string
	mu                  sync.RWMutex
	wg                  sync.WaitGroup
	snapshotContainer   []*mutable.MemTable
	snapshotInUsed      []bool
	lastSnapShotTime    uint64
	flushManager        map[string]mutable.FlushManager // mst -> flush detached or attached
	accumulateMetaIndex *sync.Map                       //mst -> immutable.AccumulateMetaIndex, record metaIndex for detached store
	strategy            shardMoveStrategy               // config to determine which strategy
}

func newColumnStoreImpl(db, rp string, snapshotTblNum int) *ColumnStoreImpl {
	return &ColumnStoreImpl{
		db:                  db,
		rp:                  rp,
		lastSnapShotTime:    fasttime.UnixTimestamp(),
		snapshotContainer:   make([]*mutable.MemTable, snapshotTblNum),
		snapshotInUsed:      make([]bool, snapshotTblNum),
		flushManager:        make(map[string]mutable.FlushManager),
		accumulateMetaIndex: &sync.Map{},
		strategy:            newShardMoveStrategy(config.GetStoreConfig().ShardMoveLayoutSwitchEnabled),
	}
}

func (storage *ColumnStoreImpl) writeSnapshot(s *shard) {
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

	tbl := s.memTablePool.Get(s.engineType)
	tbl.MTable = mutable.NewCSMemTableImpl(storage.db, storage.rp)
	s.activeTbl = tbl
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

func (storage *ColumnStoreImpl) flush(s *shard, idx int, curSize int64, walFiles *WalFiles, start time.Time) {
	s.commitSnapshot(storage.snapshotContainer[idx])
	nodeMutableLimit.freeResource(curSize)
	err := removeWalFiles(walFiles)
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

func (storage *ColumnStoreImpl) waitSnapshot() {
	storage.wg.Wait()
}

func (storage *ColumnStoreImpl) SetAccumulateMetaIndex(name string, aMetaIndex *immutable.AccumulateMetaIndex) {
	storage.accumulateMetaIndex.Store(name, aMetaIndex)
}

func (storage *ColumnStoreImpl) shouldSnapshot(s *shard) bool {
	if s.activeTbl == nil || !storage.isSnapShotTblFree() || s.forceFlushing() {
		return false
	}
	return true
}

func (storage *ColumnStoreImpl) timeToSnapshot(s *shard) bool {
	return fasttime.UnixTimestamp() >= (atomic.LoadUint64(&s.lastWriteTime)+s.writeColdDuration) ||
		fasttime.UnixTimestamp() >= (atomic.LoadUint64(&storage.lastSnapShotTime)+s.forceSnapShotDuration)
}

func (storage *ColumnStoreImpl) isSnapShotTblFree() bool {
	for i := range storage.snapshotContainer {
		if storage.snapshotContainer[i] == nil {
			return true
		}
	}
	return false
}

func (storage *ColumnStoreImpl) ForceFlush(s *shard) {
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

func (storage *ColumnStoreImpl) getFreeSnapShotTbl() int {
	for i := range storage.snapshotInUsed {
		if !storage.snapshotInUsed[i] {
			return i
		}
	}
	return -1
}

func (storage *ColumnStoreImpl) WriteCols(s *shard, cols *record.Record, mst string, binaryCols []byte) error {
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

func (storage *ColumnStoreImpl) writeCols(s *shard, cols *record.Record, mst string) error {
	// update the row count for each mst
	storage.mu.Lock()
	mutable.UpdateMstRowCount(s.msRowCount, mst, int64(cols.RowNums()))
	storage.mu.Unlock()
	return s.activeTbl.MTable.WriteCols(s.activeTbl, cols, mst)
}

func (storage *ColumnStoreImpl) WriteIndex(idx *tsi.IndexBuilder, mw *mstWriteCtx) func() error {
	if !config.GetStoreConfig().ColumnStore.MergesetEnabled {
		return func() error {
			return nil
		}
	}

	var indexErr error
	var indexWg sync.WaitGroup
	indexWg.Add(1)

	go func() {
		defer indexWg.Done()
		writeIndexStart := time.Now()
		indexErr = idx.CreateIndexIfNotExists(mw.getMstMap(), false)
		atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(writeIndexStart).Nanoseconds())
	}()

	return func() error {
		indexWg.Wait()
		return indexErr
	}
}

func (storage *ColumnStoreImpl) WriteIndexForCols(s *shard, cols *record.Record, mstName string) error {
	if s.closed.Closed() {
		return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
	}
	if config.IsLogKeeper() || !config.GetStoreConfig().ColumnStore.MergesetEnabled {
		return nil
	}

	ident := colstore.NewMeasurementIdent(storage.db, storage.rp)
	ident.SetSafeName(mstName)

	mst, ok := colstore.MstManagerIns().GetByIdent(ident)
	if !ok {
		s.log.Info("mstInfo is nil", zap.String("mst name", ident.String()))
		return errors.New("measurement info is not found")
	}

	msInfo := mst.MeasurementInfo()

	msInfo.SchemaLock.RLock()
	tagIndex := findTagIndex(cols.Schemas(), msInfo.Schema)
	msInfo.SchemaLock.RUnlock()
	// write index
	return s.indexBuilder.CreateIndexIfNotExistsByCol(cols, tagIndex, ident.Name)
}

func (storage *ColumnStoreImpl) getAllFiles(s *shard, mstName string) ([]immutable.TSSPFile, []string, error) {
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

func (storage *ColumnStoreImpl) executeShardMove(s *shard) error {
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
