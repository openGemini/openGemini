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
	"sort"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

type tsstoreImpl struct {
}

func (storage *tsstoreImpl) WriteRows(s *shard, mw *mstWriteCtx) error {
	mmPoints := mw.getMstMap()
	mw.initWriteRowsCtx(s.addRowCountsBySid, nil)
	s.immTables.LoadSequencer()
	return s.activeTbl.MTable.WriteRows(s.activeTbl, mmPoints, mw.writeRowsCtx)
}

func (storage *tsstoreImpl) WriteRowsToTable(s *shard, rows influx.Rows, mw *mstWriteCtx, binaryRows []byte) error {
	// alloc token
	// Token is released during the snapshot process, the number of tokens needs to be recorded before data is written.
	start := time.Now()
	curSize := calculateMemSize(rows)
	err := nodeMutableLimit.allocResource(curSize, mw.timer)
	atomic.AddInt64(&statistics.PerfStat.WriteGetTokenDurationNs, time.Since(start).Nanoseconds())
	if err != nil {
		s.log.Info("Alloc resource failed, need retry", zap.Int64("current mem size", curSize))
		return err
	}

	// write index
	indexErr := storage.WriteIndex(s, &rows, mw)
	if indexErr != nil && !errno.Equal(indexErr, errno.SeriesLimited) {
		nodeMutableLimit.freeResource(curSize)
		return indexErr
	}

	// write data to mem table and write wal
	err = s.writeRows(mw, binaryRows, curSize)
	if err != nil {
		return err
	}

	return indexErr
}

func (storage *tsstoreImpl) WriteCols(s *shard, cols *record.Record, mst string, binaryCols []byte) error {
	return errors.New("not implement yet")
}

func (storage *tsstoreImpl) WriteIndex(s *shard, rowsPointer *influx.Rows, mw *mstWriteCtx) error {
	rows := *rowsPointer
	mmPoints := mw.getMstMap()
	var err error

	start := time.Now()
	if !sort.IsSorted(&rows) {
		sort.Stable(&rows)
	}
	atomic.AddInt64(&statistics.PerfStat.WriteSortIndexDurationNs, time.Since(start).Nanoseconds())

	var writeIndexRequired bool
	start = time.Now()
	tm := int64(math.MinInt64)
	primaryIndex := s.indexBuilder.GetPrimaryIndex()
	idx, _ := primaryIndex.(*tsi.MergeSetIndex)
	for i := 0; i < len(rows); i++ {
		if s.closed.Closed() {
			return errno.NewError(errno.ErrShardClosed, s.ident.ShardID)
		}
		//skip StreamOnly data
		if rows[i].StreamOnly {
			continue
		}

		ri := cloneRowToDict(mmPoints, mw, &rows[i])
		if ri.Timestamp > tm {
			tm = ri.Timestamp
		}
		if !writeIndexRequired {
			ri.SeriesId, err = idx.GetSeriesIdBySeriesKey(rows[i].IndexKey)
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
		if err = s.indexBuilder.CreateIndexIfNotExists(mmPoints, true); err != nil {
			return err
		}
	} else {
		if err = s.indexBuilder.CreateSecondaryIndexIfNotExist(mmPoints); err != nil {
			return err
		}
	}
	atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(start).Nanoseconds())
	return nil
}

func (storage *tsstoreImpl) SetClient(client metaclient.MetaClient) {}

func (storage *tsstoreImpl) SetMstInfo(s *shard, name string, mstInfo *meta.MeasurementInfo) {

}

func (storage *tsstoreImpl) SetAccumulateMetaIndex(name string, detachedMetaInfo *immutable.AccumulateMetaIndex) {
}

func (storage *tsstoreImpl) shouldSnapshot(s *shard) bool {
	if s.activeTbl == nil || s.snapshotTbl != nil || s.forceFlushing() {
		return false
	}
	return true
}

func (storage *tsstoreImpl) timeToSnapshot(s *shard) bool {
	return fasttime.UnixTimestamp() >= (atomic.LoadUint64(&s.lastWriteTime) + s.writeColdDuration)
}

func (storage *tsstoreImpl) ForceFlush(s *shard) {
	if s.indexBuilder == nil {
		return
	}
	s.enableForceFlush()
	defer s.disableForceFlush()

	s.waitSnapshot()
	s.prepareSnapshot()
	s.storage.writeSnapshot(s)
	s.endSnapshot()
}

func (storage *tsstoreImpl) writeSnapshot(s *shard) {
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

	s.activeTbl = s.memTablePool.Get(s.engineType)
	s.activeTbl.SetIdx(s.skIdx)
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
	s.snapshotTbl.UnRef()
	s.snapshotTbl = nil
	s.snapshotLock.Unlock()

	atomic.AddInt64(&statistics.PerfStat.FlushSnapshotDurationNs, time.Since(start).Nanoseconds())
	atomic.AddInt64(&statistics.PerfStat.FlushSnapshotCount, 1)
}

func (storage *tsstoreImpl) executeShardMove(s *shard) error {
	return s.doShardMove()
}

func (storage *tsstoreImpl) waitSnapshot() {}

func (storage *tsstoreImpl) getAllFiles(s *shard, mstName string) ([]immutable.TSSPFile, []string, error) {
	// get all order tssp files, since full compact is completed
	orderTsspFiles, existOrderFiles := s.immTables.GetTSSPFiles(mstName, true)
	outOfOrderTsspFiles, existOutOfOrderFiles := s.immTables.GetTSSPFiles(mstName, false)
	// has no both order and out of order files
	if !existOrderFiles && !existOutOfOrderFiles {
		return nil, nil, nil
	}

	var err error
	allFiles := make([]immutable.TSSPFile, 0, orderTsspFiles.Len())
	coldTmpFilesPath := make([]string, 0, orderTsspFiles.Len())
	if existOrderFiles {
		orderTsspFiles.RLock()
		allFiles, coldTmpFilesPath, err = genAllFiles(s, orderTsspFiles.Files(), allFiles, coldTmpFilesPath)
		immutable.UnrefFilesReader(orderTsspFiles.Files()...)
		immutable.UnrefFiles(orderTsspFiles.Files()...)
		orderTsspFiles.RUnlock()
		if err != nil {
			return nil, nil, err
		}
	}

	if existOutOfOrderFiles {
		outOfOrderTsspFiles.RLock()
		allFiles, coldTmpFilesPath, err = genAllFiles(s, outOfOrderTsspFiles.Files(), allFiles, coldTmpFilesPath)
		immutable.UnrefFilesReader(outOfOrderTsspFiles.Files()...)
		immutable.UnrefFiles(outOfOrderTsspFiles.Files()...)
		outOfOrderTsspFiles.RUnlock()
		if err != nil {
			return nil, nil, err
		}
	}
	return allFiles, coldTmpFilesPath, nil
}
