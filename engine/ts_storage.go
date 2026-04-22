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
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
)

type tsstoreImpl struct {
}

func (storage *tsstoreImpl) WriteCols(s *shard, cols *record.Record, mst string, binaryCols []byte) error {
	return errors.New("not implement yet")
}

func (storage *tsstoreImpl) WriteIndex(idx *tsi.IndexBuilder, mw *mstWriteCtx) func() error {
	err := storage.writeIndex(idx, mw)
	return func() error {
		return err
	}
}

func (storage *tsstoreImpl) writeIndex(idx *tsi.IndexBuilder, mw *mstWriteCtx) error {
	mmPoints := mw.getMstMap()
	var err error
	var writeIndexRequired bool
	start := time.Now()

	mergeSet, ok := idx.GetPrimaryIndex().(*tsi.MergeSetIndex)
	if !ok {
		return errno.NewInvalidTypeError("*tsi.MergeSetIndex", idx.GetPrimaryIndex())
	}

	for _, mp := range mmPoints.D {
		rows, ok := mp.Value.(*[]influx.Row)
		if !ok {
			return errors.New("can't map mmPoints")
		}

		for i := range *rows {
			ri := &(*rows)[i]

			if mergeSet.EnabledTagArray() && ri.HasTagArray() {
				writeIndexRequired = true
			}

			if !writeIndexRequired {
				ri.SeriesId, err = mergeSet.GetSeriesIdBySeriesKey(ri.IndexKey)
				if err != nil {
					return err
				}
				// PrimaryId is equal to SeriesId by default.
				ri.PrimaryId = ri.SeriesId

				if ri.SeriesId == 0 {
					writeIndexRequired = true
				}
			}
		}
	}

	failpoint.Inject("SlowDownCreateIndex", nil)
	if writeIndexRequired {
		if err = idx.CreateIndexIfNotExists(mmPoints, true); err != nil {
			return err
		}
	} else {
		if err = idx.CreateSecondaryIndexIfNotExist(mmPoints); err != nil {
			return err
		}
	}
	atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(start).Nanoseconds())
	return nil
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

	s.snapshotTbl = s.activeTbl
	curSize := s.snapshotTbl.GetMemSize()

	s.activeTbl = s.memTablePool.Get(s.engineType)
	s.activeTbl.SetIdx(s.skIdx)
	if s.SnapShotter != nil {
		s.SnapShotter.RaftFlushC <- true
		atomic.StoreUint32(&s.SnapShotter.RaftFlag, 1)
	}
	s.snapshotLock.Unlock()

	start := time.Now()
	s.indexBuilder.Flush()

	s.commitSnapshot(s.snapshotTbl)
	nodeMutableLimit.freeResource(curSize)

	err = RemoveWalFiles(walFiles)
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
	var initLen int
	if existOrderFiles {
		initLen += orderTsspFiles.Len()
	}
	if existOutOfOrderFiles {
		initLen += outOfOrderTsspFiles.Len()
	}
	allFiles := make([]immutable.TSSPFile, 0, initLen)
	coldTmpFilesPath := make([]string, 0, initLen)
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
