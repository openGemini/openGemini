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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"go.uber.org/zap"
)

const (
	pathSeparator = "_"
)

var (
	reportLoadFrequency = time.Second
)

type PtNNLock struct {
}

type DBPTInfo struct {
	mu       sync.RWMutex
	database string
	id       uint32
	opt      netstorage.EngineOptions

	logger     *zap.Logger
	exeCount   int64
	offloading bool
	ch         chan bool

	path    string
	walPath string

	// Maintains a set of shards that are in the process of deletion.
	// This prevents new shards from being created while old ones are being deleted.
	pendingShardDeletes map[uint64]struct{}
	shards              map[uint64]Shard
	pendingIndexDeletes map[uint64]struct{}
	indexBuilder        map[uint64]*tsi.IndexBuilder // [indexId, IndexBuilderer]
	pendingShardTiering map[uint64]struct{}
	closed              *interruptsignal.InterruptSignal
	newestRpShard       map[string]uint64
	loadCtx             *metaclient.LoadCtx
	unload              chan struct{}
	wg                  *sync.WaitGroup
}

func NewDBPTInfo(db string, id uint32, dataPath, walPath string, ctx *metaclient.LoadCtx) *DBPTInfo {
	return &DBPTInfo{
		database:            db,
		id:                  id,
		exeCount:            0,
		offloading:          false,
		ch:                  nil,
		closed:              interruptsignal.NewInterruptSignal(),
		shards:              make(map[uint64]Shard),
		path:                dataPath,
		walPath:             walPath,
		indexBuilder:        make(map[uint64]*tsi.IndexBuilder),
		newestRpShard:       make(map[string]uint64),
		pendingShardDeletes: make(map[uint64]struct{}),
		pendingIndexDeletes: make(map[uint64]struct{}),
		pendingShardTiering: make(map[uint64]struct{}),
		loadCtx:             ctx,
		logger:              logger.GetLogger(),
		wg:                  &sync.WaitGroup{},
	}
}

func (dbPT *DBPTInfo) enableReportShardLoad() {
	dbPT.mu.Lock()
	defer dbPT.mu.Unlock()

	if dbPT.unload != nil {
		return
	}
	dbPT.unload = make(chan struct{})
	dbPT.wg.Add(1)
	go dbPT.reportLoad()
}

func (dbPT *DBPTInfo) reportLoad() {
	t := time.NewTicker(reportLoadFrequency)
	defer func() {
		dbPT.wg.Done()
		t.Stop()
		dbPT.logger.Info("dbpt reportLoad stoped", zap.Uint32("ptid", dbPT.id))
	}()

	for {
		select {
		case <-dbPT.closed.Signal():
			return
		case <-dbPT.unload:
			return
		case <-t.C:
			if dbPT.closed.Closed() {
				return
			}
			reportCtx := dbPT.loadCtx.GetReportCtx()
			rpStats := reportCtx.GetRpStat()
			dbPT.mu.RLock()
			for rp, shardID := range dbPT.newestRpShard {
				if dbPT.closed.Closed() {
					dbPT.mu.RUnlock()
					return
				}
				if dbPT.shards[shardID] == nil {
					continue
				}

				seriesCount := dbPT.shards[shardID].SeriesCount()

				if cap(rpStats) > len(rpStats) {
					rpStats = rpStats[:len(rpStats)+1]
				} else {
					rpStats = append(rpStats, &proto2.RpShardStatus{})
				}

				if rpStats[len(rpStats)-1] == nil {
					rpStats[len(rpStats)-1] = &proto2.RpShardStatus{}
				}
				rpStats[len(rpStats)-1].RpName = proto.String(rp)
				if rpStats[len(rpStats)-1].ShardStats == nil {
					rpStats[len(rpStats)-1].ShardStats = &proto2.ShardStatus{}
				}
				rpStats[len(rpStats)-1].ShardStats.SeriesCount = proto.Int(seriesCount)
				rpStats[len(rpStats)-1].ShardStats.ShardID = proto.Uint64(shardID)
				rpStats[len(rpStats)-1].ShardStats.ShardSize = proto.Uint64(dbPT.shards[shardID].Count())
				rpStats[len(rpStats)-1].ShardStats.MaxTime = proto.Int64(dbPT.shards[shardID].MaxTime())
			}
			dbPT.mu.RUnlock()
			dbPTStat := reportCtx.GetDBPTStat()
			dbPTStat.DB = proto.String(dbPT.database)
			dbPTStat.PtID = proto.Uint32(dbPT.id)
			dbPTStat.RpStats = append(dbPTStat.RpStats, rpStats...)
			dbPT.logger.Debug("try to send dbPTStat to storage", zap.Any("dbPTStat", reportCtx.DBPTStat))
			dbPT.loadCtx.LoadCh <- reportCtx
			dbPT.logger.Debug("success to send dbPTStat to storage", zap.Any("dbPTStat", reportCtx.DBPTStat))
		}
	}
}

func (dbPT *DBPTInfo) markOffload(ch chan bool) bool {
	dbPT.mu.Lock()
	defer dbPT.mu.Unlock()

	dbPT.offloading = true
	count := atomic.LoadInt64(&dbPT.exeCount)
	if count == 0 {
		dbPT.logger.Info("markOffload suc ", zap.String("db", dbPT.database), zap.Uint32("ptID", dbPT.id))
		return true
	}

	dbPT.logger.Warn("markOffload error ", zap.String("db", dbPT.database), zap.Uint32("ptID", dbPT.id), zap.Int64("ref", count))
	dbPT.ch = ch
	return false
}

func (dbPT *DBPTInfo) unMarkOffload() {
	dbPT.mu.Lock()
	defer dbPT.mu.Unlock()
	dbPT.logger.Info("unMarkOffload ", zap.String("db", dbPT.database), zap.Uint32("ptID", dbPT.id))
	dbPT.offloading = false
	if dbPT.ch != nil {
		close(dbPT.ch)
		dbPT.ch = nil
	}
}

func (dbPT *DBPTInfo) ref() bool {
	dbPT.mu.RLock()
	defer dbPT.mu.RUnlock()
	if dbPT.offloading {
		return false
	}
	atomic.AddInt64(&dbPT.exeCount, 1)
	return true
}

func (dbPT *DBPTInfo) unref() {
	dbPT.mu.RLock()
	defer dbPT.mu.RUnlock()
	newCount := atomic.AddInt64(&dbPT.exeCount, -1)
	if newCount < 0 {
		dbPT.logger.Error("dbpt ref error!", zap.String("database", dbPT.database), zap.Uint32("pt", dbPT.id))
	}
	if !dbPT.offloading || newCount != 0 {
		return
	}

	// offloading is true means drop database
	if dbPT.ch == nil {
		dbPT.logger.Error("dbPT chan is nil", zap.String("db", dbPT.database), zap.Uint32("pt id", dbPT.id))
		panic("chan is nil")
	}

	// notify drop database continue
	dbPT.ch <- true
	close(dbPT.ch)
	dbPT.ch = nil
}

func parseIndexDir(indexDirName string) (uint64, *meta.TimeRangeInfo, error) {
	indexDir := strings.Split(indexDirName, pathSeparator)
	if len(indexDir) != 3 {
		return 0, nil, ErrInvalidDir
	}

	indexID, err := strconv.ParseUint(indexDir[0], 10, 64)
	if err != nil {
		return 0, nil, ErrInvalidDir
	}

	startT, err := strconv.ParseInt(indexDir[1], 10, 64)
	if err != nil {
		return 0, nil, ErrInvalidDir
	}

	endT, err := strconv.ParseInt(indexDir[2], 10, 64)
	if err != nil {
		return 0, nil, ErrInvalidDir
	}

	startTime := meta.UnmarshalTime(startT)
	endTime := meta.UnmarshalTime(endT)
	return indexID, &meta.TimeRangeInfo{StartTime: startTime, EndTime: endTime}, nil
}

func (dbPT *DBPTInfo) OpenIndexes(rp string) error {
	indexPath := path.Join(dbPT.path, rp, IndexFileDirectory)
	indexDirs, err := fileops.ReadDir(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	type res struct {
		i   *tsi.IndexBuilder
		err error
	}
	resC := make(chan *res)
	n := 0
	for indexIdx := range indexDirs {
		if !indexDirs[indexIdx].IsDir() {
			dbPT.logger.Warn("skip load index because it's not a dir", zap.String("dir", indexDirs[indexIdx].Name()))
			continue
		}
		n++

		go func(indexDirName string) {
			indexID, tr, err := parseIndexDir(indexDirName)
			if err != nil {
				dbPT.logger.Error("parse index dir failed", zap.Error(err))
				resC <- &res{}
				return
			}
			ipath := path.Join(indexPath, indexDirName)
			// FIXME reload index

			// todo:is it necessary to mkdir again??
			lock := fileops.FileLockOption(dbPT.LockFile())
			if err := fileops.MkdirAll(ipath, 0750, lock); err != nil {
				resC <- &res{err: err}
				return
			}

			allIndexDirs, err := fileops.ReadDir(ipath)
			if err != nil {
				resC <- &res{err: err}
				return
			}

			indexIdent := &meta.IndexIdentifier{OwnerDb: dbPT.database, OwnerPt: dbPT.id, Policy: rp}
			indexIdent.Index = &meta.IndexDescriptor{IndexID: indexID, TimeRange: *tr}
			opts := new(tsi.Options).
				Ident(indexIdent).
				Path(ipath).
				IndexType(tsi.MergeSet).
				EndTime(tr.EndTime)

			dbPT.mu.Lock()
			// init indexBuilder and default indexRelation
			indexBuilder := tsi.NewIndexBuilder(opts)
			indexBuilder.Relations = make(map[uint32]*tsi.IndexRelation)

			// init primary Index
			primaryIndex, err := tsi.NewIndex(opts)
			if err != nil {
				resC <- &res{err: err}
				dbPT.mu.Unlock()
				return
			}
			primaryIndex.SetIndexBuilder(indexBuilder)
			indexRelation, err := tsi.NewIndexRelation(opts, primaryIndex, indexBuilder)
			if err != nil {
				resC <- &res{err: err}
				dbPT.mu.Unlock()
				return
			}
			indexBuilder.Relations[uint32(tsi.MergeSet)] = indexRelation

			// init other indexRelations if exist
			for idx := range allIndexDirs {
				if containOtherIndexes(allIndexDirs[idx].Name()) {
					idxType := tsi.GetIndexTypeByName(allIndexDirs[idx].Name())
					indexPath := path.Join(ipath, allIndexDirs[idx].Name())
					opts := new(tsi.Options).
						Ident(indexIdent).
						Path(indexPath).
						IndexType(idxType).
						EndTime(tr.EndTime)
					indexRelation, _ := tsi.NewIndexRelation(opts, primaryIndex, indexBuilder)
					indexBuilder.Relations[uint32(idxType)] = indexRelation
				}
			}
			err = indexBuilder.Open()
			dbPT.mu.Unlock()

			if err != nil {
				resC <- &res{err: err}
				return
			}
			resC <- &res{i: indexBuilder}
		}(indexDirs[indexIdx].Name())
	}

	for i := 0; i < n; i++ {
		res := <-resC
		if res.err != nil {
			err = res.err
			continue
		}
		if res.i == nil {
			continue
		}
		dbPT.mu.Lock()
		dbPT.indexBuilder[res.i.GetIndexID()] = res.i
		dbPT.mu.Unlock()
	}
	close(resC)
	return err
}

func containOtherIndexes(dirName string) bool {
	if dirName == "mergeset" || dirName == "kv" {
		return false
	}
	return true
}

func parseShardDir(shardDirName string) (uint64, uint64, *meta.TimeRangeInfo, error) {
	shardDir := strings.Split(shardDirName, pathSeparator)
	if len(shardDir) != 4 {
		return 0, 0, nil, ErrInvalidDir
	}
	shardID, err := strconv.ParseUint(shardDir[0], 10, 64)
	if err != nil {
		return 0, 0, nil, ErrInvalidDir
	}
	indexID, err := strconv.ParseUint(shardDir[3], 10, 64)
	if err != nil {
		return 0, 0, nil, ErrInvalidDir
	}

	startTime, err := strconv.ParseInt(shardDir[1], 10, 64)
	if err != nil {
		return 0, 0, nil, ErrInvalidDir
	}
	endTime, err := strconv.ParseInt(shardDir[2], 10, 64)
	if err != nil {
		return 0, 0, nil, ErrInvalidDir
	}
	tr := &meta.TimeRangeInfo{StartTime: meta.UnmarshalTime(startTime), EndTime: meta.UnmarshalTime(endTime)}
	return shardID, indexID, tr, nil
}

func (dbPT *DBPTInfo) OpenShards(rp string, durationInfos map[uint64]*meta.ShardDurationInfo) error {
	rpPath := path.Join(dbPT.path, rp)
	shardDirs, err := fileops.ReadDir(rpPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	walPath := path.Join(dbPT.walPath, rp)

	type res struct {
		s   Shard
		err error
	}

	resC := make(chan *res, len(shardDirs)-1)
	n := 0
	for shIdx := range shardDirs {
		if shardDirs[shIdx].Name() == IndexFileDirectory {
			continue
		}
		n++
		go func(shardDirName string) {
			shardId, indexID, tr, err := parseShardDir(shardDirName)
			if err != nil {
				dbPT.logger.Error("skip load shard invalid shard directory", zap.String("shardDir", shardDirName))
				resC <- &res{}
				return
			}

			if durationInfos[shardId] == nil {
				dbPT.logger.Error("skip load shard because database may be delete", zap.Uint64("shardId", shardId))
				resC <- &res{}
				return
			}
			indexBuilder, ok := dbPT.indexBuilder[indexID]
			if !ok || indexBuilder == nil {
				resC <- &res{err: fmt.Errorf("shard index not exist %d", indexID)}
				return
			}

			indexBuilder.SetDuration(durationInfos[shardId].DurationInfo.Duration)
			shardPath := path.Join(rpPath, shardDirName)
			shardWalPath := path.Join(walPath, shardDirName)

			sh := NewShard(shardPath, shardWalPath, &durationInfos[shardId].Ident, indexBuilder, &durationInfos[shardId].DurationInfo, tr, dbPT.opt)
			defer func() {
				if err != nil {
					_ = sh.Close()
					resC <- &res{err: err}
					return
				}
			}()
			if err = sh.NewShardKeyIdx(durationInfos[shardId].Ident.ShardType, shardPath); err != nil {
				return
			}

			if err = sh.Open(); err != nil {
				return
			}
			resC <- &res{s: sh}
		}(shardDirs[shIdx].Name())
	}

	err = nil
	for i := 0; i < n; i++ {
		r := <-resC
		if r.err != nil {
			err = r.err
			continue
		}
		if r.s == nil {
			continue
		}
		dbPT.mu.Lock()
		if dbPT.newestRpShard[rp] == 0 || dbPT.newestRpShard[rp] < r.s.GetID() {
			dbPT.newestRpShard[rp] = r.s.GetID()
		}
		dbPT.shards[r.s.GetID()] = r.s
		dbPT.mu.Unlock()
	}

	close(resC)

	return err
}

func (dbPT *DBPTInfo) SetOption(opt netstorage.EngineOptions) {
	dbPT.opt = opt
}

func (dbPT *DBPTInfo) NewShard(rp string, shardID uint64, timeRangeInfo *meta.ShardTimeRangeInfo) (Shard, error) {
	var err error
	dbPTLockFile := dbPT.LockFile()
	rpPath := path.Join(dbPT.path, rp)
	walPath := path.Join(dbPT.walPath, rp)

	lock := fileops.FileLockOption(dbPTLockFile)
	indexBuilder, ok := dbPT.indexBuilder[timeRangeInfo.OwnerIndex.IndexID]
	if !ok {
		indexID := strconv.Itoa(int(timeRangeInfo.OwnerIndex.IndexID))
		indexPath := indexID + pathSeparator + strconv.Itoa(int(meta.MarshalTime(timeRangeInfo.OwnerIndex.TimeRange.StartTime))) +
			pathSeparator + strconv.Itoa(int(meta.MarshalTime(timeRangeInfo.OwnerIndex.TimeRange.EndTime)))
		iPath := path.Join(rpPath, IndexFileDirectory, indexPath)

		if err := fileops.MkdirAll(iPath, 0750, lock); err != nil {
			return nil, err
		}

		indexIdent := &meta.IndexIdentifier{OwnerDb: dbPT.database, OwnerPt: dbPT.id, Policy: rp}
		indexIdent.Index = &meta.IndexDescriptor{IndexID: timeRangeInfo.OwnerIndex.IndexID,
			IndexGroupID: timeRangeInfo.OwnerIndex.IndexGroupID, TimeRange: timeRangeInfo.OwnerIndex.TimeRange}

		opts := new(tsi.Options).
			Ident(indexIdent).
			Path(iPath).
			IndexType(tsi.MergeSet).
			EndTime(timeRangeInfo.OwnerIndex.TimeRange.EndTime).
			Duration(timeRangeInfo.ShardDuration.DurationInfo.Duration)

		// init indexBuilder and default indexRelation
		indexBuilder = tsi.NewIndexBuilder(opts)
		indexid := timeRangeInfo.OwnerIndex.IndexID
		dbPT.indexBuilder[indexid] = indexBuilder
		dbPT.indexBuilder[indexid].Relations = make(map[uint32]*tsi.IndexRelation)

		primaryIndex, _ := tsi.NewIndex(opts)
		primaryIndex.SetIndexBuilder(indexBuilder)
		indexRelation, _ := tsi.NewIndexRelation(opts, primaryIndex, indexBuilder)
		dbPT.indexBuilder[indexid].Relations[uint32(tsi.MergeSet)] = indexRelation
		err = indexBuilder.Open()
		if err != nil {
			return nil, err
		}
	}

	id := strconv.Itoa(int(shardID))
	shardPath := id + pathSeparator + strconv.Itoa(int(meta.MarshalTime(timeRangeInfo.TimeRange.StartTime))) +
		pathSeparator + strconv.Itoa(int(meta.MarshalTime(timeRangeInfo.TimeRange.EndTime))) +
		pathSeparator + strconv.Itoa(int(timeRangeInfo.OwnerIndex.IndexID))
	dataPath := path.Join(rpPath, shardPath)
	walPath = path.Join(walPath, shardPath)
	if err = fileops.MkdirAll(dataPath, 0750, lock); err != nil {
		return nil, err
	}
	shardIdent := &meta.ShardIdentifier{ShardID: shardID, Policy: rp, OwnerDb: dbPT.database, OwnerPt: dbPT.id}
	sh := NewShard(dataPath, walPath, shardIdent, indexBuilder, &timeRangeInfo.ShardDuration.DurationInfo, &timeRangeInfo.TimeRange, dbPT.opt)
	err = sh.NewShardKeyIdx(timeRangeInfo.ShardType, dataPath)
	if err != nil {
		return nil, err
	}

	if err = sh.Open(); err != nil {
		_ = sh.Close()
		return nil, err
	}
	return sh, err
}

func (dbPT *DBPTInfo) LockFile() string {
	return filepath.Join(dbPT.path, "lock", "LOCK")
}

func (dbPT *DBPTInfo) Shard(id uint64) Shard {
	dbPT.mu.RLock()
	defer dbPT.mu.RUnlock()
	sh, ok := dbPT.shards[id]
	if !ok {
		return nil
	}

	return sh
}

func (dbPT *DBPTInfo) LoadAllShards(rp string, shardIDs []uint64) error {
	// TODO mjq load shard from wal
	return nil
}

func (dbPT *DBPTInfo) closeDBPt() error {
	dbPT.mu.Lock()

	dbPT.closed.Close()
	close(dbPT.unload)

	start := time.Now()
	dbPT.logger.Info("start close dbpt...", zap.String("db", dbPT.database), zap.Uint32("pt", dbPT.id))
	for id, sh := range dbPT.shards {
		if err := sh.Close(); err != nil {
			dbPT.mu.Unlock()
			dbPT.logger.Error("close shard fail", zap.Uint64("id", id), zap.Error(err))
			return err
		}
	}

	end := time.Now()
	d := end.Sub(start)
	dbPT.logger.Info("dbpt shard close done", zap.String("db", dbPT.database), zap.Uint32("pt", dbPT.id), zap.Duration("time used", d))
	for id, iBuild := range dbPT.indexBuilder {
		if err := iBuild.Close(); err != nil {
			dbPT.mu.Unlock()
			dbPT.logger.Error("close index fail", zap.Uint64("id", id), zap.Error(err))
			return err
		}
	}
	dbPT.mu.Unlock()
	d = time.Since(start)
	dbPT.logger.Info("close dbpt done", zap.String("db", dbPT.database), zap.Uint32("pt", dbPT.id), zap.Duration("time used", d))

	dbPT.wg.Wait()

	return nil
}

func (dbPT *DBPTInfo) seriesCardinality(measurements [][]byte, measurementCardinalityInfos []meta.MeasurementCardinalityInfo) ([]meta.MeasurementCardinalityInfo, error) {
	for _, indexBuilder := range dbPT.indexBuilder {
		var seriesCount uint64
		idx := indexBuilder.GetPrimaryIndex().(*tsi.MergeSetIndex)
		for i := range measurements {
			count, err := idx.SeriesCardinality(measurements[i], nil, tsi.DefaultTR)
			if err != nil {
				return nil, err
			}
			seriesCount += count
		}
		if seriesCount == 0 {
			continue
		}
		measurementCardinalityInfos = append(measurementCardinalityInfos,
			meta.MeasurementCardinalityInfo{
				CardinalityInfos: []meta.CardinalityInfo{{TimeRange: indexBuilder.Ident().Index.TimeRange, Cardinality: seriesCount}}})
	}
	return measurementCardinalityInfos, nil
}

func (dbPT *DBPTInfo) seriesCardinalityWithCondition(measurements [][]byte, condition influxql.Expr, measurementCardinalityInfos []meta.MeasurementCardinalityInfo) ([]meta.MeasurementCardinalityInfo, error) {
	for i := range measurements {
		cardinality := &meta.MeasurementCardinalityInfo{Name: string(measurements[i])}
		for _, indexBuilder := range dbPT.indexBuilder {
			idx := indexBuilder.GetPrimaryIndex().(*tsi.MergeSetIndex)
			stime := time.Now()
			count, err := idx.SeriesCardinality(measurements[i], condition, tsi.DefaultTR)
			if err != nil {
				return nil, err
			}
			if count == 0 {
				continue
			}
			log.Info("get series cardinality", zap.String("mst", string(measurements[i])), zap.Duration("cost", time.Since(stime)))
			cardinality.CardinalityInfos = append(cardinality.CardinalityInfos, meta.CardinalityInfo{
				TimeRange:   indexBuilder.Ident().Index.TimeRange,
				Cardinality: count})
		}
		measurementCardinalityInfos = append(measurementCardinalityInfos, *cardinality)
	}
	return measurementCardinalityInfos, nil
}
