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
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/sysinfo"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

const IndexFileDirectory = "index"
const DataDirectory = "data"
const WalDirectory = "wal"

var log *zap.Logger

func init() {
	log = logger.GetLogger()
	netstorage.RegisterNewEngineFun(config.EngineType1, NewEngine)
}

type Engine struct {
	mu       sync.RWMutex // read/write lock for Engine
	closed   *interruptsignal.InterruptSignal
	dataPath string
	walPath  string
	ReadOnly bool

	engOpt       netstorage.EngineOptions
	DBPartitions map[string]map[uint32]*DBPTInfo

	log *zap.Logger

	loadCtx     *meta.LoadCtx
	dropMu      sync.RWMutex
	droppingDB  map[string]string
	droppingRP  map[string]string
	droppingMst map[string]string

	statCount int64
}

const maxInt = int(^uint(0) >> 1)

func sysTotalMemory() int {
	sysTotalMem, err := sysinfo.TotalMemory()
	if err != nil {
		panic(err)
	}

	totalMem := maxInt
	if sysTotalMem < uint64(maxInt) {
		totalMem = int(sysTotalMem)
	}

	mem := cgroup.GetMemoryLimit()
	if mem <= 0 || int64(int(mem)) != mem || int(mem) > totalMem {
		mem = cgroup.GetHierarchicalMemoryLimit()
		if mem <= 0 || int64(int(mem)) != mem || int(mem) > totalMem {
			return totalMem
		}
	}
	return int(mem)
}

func NewEngine(dataPath, walPath string, options netstorage.EngineOptions, ctx *meta.LoadCtx) (netstorage.Engine, error) {
	log = logger.GetLogger()
	eng := &Engine{
		closed:       interruptsignal.NewInterruptSignal(),
		dataPath:     dataPath,
		walPath:      walPath,
		engOpt:       options,
		DBPartitions: make(map[string]map[uint32]*DBPTInfo, 64),
		loadCtx:      ctx,
		log:          logger.GetLogger(),
		droppingDB:   make(map[string]string),
		droppingRP:   make(map[string]string),
		droppingMst:  make(map[string]string),
	}

	SetFullCompColdDuration(options.FullCompactColdDuration)
	immutable.SetMaxCompactor(options.MaxConcurrentCompactions)
	immutable.SetMaxFullCompactor(options.MaxFullCompactions)
	immutable.SetImmTableMaxMemoryPercentage(sysTotalMemory(), options.ImmTableMaxMemoryPercentage)
	immutable.SetCacheDataBlock(options.CacheDataBlock)
	immutable.SetCacheMetaData(options.CacheMetaBlock)
	immutable.EnableMmapRead(options.EnableMmapRead)
	immutable.EnableReadCache(options.ReadCacheLimit)
	immutable.SetCompactLimit(options.CompactThroughput, options.CompactThroughputBurst)
	immutable.SetSnapshotLimit(options.SnapshotThroughput, options.SnapshotThroughputBurst)
	immutable.SegMergeFlag(int32(options.CompactionMethod))
	immutable.Init()

	return eng, nil
}

func (e *Engine) GetLockFile() string {
	return ""
}

func (e *Engine) Open(ptIds []uint32, durationInfos map[uint64]*meta2.ShardDurationInfo) error {
	e.log.Info("start open engine...")
	start := time.Now()
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.OpenDurations, d.Nanoseconds())
		stat.UpdateEngineStatS()
		log.Info("open engine done", zap.Duration("time used", d))
	}(start)

	lockFile := fileops.FileLockOption(e.GetLockFile())

	if err := fileops.MkdirAll(e.dataPath, 0750, lockFile); err != nil {
		atomic.AddInt64(&stat.EngineStat.OpenErrors, 1)
		return err
	}

	err := e.loadShards(ptIds, durationInfos)
	if err != nil {
		atomic.AddInt64(&stat.EngineStat.OpenErrors, 1)
		return err
	}

	return nil
}

func (e *Engine) loadShards(ptIds []uint32, durationInfos map[uint64]*meta2.ShardDurationInfo) error {
	dataPath := path.Join(e.dataPath, DataDirectory)
	_, err := fileops.Stat(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	errChan := make(chan error)
	n := 0
	dbRpLock := make(map[string]string)
	for _, sdi := range durationInfos {
		dbRpPath := path.Join(sdi.Ident.OwnerDb, strconv.Itoa(int(sdi.Ident.OwnerPt)), sdi.Ident.Policy)
		if _, ok := dbRpLock[dbRpPath]; ok {
			continue
		}
		dbRpLock[dbRpPath] = ""
		n++
		go func(db string, pt uint32, rp string) {
			err := e.loadDbRp(db, pt, rp, durationInfos)
			if err != nil {
				e.log.Error("fail to load db rp", zap.String("db", db), zap.Uint32("pt", pt),
					zap.String("rp", rp), zap.Error(err))
			}
			errChan <- err
		}(sdi.Ident.OwnerDb, sdi.Ident.OwnerPt, sdi.Ident.Policy)
	}

	for i := 0; i < n; i++ {
		err := <-errChan
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.closed.Close()

	start := time.Now()
	log.Info("start close engine...")
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.CloseDurations, d.Nanoseconds())
		stat.UpdateEngineStatS()
		log.Info("close engine done", zap.Duration("time used", d))
	}(start)

	wg := sync.WaitGroup{}
	for _, dbpts := range e.DBPartitions {
		wg.Add(len(dbpts))
	}

	for _, dbpts := range e.DBPartitions {
		for _, pt := range dbpts {
			go func(pt *DBPTInfo) {
				defer wg.Done()

				if err := pt.closeDBPt(); err != nil {
					atomic.AddInt64(&stat.EngineStat.CloseErrors, 1)
					log.Error("failed to close db pt", zap.String("path", pt.path))
				}
			}(pt)
		}
	}

	wg.Wait()

	return nil
}

func (e *Engine) ForceFlush() {
	e.mu.RLock()
	defer e.mu.RUnlock()
	start := time.Now()
	log.Info("start force flush shard...")

	flushDBPT := func(db string, ptID uint32) error {
		err := e.checkAndAddRefPTNoLock(db, ptID)
		if err != nil {
			return err
		}
		dbPTInfo := e.DBPartitions[db][ptID]
		defer dbPTInfo.unref()
		dbPTInfo.mu.RLock()
		defer dbPTInfo.mu.RUnlock()
		for _, sh := range dbPTInfo.shards {
			sh.ForceFlush()
		}
		return nil
	}

	for db, partitions := range e.DBPartitions {
		for id := range partitions {
			err := flushDBPT(db, id)
			if err != nil {
				continue
			}
		}
	}

	d := time.Since(start)
	log.Info("shard flush done", zap.Duration("time used(s)", d))
}

func (e *Engine) SetReadOnly(readonly bool) {
	e.ReadOnly = readonly
}

func (e *Engine) UpdateShardDurationInfo(info *meta2.ShardDurationInfo) error {
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(info.Ident.OwnerDb, info.Ident.OwnerPt); err != nil {
		e.mu.RUnlock()
		return err
	}
	dbPT := e.DBPartitions[info.Ident.OwnerDb][info.Ident.OwnerPt]
	e.mu.RUnlock()
	defer e.unrefDBPT(info.Ident.OwnerDb, info.Ident.OwnerPt)

	dbPT.mu.RLock()
	defer dbPT.mu.RUnlock()
	shard := dbPT.shards[info.Ident.ShardID]
	if shard == nil || shard.GetIndexBuild() == nil {
		return nil
	}
	log.Info("duration info", zap.Uint64("shardId", info.Ident.ShardID),
		zap.Uint64("shard group id", info.Ident.ShardGroupID),
		zap.Duration("duration", info.DurationInfo.Duration))
	shard.Ident().ShardGroupID = info.Ident.ShardGroupID
	shard.Duration().Duration = info.DurationInfo.Duration
	shard.Duration().Tier = info.DurationInfo.Tier
	shard.Duration().TierDuration = info.DurationInfo.TierDuration
	shard.GetIndexBuild().SetDuration(info.DurationInfo.Duration)
	return nil
}

func (e *Engine) ExpiredShards() []*meta2.ShardIdentifier {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var res []*meta2.ShardIdentifier
	for db := range e.DBPartitions {
		for pt := range e.DBPartitions[db] {
			for sid := range e.DBPartitions[db][pt].shards {
				if e.DBPartitions[db][pt].shards[sid].Expired() {
					res = append(res, e.DBPartitions[db][pt].shards[sid].Ident())
				}
			}
		}
	}
	return res
}

func (e *Engine) ExpiredIndexes() []*meta2.IndexIdentifier {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var res []*meta2.IndexIdentifier
	for db := range e.DBPartitions {
		for pt := range e.DBPartitions[db] {
			for idxId := range e.DBPartitions[db][pt].indexBuilder {
				if e.DBPartitions[db][pt].indexBuilder[idxId].Expired() {
					res = append(res, e.DBPartitions[db][pt].indexBuilder[idxId].Ident())
				}
			}
		}
	}
	return res
}

// todo:need confirm
func (e *Engine) DeleteShard(db string, ptId uint32, shardID uint64) error {
	e.log.Info("start delete shard...", zap.String("db", db), zap.Uint64("shardID", shardID))
	start := time.Now()
	atomic.AddInt64(&stat.EngineStat.DelShardCount, 1)
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.DelShardDuration, d.Nanoseconds())
		stat.UpdateEngineStatS()
		e.log.Info("delete shard done", zap.String("db", db), zap.Uint64("shardID", shardID),
			zap.Duration("time used", d))
	}(start)

	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}
	dbPtInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	defer e.unrefDBPT(db, ptId)

	dbPtInfo.mu.Lock()
	sh, ok := dbPtInfo.shards[shardID]
	if !ok {
		dbPtInfo.mu.Unlock()
		return ErrShardNotFound
	}

	if _, ok := dbPtInfo.pendingShardDeletes[shardID]; ok {
		dbPtInfo.mu.Unlock()
		return fmt.Errorf("shard %d already in deleting", shardID)
	}
	// remove from pt map
	delete(dbPtInfo.shards, shardID)
	dbPtInfo.pendingShardDeletes[shardID] = struct{}{}
	dbPtInfo.mu.Unlock()

	defer func(pt *DBPTInfo) {
		pt.mu.Lock()
		delete(pt.pendingShardDeletes, shardID)
		pt.mu.Unlock()
	}(dbPtInfo)

	// start close shard and release resource
	if err := sh.Close(); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}

	lock := fileops.FileLockOption("")
	// remove shard's wal&data on-disk, index data will not delete right now
	if err := fileops.RemoveAll(sh.DataPath(), lock); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}
	if err := fileops.RemoveAll(sh.WalPath(), lock); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}

	return nil
}

func (e *Engine) DeleteIndex(db string, ptId uint32, indexID uint64) error {
	e.log.Info("start delete index...", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("indexID", indexID))
	start := time.Now()
	atomic.AddInt64(&stat.EngineStat.DelIndexCount, 1)
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.DelIndexDuration, d.Nanoseconds())
		stat.UpdateEngineStatS()
		e.log.Info("delete index done", zap.String("db", db), zap.Uint64("indexID", indexID),
			zap.Duration("time used", d))
	}(start)

	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		atomic.AddInt64(&stat.EngineStat.DelIndexErr, 1)
		return err
	}
	dbPtInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	defer e.unrefDBPT(db, ptId)

	dbPtInfo.mu.Lock()
	iBuild, ok := dbPtInfo.indexBuilder[indexID]
	if !ok {
		dbPtInfo.mu.Unlock()
		return ErrIndexNotFound
	}

	if _, ok := dbPtInfo.pendingIndexDeletes[indexID]; ok {
		dbPtInfo.mu.Unlock()
		return fmt.Errorf("index %d already in deleting", indexID)
	}

	delete(dbPtInfo.indexBuilder, indexID)
	dbPtInfo.pendingIndexDeletes[indexID] = struct{}{}
	dbPtInfo.mu.Unlock()

	defer func(pt *DBPTInfo) {
		pt.mu.Lock()
		delete(pt.pendingIndexDeletes, indexID)
		pt.mu.Unlock()
	}(dbPtInfo)

	if err := iBuild.Close(); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelIndexErr, 1)
		return err
	}

	lock := fileops.FileLockOption("")
	if err := fileops.RemoveAll(iBuild.Path(), lock); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelIndexErr, 1)
		return err
	}

	iBuild = nil

	return nil
}

func (e *Engine) FetchShardsNeedChangeStore() (shardsToWarm, shardsToCold []*meta2.ShardIdentifier) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for db := range e.DBPartitions {
		for pt := range e.DBPartitions[db] {
			for _, shard := range e.DBPartitions[db][pt].shards {
				tier, expired := shard.TierDurationExpired()
				if !expired {
					continue
				}
				if tier == meta2.Hot {
					shardsToWarm = append(shardsToWarm, shard.Ident())
				} else {
					shardsToCold = append(shardsToCold, shard.Ident())
				}
			}
		}
	}
	return shardsToWarm, shardsToCold
}

func (e *Engine) ChangeShardTierToWarm(db string, ptId uint32, shardID uint64) error {
	log.Info("change hot shard to warm", zap.String("db", db), zap.Uint64("shardID", shardID))
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		return err
	}
	dbPtInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	defer e.unrefDBPT(db, ptId)

	dbPtInfo.mu.Lock()
	sh, ok := dbPtInfo.shards[shardID]
	if !ok {
		dbPtInfo.mu.Unlock()
		return ErrShardNotFound
	}

	if _, ok := dbPtInfo.pendingShardTiering[shardID]; ok {
		dbPtInfo.mu.Unlock()
		return fmt.Errorf("shard %d already in changing tier", shardID)
	}
	// remove from pt map
	dbPtInfo.pendingShardTiering[shardID] = struct{}{}
	dbPtInfo.mu.Unlock()

	defer func(pt *DBPTInfo) {
		pt.mu.Lock()
		delete(pt.pendingShardTiering, shardID)
		pt.mu.Unlock()
	}(dbPtInfo)

	// start change shard tier
	sh.ChangeShardTierToWarm()

	return nil
}

func (e *Engine) WriteRows(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte) error {
	if err := e.checkReadonly(); err != nil {
		return err
	}
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		return err
	}
	defer e.unrefDBPT(db, ptId)

	dbPTInfo := e.DBPartitions[db][ptId]
	dbPTInfo.mu.RLock()
	sh, ok := dbPTInfo.shards[shardID]
	if !ok {
		dbPTInfo.mu.RUnlock()
		e.mu.RUnlock()
		return ErrShardNotFound
	}
	dbPTInfo.mu.RUnlock()
	e.mu.RUnlock()

	return sh.WriteRows(rows, binaryRows)
}

func (e *Engine) checkReadonly() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.ReadOnly {
		return errno.NewError(errno.ErrWriteReadonly)
	}
	return nil
}

func (e *Engine) CreateShard(db, rp string, ptId uint32, shardID uint64, timeRangeInfo *meta2.ShardTimeRangeInfo) error {
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		return err
	}
	dbPTInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()
	defer e.unrefDBPT(db, ptId)

	dbPTInfo.mu.Lock()
	dbPTInfo.SetOption(e.engOpt)
	defer dbPTInfo.mu.Unlock()
	_, ok := dbPTInfo.shards[shardID]
	if !ok {
		sh, err := dbPTInfo.NewShard(rp, shardID, timeRangeInfo)
		if err != nil {
			return err
		}
		dbPTInfo.shards[shardID] = sh
		existShardID, ok := dbPTInfo.newestRpShard[rp]
		if !ok || existShardID < shardID {
			dbPTInfo.newestRpShard[rp] = shardID
		}
	}
	return nil
}

func (e *Engine) GetShardSplitPoints(db string, ptId uint32, shardID uint64, idxes []int64) ([]string, error) {
	e.mu.RLock()
	if !e.isDBPtExist(db, ptId) {
		e.mu.RUnlock()
		return nil, ErrPTNotFound
	}
	dbPtInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	shard := dbPtInfo.Shard(shardID)
	if shard == nil {
		return nil, ErrShardNotFound
	}

	return shard.GetSplitPoints(idxes)
}

func (e *Engine) isDBPtExist(db string, ptId uint32) bool {
	if dbPT, dbExist := e.DBPartitions[db]; dbExist {
		if _, dbPTExist := dbPT[ptId]; dbPTExist {
			return true
		}
	}
	return false
}

func (e *Engine) addDBPTInfo(dbPTInfo *DBPTInfo) {
	dbPT, dbExist := e.DBPartitions[dbPTInfo.database]
	if dbExist {
		if _, dbPTExist := dbPT[dbPTInfo.id]; !dbPTExist {
			dbPT[dbPTInfo.id] = dbPTInfo
		}
	} else {
		dbPT = make(map[uint32]*DBPTInfo)
		dbPT[dbPTInfo.id] = dbPTInfo
		e.DBPartitions[dbPTInfo.database] = dbPT
	}
}

func (e *Engine) dropDBPTInfo(database string, ptID uint32) {
	if dbPT, dbExist := e.DBPartitions[database]; dbExist {
		delete(dbPT, ptID)
		if len(dbPT) == 0 {
			delete(e.DBPartitions, database)
		}
	}
}

func (e *Engine) CreateDBPT(db string, pt uint32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ptPath := path.Join(e.dataPath, DataDirectory, db, strconv.Itoa(int(pt)))
	walPath := path.Join(e.walPath, WalDirectory, db, strconv.Itoa(int(pt)))
	dbPTInfo := NewDBPTInfo(db, pt, ptPath, walPath, e.loadCtx)
	e.addDBPTInfo(dbPTInfo)
	dbPTInfo.SetOption(e.engOpt)
	dbPTInfo.enableReportShardLoad()
}

func (e *Engine) createDBPTIfNotExist(db string, pt uint32) {
	e.mu.RLock()
	if e.isDBPtExist(db, pt) {
		e.mu.RUnlock()
		return
	}
	e.mu.RUnlock()
	e.CreateDBPT(db, pt)
}

func (e *Engine) loadDbRp(db string, pt uint32, rp string, durationInfos map[uint64]*meta2.ShardDurationInfo) error {
	rpPath := path.Join(e.dataPath, DataDirectory, db, strconv.Itoa(int(pt)), rp)
	_, err := fileops.Stat(rpPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		log.Warn("fail to get rpPath stat", zap.String("rpPath", rpPath), zap.Error(err))
		return err
	}
	e.createDBPTIfNotExist(db, pt)
	e.mu.RLock()
	dbPTInfo := e.DBPartitions[db][pt]
	e.mu.RUnlock()

	err = dbPTInfo.OpenIndexes(rp)
	if err != nil {
		e.log.Error("open index failed", zap.String("db", db), zap.Uint32("pt", pt), zap.String("rp", rp), zap.Error(err))
		return err
	}

	err = dbPTInfo.OpenShards(rp, durationInfos)
	if err != nil {
		e.log.Error("open shards failed", zap.String("db", db), zap.Uint32("pt", pt), zap.String("rp", rp), zap.Error(err))
		return err
	}

	return nil
}

func (e *Engine) startDrop(name string, droppingMap map[string]string) error {
	for st := time.Now(); time.Since(st) < 5*time.Minute; {
		e.dropMu.Lock()
		if _, ok := droppingMap[name]; ok {
			e.dropMu.Unlock()
			log.Warn("concurrency delete", zap.String("del", name))
			time.Sleep(5 * time.Second)
			continue
		}
		droppingMap[name] = ""
		e.dropMu.Unlock()
		return nil
	}
	return fmt.Errorf("concurrency delete database timeout")
}

func (e *Engine) endDrop(name string, droppingMap map[string]string) {
	e.dropMu.Lock()
	delete(droppingMap, name)
	e.dropMu.Unlock()
}

func (e *Engine) DeleteDatabase(db string, ptId uint32) (err error) {
	traceId := tsi.GenerateUUID()
	begin := time.Now()
	e.log.Info("drop database begin", zap.String("db", db), zap.Uint64("trace_id", traceId))
	defer func() {
		e.log.Info("drop database finish",
			zap.Error(err),
			zap.String("time use", time.Since(begin).String()),
			zap.String("db", db), zap.Uint64("trace_id", traceId))
	}()

	if err := e.startDrop(db, e.droppingDB); err != nil {
		return err
	}
	defer e.endDrop(db, e.droppingDB)

	atomic.AddInt64(&stat.EngineStat.DropDatabaseCount, 1)
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.DropDatabaseDurations, d.Nanoseconds())
		stat.UpdateEngineStatS()
	}(begin)

	e.mu.RLock()
	dbInfo, ok := e.DBPartitions[db]

	if !ok {
		e.mu.RUnlock()
		dataPath := path.Join(e.dataPath, DataDirectory, db, strconv.Itoa(int(ptId)))
		walPath := path.Join(e.walPath, WalDirectory, db, strconv.Itoa(int(ptId)))
		return deleteDataAndWalPath(dataPath, walPath)
	}

	for ptId, dbPTInfo := range dbInfo {
		done := make(chan bool, 1)
		if ok := dbPTInfo.markOffload(done); !ok {
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				log.Warn("offload dbPt timeout", zap.String("db", db), zap.Uint32("pt id", ptId))
				e.unMarkOffloadInLock(db)
				e.mu.RUnlock()
				atomic.AddInt64(&stat.EngineStat.DropDatabaseErrs, 1)
				return meta2.ErrConflictWithIo
			}
		}
		close(dbPTInfo.unload)
		dbPTInfo.wg.Wait()
	}

	err = e.deleteShardsAndIndexes(db)
	if err != nil {
		e.unMarkOffloadInLock(db)
		e.mu.RUnlock()
		atomic.AddInt64(&stat.EngineStat.DropDatabaseErrs, 1)
		return err
	}
	for ptId := range dbInfo {
		if err = deleteDataAndWalPath(dbInfo[ptId].path, dbInfo[ptId].walPath); err != nil {
			e.unMarkOffloadInLock(db)
			e.mu.RUnlock()
			atomic.AddInt64(&stat.EngineStat.DropDatabaseErrs, 1)
			return err
		}
	}
	e.mu.RUnlock()
	e.dropDBPt(db)
	return nil
}

func deleteDataAndWalPath(dataPath, walPath string) error {
	if err := deleteDir(dataPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := deleteDir(walPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func deleteDir(path string) error {
	lock := fileops.FileLockOption("")
	if err := fileops.RemoveAll(path, lock); err != nil {
		return err
	}

	return nil
}

func (e *Engine) unMarkOffloadInLock(db string) {
	for _, dbPTInfo := range e.DBPartitions[db] {
		if dbPTInfo.offloading {
			dbPTInfo.unMarkOffload()
		}
	}
}

func (e *Engine) deleteShardsAndIndexes(db string) error {
	for _, dbPTInfo := range e.DBPartitions[db] {
		dbPTInfo.mu.Lock()
		for id, shard := range dbPTInfo.shards {
			if err := shard.Close(); err != nil {
				dbPTInfo.mu.Unlock()
				return err
			}
			delete(dbPTInfo.shards, id)
		}

		for id, iBuild := range dbPTInfo.indexBuilder {
			if err := iBuild.Close(); err != nil {
				return err
			}
			delete(dbPTInfo.indexBuilder, id)
		}
		dbPTInfo.mu.Unlock()
	}
	return nil
}

func (e *Engine) dropDBPt(db string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id := range e.DBPartitions[db] {
		e.dropDBPTInfo(db, id)
	}
}

func (e *Engine) DropRetentionPolicy(db string, rp string, ptId uint32) error {
	rpName := db + "." + rp
	if err := e.startDrop(rpName, e.droppingRP); err != nil {
		return err
	}
	defer e.endDrop(rpName, e.droppingRP)

	atomic.AddInt64(&stat.EngineStat.DropRPCount, 1)
	start := time.Now()
	e.log.Info("start drop retention policy...", zap.String("db", db), zap.String("rp", rp))
	defer func(st time.Time) {
		d := time.Since(st)
		atomic.AddInt64(&stat.EngineStat.DropRPDurations, d.Nanoseconds())
		stat.UpdateEngineStatS()
		e.log.Info("drop retention policy done",
			zap.String("db", db), zap.String("rp", rp), zap.Duration("duration", d))
	}(start)

	e.mu.RLock()
	ptInfo := e.DBPartitions[db]
	if ptInfo == nil {
		e.mu.RUnlock()
		dataPath := path.Join(e.dataPath, DataDirectory, db, strconv.Itoa(int(ptId)), rp)
		walPath := path.Join(e.walPath, WalDirectory, db, strconv.Itoa(int(ptId)), rp)
		if err := deleteDataAndWalPath(dataPath, walPath); err != nil {
			atomic.AddInt64(&stat.EngineStat.DropRPErrs, 1)
			return err
		}
		return nil
	}

	ptIds, err := e.refDBPTsNoLock(ptInfo, db)
	if err != nil {
		atomic.AddInt64(&stat.EngineStat.DropRPErrs, 1)
		e.mu.RUnlock()
		return err
	}
	e.mu.RUnlock()
	defer e.unrefDBPTs(db, ptIds)

	if err := e.walkShards(db, func(dbPTInfo *DBPTInfo, shardID uint64, sh Shard) error {
		if sh.RPName() != rp {
			return nil
		}

		iBuild := sh.GetIndexBuild()
		indexID := iBuild.GetIndexID()
		_, ok := dbPTInfo.indexBuilder[indexID]
		if ok {
			if err := iBuild.Close(); err != nil {
				return err
			}
		}

		if err := sh.Close(); err != nil {
			return err
		}

		dbPTInfo.mu.Lock()
		delete(dbPTInfo.indexBuilder, indexID)
		delete(dbPTInfo.shards, shardID)
		delete(dbPTInfo.newestRpShard, rp)
		dbPTInfo.mu.Unlock()
		return nil
	}); err != nil {
		atomic.AddInt64(&stat.EngineStat.DropRPErrs, 1)
		return err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	for pt := range e.DBPartitions[db] {
		if err = deleteDataAndWalPath(filepath.Join(e.DBPartitions[db][pt].path, rp), filepath.Join(e.DBPartitions[db][pt].walPath, rp)); err != nil {
			atomic.AddInt64(&stat.EngineStat.DropRPErrs, 1)
			return err
		}
	}
	return nil
}

func (e *Engine) walkShards(db string, fn func(dbPTInfo *DBPTInfo, shardID uint64, sh Shard) error) error {
	resC := make(chan error)
	var n int
	for _, dbPTInfo := range e.DBPartitions[db] {
		for shardId := range dbPTInfo.shards {
			n++
			go func(info *DBPTInfo, id uint64, sh Shard) {
				err := fn(info, id, sh)
				if err != nil {
					resC <- fmt.Errorf("shard %d: %s", id, err)
					return
				}
				resC <- err
			}(dbPTInfo, shardId, dbPTInfo.shards[shardId])
		}
	}

	var err error
	for i := 0; i < n; i++ {
		res := <-resC
		if res != nil {
			err = res
		}
	}
	close(resC)
	return err
}

func (e *Engine) DropMeasurement(db string, rp string, name string, shardIds []uint64) error {
	e.log.Info("start delete measurement...", zap.String("db", db), zap.String("name", name))
	start := time.Now()
	atomic.AddInt64(&stat.EngineStat.DropMstCount, 1)
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.DropMstDurations, d.Nanoseconds())
		stat.UpdateEngineStatS()
		e.log.Info("delete measurement done", zap.String("db", db), zap.String("name", name),
			zap.Duration("time used", d))
	}(start)

	mstName := db + "." + rp + "." + name
	if err := e.startDrop(mstName, e.droppingMst); err != nil {
		return err
	}
	defer e.endDrop(mstName, e.droppingMst)

	e.mu.RLock()
	pts, ok := e.DBPartitions[db]
	if !ok || len(pts) == 0 {
		e.mu.RUnlock()
		return nil
	}

	ptIds, err := e.refDBPTsNoLock(pts, db)
	if err != nil {
		atomic.AddInt64(&stat.EngineStat.DropRPErrs, 1)
		e.mu.RUnlock()
		return err
	}
	e.mu.RUnlock()
	defer e.unrefDBPTs(db, ptIds)

	for ptID, pt := range pts {
		pt.mu.RLock()
		// Drop measurement from index.
		for id, iBuild := range pt.indexBuilder {
			if err := iBuild.DropMeasurement([]byte(name)); err != nil {
				pt.mu.RUnlock()
				e.log.Error("drop measurement fail", zap.Uint32("ptid", ptID),
					zap.Uint64("indexId", id), zap.Error(err))
				atomic.AddInt64(&stat.EngineStat.DropMstErrs, 1)
				return err
			}
		}

		// drop measurement from shard
		for _, id := range shardIds {
			sh, ok := pt.shards[id]
			if !ok {
				continue
			}
			if err := sh.DropMeasurement(context.TODO(), name); err != nil {
				e.log.Error("drop measurement fail", zap.Uint32("ptid", ptID),
					zap.Uint64("shard", id), zap.Error(err))
				pt.mu.RUnlock()
				atomic.AddInt64(&stat.EngineStat.DropMstErrs, 1)
				return err
			}
		}
		pt.mu.RUnlock()
	}

	return nil
}

func (e *Engine) SeriesCardinality(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr) ([]meta2.MeasurementCardinalityInfo, error) {
	e.mu.RLock()
	if err := e.checkAndAddRefPTSNoLock(db, ptIDs); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPTs(db, ptIDs)
	pts, ok := e.DBPartitions[db]
	e.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	var measurementCardinalityInfos []meta2.MeasurementCardinalityInfo
	var err error
	for i := range ptIDs {
		pt, ok := pts[ptIDs[i]]
		if !ok {
			continue
		}
		pt.mu.RLock()
		if condition != nil {
			measurementCardinalityInfos, err = pt.seriesCardinalityWithCondition(measurements, condition, measurementCardinalityInfos)
		} else {
			measurementCardinalityInfos, err = pt.seriesCardinality(measurements, measurementCardinalityInfos)
		}

		if err != nil {
			pt.mu.RUnlock()
			return nil, err
		}
		pt.mu.RUnlock()
	}
	return measurementCardinalityInfos, nil
}

func (e *Engine) SeriesExactCardinality(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr) (map[string]uint64, error) {
	e.mu.RLock()
	if err := e.checkAndAddRefPTSNoLock(db, ptIDs); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPTs(db, ptIDs)
	pts, ok := e.DBPartitions[db]
	e.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	keysMap := make(map[string]map[string]struct{}, 64)
	for _, name := range measurements {
		keysMap[string(name)] = make(map[string]struct{}, 64)
	}
	series := make([][]byte, 1)
	seriesLen := 0
	defer func() {
		series = series[:seriesLen]
		for i := range series {
			if len(series[i]) > 0 {
				influx.PutBytesBuffer(series[i])
			}
		}
	}()
	var err error
	for _, ptID := range ptIDs {
		pt, ok := pts[ptID]
		if !ok {
			continue
		}
		pt.mu.RLock()
		for _, iBuild := range pt.indexBuilder {
			for _, name := range measurements {
				stime := time.Now()
				idx := iBuild.GetPrimaryIndex().(*tsi.MergeSetIndex)
				series, err = idx.SearchSeriesKeys(series[:0], name, condition)
				if len(series) > seriesLen {
					seriesLen = len(series)
				}
				log.Info("search series keys", zap.String("name", string(name)),
					zap.Duration("cost", time.Since(stime)))
				if err != nil {
					pt.mu.RUnlock()
					return nil, err
				}
				stime = time.Now()
				for _, key := range series {
					keysMap[string(name)][string(key)] = struct{}{}
				}
				log.Info("remove dupicate key", zap.String("name", string(name)),
					zap.Duration("cost", time.Since(stime)))
			}
		}
		pt.mu.RUnlock()
	}

	// Count all measurement series cardinality
	result := make(map[string]uint64, len(measurements))
	for _, name := range measurements {
		result[string(name)] = uint64(len(keysMap[string(name)]))
	}
	return result, nil
}

func (e *Engine) SeriesKeys(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr) ([]string, error) {
	e.mu.RLock()
	if err := e.checkAndAddRefPTSNoLock(db, ptIDs); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPTs(db, ptIDs)
	pts, ok := e.DBPartitions[db]
	e.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	keyMap := make(map[string]struct{})
	series := make([][]byte, 1)
	seriesLen := 0
	defer func() {
		series = series[:seriesLen]
		for i := range series {
			if len(series[i]) > 0 {
				influx.PutBytesBuffer(series[i])
			}
		}
	}()
	var err error
	for _, ptID := range ptIDs {
		pt, ok := pts[ptID]
		if !ok {
			continue
		}
		pt.mu.RLock()
		for _, iBuild := range pt.indexBuilder {
			for _, name := range measurements {
				idx := iBuild.GetPrimaryIndex().(*tsi.MergeSetIndex)
				series, err = idx.SearchSeriesKeys(series[:0], name, condition)
				if len(series) > seriesLen {
					seriesLen = len(series)
				}
				if err != nil {
					pt.mu.RUnlock()
					return nil, err
				}
				for _, key := range series {
					keyMap[string(key)] = struct{}{}
				}
			}
		}
		pt.mu.RUnlock()
	}

	result := make([]string, 0, len(keyMap))
	for key := range keyMap {
		result = append(result, key)
	}
	sort.Strings(result)

	return result, nil
}

func (e *Engine) TagValuesCardinality(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr) (map[string]uint64, error) {
	e.mu.RLock()
	if err := e.checkAndAddRefPTSNoLock(db, ptIDs); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPTs(db, ptIDs)
	pts, ok := e.DBPartitions[db]
	e.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	tvMap := make(map[string]map[string]struct{}, len(tagKeys))
	for name := range tagKeys {
		tvMap[name] = make(map[string]struct{}, 64)
	}
	for _, ptID := range ptIDs {
		pt, ok := pts[ptID]
		if !ok {
			continue
		}
		pt.mu.RLock()
		for _, iBuild := range pt.indexBuilder {
			for name, tks := range tagKeys {
				idx := iBuild.GetPrimaryIndex().(*tsi.MergeSetIndex)
				values, err := idx.SearchTagValues([]byte(name), tks, condition)
				if err != nil {
					pt.mu.RUnlock()
					return nil, err
				}
				if values == nil {
					// Measurement name not found
					continue
				}
				for _, vs := range values {
					for _, v := range vs {
						tvMap[name][v] = struct{}{}
					}
				}
			}
		}
		pt.mu.RUnlock()
	}

	result := make(map[string]uint64, len(tagKeys))
	for name := range tagKeys {
		result[name] = uint64(len(tvMap[name]))
	}
	return result, nil
}

func (e *Engine) TagValues(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr) (netstorage.TablesTagSets, error) {
	e.mu.RLock()
	if err := e.checkAndAddRefPTSNoLock(db, ptIDs); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPTs(db, ptIDs)
	pts, ok := e.DBPartitions[db]
	e.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	tagValuess := make(netstorage.TablesTagSets, 0)
	results := make(map[string][][]string, len(tagKeys))
	for _, ptID := range ptIDs {
		pt, ok := pts[ptID]
		if !ok {
			continue
		}
		pt.mu.RLock()
		for _, iBuild := range pt.indexBuilder {
			for name, tks := range tagKeys {
				idx := iBuild.GetPrimaryIndex().(*tsi.MergeSetIndex)
				values, err := idx.SearchTagValues([]byte(name), tks, condition)
				if err != nil {
					pt.mu.RUnlock()
					return nil, err
				}
				if values == nil {
					// Measurement name not found
					continue
				}

				appendValuesToMap(results, name, values)
			}
		}
		pt.mu.RUnlock()
	}

	// transform to tagvaluess
	for name, tvs := range results {
		tv := netstorage.TableTagSets{
			Name:   name,
			Values: make(netstorage.TagSets, 0, len(tvs[0])),
		}
		for i, tk := range tagKeys[name] {
			for _, v := range tvs[i] {
				tv.Values = append(tv.Values, netstorage.TagSet{Key: record.Bytes2str(tk), Value: v})
			}
		}
		tagValuess = append(tagValuess, tv)
	}

	return tagValuess, nil
}

func appendValuesToMap(results map[string][][]string, name string, values [][]string) {
	if _, ok := results[name]; !ok {
		results[name] = make([][]string, len(values))
	}
	for i := 0; i < len(values); i++ {
		results[name][i] = strings.UnionSlice(append(results[name][i], values[i]...))
	}
}

func (e *Engine) DropSeries(database string, sources []influxql.Source, ptId []uint32, condition influxql.Expr) (int, error) {
	panic("implement me")
}

func (e *Engine) DbPTRef(db string, ptId uint32) error {
	var err error
	e.mu.RLock()
	err = e.checkAndAddRefPTNoLock(db, ptId)
	e.mu.RUnlock()
	return err
}

func (e *Engine) DbPTUnref(db string, ptId uint32) {
	e.mu.RLock()
	e.unrefDBPTNoLock(db, ptId)
	e.mu.RUnlock()
}

func (e *Engine) CreateLogicalPlan(ctx context.Context, db string, ptId uint32, shardID uint64,
	sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error) {
	e.mu.RLock()
	dbPTInfo := e.DBPartitions[db][ptId]
	dbPTInfo.mu.RLock()
	sh, ok := dbPTInfo.shards[shardID]
	if !ok {
		dbPTInfo.mu.RUnlock()
		e.mu.RUnlock()
		return nil, nil
	}
	dbPTInfo.mu.RUnlock()
	e.mu.RUnlock()

	// FIXME:context cancel func
	return sh.CreateLogicalPlan(ctx, sources, schema)
}

func (e *Engine) LogicalPlanCost(db string, ptId uint32, sources influxql.Sources, opt query.ProcessorOptions) (hybridqp.LogicalPlanCost, error) {
	panic("implement me")
}

func (e *Engine) checkAndAddRefPTSNoLock(database string, ptIDs []uint32) error {
	delRefPtId := make([]uint32, 0, len(ptIDs))
	var err error
	for _, ptId := range ptIDs {
		if err = e.checkAndAddRefPTNoLock(database, ptId); err != nil {
			break
		} else {
			delRefPtId = append(delRefPtId, ptId)
		}
	}

	if err != nil {
		e.unrefDBPTSNoLock(database, delRefPtId)
	}
	return err
}

func (e *Engine) checkAndAddRefPTNoLock(database string, ptID uint32) error {
	if dbPT, dbExist := e.DBPartitions[database]; dbExist {
		if _, ok := dbPT[ptID]; ok {
			if suc := e.DBPartitions[database][ptID].ref(); suc {
				return nil
			} else {
				return meta2.ErrDBPTClose
			}
		} else {
			return ErrPTNotFound
		}
	} else {
		return ErrPTNotFound
	}
}

func (e *Engine) unrefDBPT(database string, ptID uint32) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	e.unrefDBPTNoLock(database, ptID)
}

func (e *Engine) unrefDBPTs(database string, ptIDs []uint32) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, ptID := range ptIDs {
		e.unrefDBPTNoLock(database, ptID)
	}
}

func (e *Engine) refDBPTsNoLock(ptInfo map[uint32]*DBPTInfo, db string) ([]uint32, error) {
	ptIds := make([]uint32, len(ptInfo))
	i := 0
	for id := range ptInfo {
		ptIds[i] = id
		i++
	}

	if err := e.checkAndAddRefPTSNoLock(db, ptIds); err != nil {
		return nil, err
	}

	return ptIds, nil
}

func (e *Engine) unrefDBPTSNoLock(database string, ptIDs []uint32) {
	for _, ptID := range ptIDs {
		e.unrefDBPTNoLock(database, ptID)
	}
}

func (e *Engine) unrefDBPTNoLock(database string, ptID uint32) {
	dbPT, dbExist := e.DBPartitions[database]
	if !dbExist {
		return
	}
	_, ok := dbPT[ptID]
	if !ok {
		log.Error("pt not found", zap.String("database", database), zap.Uint32("pt", ptID))
		panic("pt not found")
	}
	dbPT[ptID].unref()
}

func (e *Engine) SysCtrl(req *netstorage.SysCtrlRequest) error {
	return e.processReq(req)
}

func (e *Engine) Statistics(buffer []byte) ([]byte, error) {
	e.statCount++
	if stat.FileStatisticsLimited(e.statCount) {
		return nil, nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed.Closed() {
		return buffer, nil
	}

	for db, partitions := range e.DBPartitions {
		for id := range partitions {
			dbPTInfo := e.DBPartitions[db][id]
			dbPTInfo.mu.RLock()
			for _, sh := range dbPTInfo.shards {
				buffer, _ = sh.Statistics(buffer)
			}
			dbPTInfo.mu.RUnlock()
		}
	}
	return buffer, nil
}
