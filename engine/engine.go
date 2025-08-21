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

package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	msgservice_data "github.com/openGemini/openGemini/lib/msgservice/data"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/raftlog"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

var log *logger.Logger
var openShardsLimit limiter.Fixed
var replayWalLimit limiter.Fixed

func init() {
	log = logger.NewLogger(errno.ModuleStorageEngine)
	RegisterNewEngineFun(config.EngineType1, NewEngine)
}

const MaxFileInfoSize = 1024

const DefaultUploadFrequence = 500 * time.Millisecond

type EngineImpl struct {
	mu       sync.RWMutex // read/write lock for Engine
	closed   *interruptsignal.InterruptSignal
	dataPath string
	walPath  string

	engOpt       EngineOptions
	DBPartitions map[string]map[uint32]*DBPTInfo

	log *logger.Logger

	loadCtx     *meta.LoadCtx
	dropMu      sync.RWMutex
	droppingDB  map[string]string
	droppingRP  map[string]string
	droppingMst map[string]string

	DownSamplePolicies map[string]*meta2.StoreDownSamplePolicy

	statCount int64

	mgtLock       sync.RWMutex // lock for migration
	migratingDbPT map[string]map[uint32]struct{}
	metaClient    meta.MetaClient
	fileInfos     chan []immutable.FileInfoExtend
	backup        *Backup

	onPTOffload map[uint64]func(ptID uint32)

	clearShardMapLock    sync.RWMutex
	clearRepColdShardMap map[string]struct{}
	clearIndexMapLock    sync.RWMutex
	clearRepColdIndexMap map[string]struct{}
}

func NewEngine(dataPath, walPath string, options EngineOptions, ctx *meta.LoadCtx) (Engine, error) {
	eng := &EngineImpl{
		closed:               interruptsignal.NewInterruptSignal(),
		dataPath:             dataPath,
		walPath:              walPath,
		engOpt:               options,
		DBPartitions:         make(map[string]map[uint32]*DBPTInfo, 64),
		loadCtx:              ctx,
		log:                  logger.NewLogger(errno.ModuleStorageEngine),
		droppingDB:           make(map[string]string),
		droppingRP:           make(map[string]string),
		droppingMst:          make(map[string]string),
		migratingDbPT:        make(map[string]map[uint32]struct{}),
		fileInfos:            nil,
		onPTOffload:          make(map[uint64]func(ptID uint32)),
		clearRepColdShardMap: make(map[string]struct{}),
		clearRepColdIndexMap: make(map[string]struct{}),
	}

	eng.DownSamplePolicies = make(map[string]*meta2.StoreDownSamplePolicy)
	openShardsLimit = limiter.NewFixed(options.OpenShardLimit)
	replayWalLimit = limiter.NewFixed(options.OpenShardLimit)

	SetFullCompColdDuration(options.FullCompactColdDuration)
	fileops.EnableMmapRead(options.EnableMmapRead)
	fileops.SetPageSize(options.ReadPageSize)
	fileops.SetMetaPageList(options.ReadMetaPageSize)
	fileops.EnableReadMetaCache(options.ReadMetaCacheLimit)
	fileops.EnableReadDataCache(options.ReadDataCacheLimit)
	immutable.SetMaxCompactor(options.MaxConcurrentCompactions)
	immutable.SetMaxFullCompactor(options.MaxFullCompactions)
	immutable.SetCompactLimit(options.CompactThroughput, options.CompactThroughputBurst)
	immutable.SetSnapshotLimit(options.SnapshotThroughput, options.SnapshotThroughputBurst)
	fileops.SetBackgroundReadLimiter(options.BackgroundReadThroughput)
	immutable.SetMergeFlag4TsStore(int32(options.CompactionMethod))
	immutable.SetSnapshotTblNum(options.SnapshotTblNum)
	immutable.SetCompactionEnabled(options.CsCompactionEnabled)
	immutable.SetDetachedFlushEnabled(options.CsDetachedFlushEnabled)
	immutable.SetFragmentsNumPerFlush(options.FragmentsNumPerFlush)
	immutable.SetMaxRowsPerSegment4TsStore(options.MaxRowsPerSegment)
	obs.SetPrefixDataPath(dataPath)
	immutable.Init()

	return eng, nil
}

func (e *EngineImpl) GetLockFile() string {
	return ""
}

func (e *EngineImpl) Open(durationInfos map[uint64]*meta2.ShardDurationInfo, dbBriefInfos map[string]*meta2.DatabaseBriefInfo, m meta.MetaClient) error {
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

	e.SetMetaClient(m)
	err := e.loadShards(durationInfos, dbBriefInfos, immutable.LOAD, m)
	if err != nil {
		atomic.AddInt64(&stat.EngineStat.OpenErrors, 1)
		return err
	}

	return nil
}

func (e *EngineImpl) SetMetaClient(m meta.MetaClient) {
	e.mu.Lock()
	e.metaClient = m
	e.mu.Unlock()
}

func (e *EngineImpl) ClearRepCold(req *msgservice.SendClearEventsRequest) error {
	pair := req.GetPair()
	indexInfo := pair.IndexInfo
	var wg sync.WaitGroup
	errCh := make(chan error)
	if indexInfo != nil {
		wg.Add(1)
		go func(indexInfo *msgservice_data.IndexPair) {
			err := e.ClearIndexRepCold(req.GetDatabase(), req.GetRp(), req.GetPtId(), indexInfo)
			wg.Done()
			if err != nil {
				errCh <- err
				e.log.Error("clear shard rep cold failed", zap.String("db", req.GetDatabase()),
					zap.Uint32("pt", req.GetPtId()), zap.Uint64("indexId", indexInfo.GetToClearIndex()), zap.Error(err))
			}
		}(indexInfo)
	}

	shardPairs := pair.GetShardInfo()
	if len(shardPairs) > 0 {
		for i := range shardPairs {
			wg.Add(1)
			go func(shardPair *msgservice_data.ShardPair) {
				err := e.ClearShardRepCold(req.GetDatabase(), req.GetRp(), req.GetPtId(), shardPair)
				wg.Done()
				if err != nil {
					errCh <- err
					e.log.Error("clear shard rep cold failed", zap.String("db", req.GetDatabase()),
						zap.Uint32("pt", req.GetPtId()), zap.Uint64("shardId", shardPair.GetToClearShard()), zap.Error(err))
				}
			}(shardPairs[i])
		}
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()
	var errs []error
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		er := fmt.Errorf("error clear rep cold %v", errs[0])
		for i, err := range errs {
			if i == 0 {
				continue
			}
			er = errors.Join(er, err)
		}
		return er
	}
	return nil
}

func (e *EngineImpl) uploadFileInfos() {
	fileInfo := make([]meta2.FileInfo, 0, MaxFileInfoSize)
	ticker := time.NewTicker(DefaultUploadFrequence)
	defer ticker.Stop()

uploadChannel:
	for {
		select {
		case fileInfoExtend := <-e.fileInfos:
			currentFileNum := len(fileInfo)
			incomingFileNum := len(fileInfoExtend)
			if currentFileNum+incomingFileNum > MaxFileInfoSize {
				fileInfo = append(make([]meta2.FileInfo, 0, currentFileNum+incomingFileNum), fileInfo...)
			}
			db, rp, _ := e.metaClient.ShardOwner(fileInfoExtend[0].FileInfo.ShardID)
			mstID, err := e.metaClient.GetMeasurementID(db, rp, influx.GetOriginMstName(fileInfoExtend[0].Name))
			if err != nil {
				e.log.Error("measurement ID not found")
				continue
			}
			for _, fi := range fileInfoExtend {
				fileInfo = append(fileInfo, fi.FileInfo)
				fileInfo[len(fileInfo)-1].MstID = mstID
			}
			if len(fileInfo) > MaxFileInfoSize {
				err := e.metaClient.InsertFiles(fileInfo)
				if err != nil {
					e.log.Error("InsertFiles failed.")
					continue // try later if failed
				}
				fileInfo = fileInfo[:0]
			}
		case <-ticker.C:
			if len(fileInfo) != 0 {
				err := e.metaClient.InsertFiles(fileInfo)
				if err != nil {
					e.log.Error("InsertFiles failed.")
					continue // try later if failed
				}
				fileInfo = fileInfo[:0]
			}
			if e.closed.Closed() {
				break uploadChannel
			}
		}
	}
}

func (e *EngineImpl) loadShards(durationInfos map[uint64]*meta2.ShardDurationInfo, dbBriefInfos map[string]*meta2.DatabaseBriefInfo, loadStat int, client meta.MetaClient) error {
	e.log.Info("start loadShards...", zap.Int("loadStat", loadStat))
	dataPath := path.Join(e.dataPath, config.DataDirectory)
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
		go func(db string, pt uint32, rp string, engineType config.EngineType) {
			enableTagArray := false
			if len(dbBriefInfos) != 0 {
				if _, ok := dbBriefInfos[db]; !ok {
					e.log.Error("fail to get dbBriefInfos", zap.String("db", db), zap.Uint32("pt", pt),
						zap.String("rp", rp))
					errChan <- errno.NewError(errno.DatabaseNotFound)
					return
				}
				enableTagArray = dbBriefInfos[db].EnableTagArray
			}
			e.createDBPTIfNotExist(db, pt, enableTagArray)
			e.mu.RLock()
			dbPTInfo := e.DBPartitions[db][pt]
			e.mu.RUnlock()
			err := dbPTInfo.loadShards(0, rp, durationInfos, loadStat, client, engineType)
			if err != nil {
				e.log.Error("fail to load db rp", zap.String("db", db), zap.Uint32("pt", pt),
					zap.String("rp", rp), zap.Error(err))
			}
			errChan <- err
		}(sdi.Ident.OwnerDb, sdi.Ident.OwnerPt, sdi.Ident.Policy, config.EngineType(sdi.Ident.EngineType))
	}

	for i := 0; i < n; i++ {
		err := <-errChan
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *EngineImpl) Close() error {
	e.mu.Lock()

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
	e.mu.Unlock()

	for db := range e.DBPartitions {
		e.dropDBPt(db)
	}
	return nil
}

type ShardStatus struct {
	ShardId  uint64
	Opened   bool
	ReadOnly bool
}

// MarshalText keeps marshaled dict items order
func (s ShardStatus) MarshalText() (data []byte, err error) {
	ctx := fmt.Sprintf("{ShardId: %d, Opened: %t, ReadOnly: %t}", s.ShardId, s.Opened, s.ReadOnly)
	return []byte(ctx), nil
}

func (e *EngineImpl) getShardStatus(param map[string]string) (map[string]string, error) {
	var dbName, rpName string
	if db, ok := param["db"]; ok {
		dbName = db
	}
	if rp, ok := param["rp"]; ok {
		rpName = rp
	}

	var ptId uint32 = math.MaxUint32
	if id, ok := param["pt"]; ok {
		if n, err := strconv.Atoi(id); err == nil {
			ptId = uint32(n)
		}
	}
	var shardId uint64 = math.MaxUint64
	if id, ok := param["shard"]; ok {
		if n, err := strconv.Atoi(id); err == nil {
			shardId = uint64(n)
		}
	}
	e.log.Info("query shard status", zap.String("db", dbName), zap.String("rp", rpName), zap.Uint32("pt", ptId), zap.Uint64("shard", shardId))

	resp := make(map[string][]ShardStatus)

	e.mu.RLock()
	defer e.mu.RUnlock()
	for db, partitions := range e.DBPartitions {
		if dbName != "" && dbName != db {
			continue
		}
		for pt, dbptInfo := range partitions {
			if ptId != math.MaxUint32 && ptId != pt {
				continue
			}
			dbptInfo.mu.RLock()
			for sid, shd := range dbptInfo.shards {
				if shardId != math.MaxUint64 && shardId != sid {
					continue
				}
				if rpName != "" && rpName != shd.GetRPName() {
					continue
				}

				key := fmt.Sprintf("db: %s, rp: %s, pt: %d", db, shd.GetRPName(), pt)
				value := ShardStatus{
					ShardId:  sid,
					Opened:   shd.IsOpened(),
					ReadOnly: shd.GetIdent().ReadOnly,
				}
				resp[key] = append(resp[key], value)
			}
			dbptInfo.mu.RUnlock()
		}
	}

	var result = make(map[string]string)
	for k, v := range resp {
		val, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		result[k] = string(val)
	}
	return result, nil
}

func (e *EngineImpl) ForceFlush() {
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

func (e *EngineImpl) UpdateIndexDurationInfo(info *meta2.IndexDurationInfo, nilIndexMap *map[uint64]*meta2.IndexDurationInfo) error {
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
	index := dbPT.indexBuilder[info.Ident.IndexID]
	if index == nil {
		(*nilIndexMap)[info.Ident.IndexID] = info
		log.Info("nil index duration info", zap.Uint64("indexId", info.Ident.IndexID),
			zap.Uint64("index group id", info.Ident.IndexGroupID),
			zap.Duration("duration", info.DurationInfo.Duration),
			zap.Duration("mergeDuration", info.DurationInfo.MergeDuration))
		return nil
	}
	log.Info("duration info", zap.Uint64("indexId", info.Ident.IndexID),
		zap.Uint64("index group id", info.Ident.IndexGroupID),
		zap.Duration("duration", info.DurationInfo.Duration), zap.Duration("merge Duration", info.DurationInfo.MergeDuration))
	index.Ident().Index.IndexGroupID = info.Ident.IndexGroupID
	index.SetDuration(info.DurationInfo.Duration)
	index.SetMergeDuration(info.DurationInfo.MergeDuration)
	return nil
}

func (e *EngineImpl) UpdateShardDurationInfo(info *meta2.ShardDurationInfo, nilShardMap *map[uint64]*meta2.ShardDurationInfo) error {
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
	if shard == nil || shard.GetIndexBuilder() == nil {
		(*nilShardMap)[info.Ident.ShardID] = info
		log.Info("nil shard duration info", zap.Uint64("shardId", info.Ident.ShardID),
			zap.Uint64("shard group id", info.Ident.ShardGroupID),
			zap.Duration("duration", info.DurationInfo.Duration), zap.Duration("merge Duration", info.DurationInfo.MergeDuration))
		return nil
	}
	log.Info("duration info", zap.Uint64("shardId", info.Ident.ShardID),
		zap.Uint64("shard group id", info.Ident.ShardGroupID),
		zap.Duration("duration", info.DurationInfo.Duration), zap.Duration("merge duration", info.DurationInfo.MergeDuration))
	shard.GetIdent().ShardGroupID = info.Ident.ShardGroupID
	shard.GetDuration().Duration = info.DurationInfo.Duration
	shard.GetDuration().Tier = info.DurationInfo.Tier
	shard.GetDuration().TierDuration = info.DurationInfo.TierDuration
	shard.GetDuration().MergeDuration = info.DurationInfo.MergeDuration
	shard.GetIndexBuilder().SetDuration(info.DurationInfo.Duration)
	shard.GetIndexBuilder().SetMergeDuration(info.DurationInfo.MergeDuration)
	return nil
}

func (e *EngineImpl) containSid(shards []*meta2.ShardIdentifier, sid uint64) bool {
	for _, s := range shards {
		if s.ShardID == sid {
			return true
		}
	}
	return false
}

func (e *EngineImpl) containIdxid(indexes []*meta2.IndexIdentifier, idxId uint64) bool {
	for _, s := range indexes {
		if s.Index.IndexID == idxId {
			return true
		}
	}
	return false
}

func (e *EngineImpl) nilShardIsExpired(duration time.Duration, endTime time.Time) bool {
	now := time.Now().UTC()

	if duration != 0 && endTime.Add(duration).Before(now) {
		return true
	}
	return false
}

func (e *EngineImpl) ExpiredShards(nilShardMap *map[uint64]*meta2.ShardDurationInfo) []*meta2.ShardIdentifier {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var res []*meta2.ShardIdentifier
	for db := range e.DBPartitions {
		for _, pti := range e.DBPartitions[db] {
			pti.mu.RLock()
			for sid := range pti.shards {
				if pti.shards[sid].IsExpired() {
					res = append(res, pti.shards[sid].GetIdent())
				}
			}
			for sid, info := range *nilShardMap {
				if e.containSid(res, sid) {
					continue
				}
				if e.nilShardIsExpired(info.DurationInfo.Duration, info.Ident.EndTime) {
					res = append(res, &info.Ident)
				}
			}
			pti.mu.RUnlock()
		}
	}
	return res
}

func (e *EngineImpl) ExpiredShardsForMst(db, rp string, mst *meta2.MeasurementTTLTnfo) []*meta2.ShardIdentifier {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var res []*meta2.ShardIdentifier

	for _, pti := range e.DBPartitions[db] {
		pti.mu.RLock()
		for _, sh := range pti.shards {
			if rp == sh.GetRPName() && e.nilShardIsExpired(time.Duration(mst.TTL), sh.GetEndTime()) {
				res = append(res, sh.GetIdent())
			}
		}
		pti.mu.RUnlock()
	}
	return res
}

func (e *EngineImpl) ExpiredIndexes(nilIndexMap *map[uint64]*meta2.IndexDurationInfo) []*meta2.IndexIdentifier {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var res []*meta2.IndexIdentifier
	for db := range e.DBPartitions {
		for _, pti := range e.DBPartitions[db] {
			pti.mu.RLock()
			for idxId := range e.DBPartitions[db][pti.id].indexBuilder {
				if e.DBPartitions[db][pti.id].indexBuilder[idxId].Expired() {
					res = append(res, e.DBPartitions[db][pti.id].indexBuilder[idxId].Ident())
				}
			}
			for idxId, info := range *nilIndexMap {
				if e.containIdxid(res, idxId) {
					continue
				}
				if e.nilShardIsExpired(info.DurationInfo.Duration, info.Ident.EndTime) {
					index := meta2.IndexDescriptor{
						IndexID:      info.Ident.IndexID,
						IndexGroupID: info.Ident.IndexGroupID,
						TimeRange: meta2.TimeRangeInfo{
							StartTime: info.Ident.StartTime,
							EndTime:   info.Ident.EndTime,
						},
					}
					res = append(res, &meta2.IndexIdentifier{
						OwnerDb: info.Ident.OwnerDb,
						OwnerPt: info.Ident.OwnerPt,
						Policy:  info.Ident.Policy,
						Index:   &index,
					})
				}
			}
			pti.mu.RUnlock()
		}
	}
	return res
}

func (e *EngineImpl) ExpiredIndexesForMst(db, rp string, mst *meta2.MeasurementTTLTnfo) []*meta2.IndexIdentifier {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var res []*meta2.IndexIdentifier

	for _, pti := range e.DBPartitions[db] {
		pti.mu.RLock()
		for idxId := range e.DBPartitions[db][pti.id].indexBuilder {
			if rp == e.DBPartitions[db][pti.id].indexBuilder[idxId].RPName() &&
				e.nilShardIsExpired(time.Duration(mst.TTL), e.DBPartitions[db][pti.id].indexBuilder[idxId].GetEndTime()) {
				res = append(res, e.DBPartitions[db][pti.id].indexBuilder[idxId].Ident())
			}
		}
		pti.mu.RUnlock()
	}

	return res
}

func (e *EngineImpl) DeleteMstInShard(db string, ptId uint32, shardID uint64, mst string) error {
	e.log.Info("start delete mst...", zap.String("db", db), zap.Uint64("shardID", shardID), zap.String("mst", mst))
	start := time.Now()
	atomic.AddInt64(&stat.EngineStat.DelShardCount, 1)
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.DelShardDuration, d.Nanoseconds())
		stat.UpdateEngineStatS()
		e.log.Info("delete mst done", zap.String("db", db), zap.Uint64("shardID", shardID), zap.String("mst", mst),
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
	if !dbPtInfo.bgrEnabled {
		dbPtInfo.mu.Unlock()
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	sh, ok := dbPtInfo.shards[shardID]
	if !ok {
		dbPtInfo.mu.Unlock()
		return errno.NewError(errno.ShardNotFound, shardID)
	}
	dbPtInfo.mu.Unlock()

	if err := sh.DropMeasurement(context.Background(), mst); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}

	return nil
}

// todo:need confirm
func (e *EngineImpl) DeleteShard(db string, ptId uint32, shardID uint64) error {
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
	if !dbPtInfo.bgrEnabled {
		dbPtInfo.mu.Unlock()
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	sh, ok := dbPtInfo.shards[shardID]
	if !ok {
		dbPtInfo.mu.Unlock()
		return errno.NewError(errno.ShardNotFound, shardID)
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

	lock := fileops.FileLockOption(*dbPtInfo.lockPath)
	// remove shard's wal&data on-disk, index data will not delete right now, if obs-option is not empty, then should remove remote shard dir
	obsOption := sh.GetObsOption()
	if obsOption != nil {
		if err := fileops.RemoveAll(fileops.GetRemoteDataPath(obsOption, sh.GetDataPath()), lock); err != nil {
			atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
			return err
		}
	}
	if err := fileops.RemoveAll(sh.GetDataPath(), lock); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}
	if err := fileops.RemoveAll(sh.GetWalPath(), lock); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelShardErr, 1)
		return err
	}

	return nil
}

func (e *EngineImpl) DeleteMstInIndex(db string, ptId uint32, indexID uint64, msts []string, onlyUseDiskThreshold uint64) error {
	e.log.Info("start delete msts in index...", zap.String("db", db), zap.Uint32("pt", ptId),
		zap.Uint64("indexID", indexID), zap.Strings("msts", msts))
	start := time.Now()
	atomic.AddInt64(&stat.EngineStat.DelIndexCount, 1)
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.DelIndexDuration, d.Nanoseconds())
		stat.UpdateEngineStatS()
		e.log.Info("delete msts in index done", zap.String("db", db), zap.Uint64("indexID", indexID),
			zap.Strings("msts", msts), zap.Duration("time used", d))
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

	dbPtInfo.mu.RLock()
	iBuild, ok := dbPtInfo.indexBuilder[indexID]
	if !ok {
		dbPtInfo.mu.RUnlock()
		return errno.NewError(errno.IndexNotFound, db, ptId, indexID)
	}
	dbPtInfo.mu.RUnlock()

	if err := iBuild.DeleteMsts(msts, onlyUseDiskThreshold); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelIndexErr, 1)
		return err
	}

	return nil
}

func (e *EngineImpl) DeleteIndex(db string, ptId uint32, indexID uint64) error {
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
		return errno.NewError(errno.IndexNotFound, db, ptId, indexID)
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

	lock := fileops.FileLockOption(*dbPtInfo.lockPath)
	if err := fileops.RemoveAll(iBuild.Path(), lock); err != nil {
		atomic.AddInt64(&stat.EngineStat.DelIndexErr, 1)
		return err
	}

	iBuild = nil

	return nil
}

func (e *EngineImpl) ExpiredCacheIndexes() []*meta2.IndexIdentifier {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var res []*meta2.IndexIdentifier
	for db := range e.DBPartitions {
		for _, pti := range e.DBPartitions[db] {
			pti.mu.RLock()
			for idxId := range e.DBPartitions[db][pti.id].indexBuilder {
				if e.DBPartitions[db][pti.id].indexBuilder[idxId].ExpiredCache() {
					res = append(res, e.DBPartitions[db][pti.id].indexBuilder[idxId].Ident())
				}
			}
			pti.mu.RUnlock()
		}
	}
	return res
}

func (e *EngineImpl) ClearIndexCache(db string, ptId uint32, indexID uint64) error {
	e.log.Info("start clear index cache...", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("indexID", indexID))

	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		return err
	}
	dbPtInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	defer e.unrefDBPT(db, ptId)

	dbPtInfo.mu.RLock()
	iBuild, ok := dbPtInfo.indexBuilder[indexID]
	if !ok {
		dbPtInfo.mu.RUnlock()
		return errno.NewError(errno.IndexNotFound, db, ptId, indexID)
	}

	dbPtInfo.mu.RUnlock()

	if err := iBuild.ClearCache(); err != nil {
		return err
	}

	e.log.Info("clear index cache success", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("indexID", indexID))
	return nil
}

func (e *EngineImpl) FetchShardsNeedChangeStore() (shardsToWarm, shardsToCold []*meta2.ShardIdentifier) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var latestShardID uint64

	for db := range e.DBPartitions {
		for pt := range e.DBPartitions[db] {
			e.DBPartitions[db][pt].mu.RLock()
			if config.GetStoreConfig().EnableWriteHistoryOrderedData {
				latestShardID = e.getLatestShard(e.DBPartitions[db][pt].shards)
			}
			for id, shard := range e.DBPartitions[db][pt].shards {
				tier := shard.GetTier()
				expired := shard.IsTierExpired()
				if !expired || tier == util.Cold || (config.GetStoreConfig().EnableWriteHistoryOrderedData && id == latestShardID) {
					continue
				}
				if tier == util.Hot {
					shardsToWarm = append(shardsToWarm, shard.GetIdent())
				} else {
					shardsToCold = append(shardsToCold, shard.GetIdent())
				}
			}
			e.DBPartitions[db][pt].mu.RUnlock()
		}
	}
	return shardsToWarm, shardsToCold
}

func (e *EngineImpl) FetchIndexesNeedChangeStore() (indexesToCold []*meta2.IndexIdentifier) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for db := range e.DBPartitions {
		for pt := range e.DBPartitions[db] {
			e.DBPartitions[db][pt].mu.RLock()
			for _, index := range e.DBPartitions[db][pt].indexBuilder {
				tier := index.GetTier()
				expired := index.IsTierExpired()
				if !expired || tier == util.Cold {
					continue
				}
				if tier == util.TierBegin || tier == util.Hot {
					indexesToCold = append(indexesToCold, index.Ident())
				}
			}
			e.DBPartitions[db][pt].mu.RUnlock()
		}
	}
	return indexesToCold
}

func (e *EngineImpl) getLatestShard(shards map[uint64]Shard) uint64 {
	minEndTime := time.Time{}
	var latestShardID uint64
	for id, shard := range shards {
		if shard.GetEndTime().After(minEndTime) {
			minEndTime = shard.GetEndTime()
			latestShardID = id
		}
	}
	return latestShardID
}

func (e *EngineImpl) ChangeShardTierToWarm(db string, ptId uint32, shardID uint64) error {
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
		return errno.NewError(errno.ShardNotFound, shardID)
	}

	if _, ok = dbPtInfo.pendingShardTiering[shardID]; ok {
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

func (e *EngineImpl) openShardLazy(sh Shard) error {
	if sh.IsOpened() {
		return nil
	}

	start := time.Now()
	e.log.Info("lazy shard open start", zap.String("path", sh.GetDataPath()), zap.Uint32("pt", sh.GetIdent().OwnerPt))
	if err := sh.OpenAndEnable(e.metaClient); err != nil {
		e.log.Error("lazy shard open error", zap.Error(err), zap.Duration("duration", time.Since(start)))
		return err
	}
	e.log.Info("lazy shard open end", zap.Duration("duration", time.Since(start)))
	return nil
}

func (e *EngineImpl) OpenShardLazy(sh Shard) error {
	return e.openShardLazy(sh)
}

// getShard return Shard for write api
func (e *EngineImpl) getShard(db string, ptId uint32, shardID uint64) (Shard, error) {
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPT(db, ptId)

	dbPTInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	sh := dbPTInfo.Shard(shardID)
	if sh == nil {
		return nil, errno.NewError(errno.ShardNotFound, shardID)
	}

	if err := e.openShardLazy(sh); err != nil {
		return nil, err
	}
	return sh, nil
}

// getShard return Shard for write api
func (e *EngineImpl) GetShardAndDbPt(db string, ptId uint32, shardID uint64) (Shard, *DBPTInfo, error) {
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		return nil, nil, err
	}
	defer e.unrefDBPT(db, ptId)

	dbPTInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	sh := dbPTInfo.Shard(shardID)
	if sh == nil {
		return nil, nil, errno.NewError(errno.ShardNotFound, shardID)
	}

	if err := e.openShardLazy(sh); err != nil {
		return nil, nil, err
	}
	return sh, dbPTInfo, nil
}

// checkAndGetDBPTInfo returns DBPTInfo for replication write api
func (e *EngineImpl) checkAndGetDBPTInfo(db string, ptId uint32) (*DBPTInfo, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		return nil, err
	}
	defer e.unrefDBPT(db, ptId)

	dbPTInfo := e.DBPartitions[db][ptId]
	return dbPTInfo, nil
}

func (e *EngineImpl) WriteToRaft(db, rp string, ptId uint32, tail []byte) error {
	defer func() {
		if e := recover(); e != nil {
			log.Error("runtime panic", zap.String("writeToRaft raise stack:", string(debug.Stack())),
				zap.Error(errno.NewError(errno.RecoverPanic, e)),
				zap.String("db", db),
				zap.Uint32("ptId", ptId))
		}
	}()
	dbpt, err := e.checkAndGetDBPTInfo(db, ptId)
	if err != nil {
		return err
	}
	// 1.build dataWrapper
	newTail := make([]byte, 0, len(tail))
	newTail = append(newTail, tail...)
	wrapper := &raftlog.DataWrapper{
		DataType:  raftlog.Normal,
		Data:      newTail,
		Identity:  dbpt.node.GetIdentity(),
		ProposeId: dbpt.node.GenerateProposeId(),
	}
	marshal := wrapper.Marshal()

	// 2.add committedDataC
	var c chan error
	c, err = dbpt.node.AddCommittedDataC(wrapper)
	if err != nil {
		logger.GetLogger().Error("raftNode AddCommitedDataC err", zap.Error(err))
		return err
	}
	defer dbpt.node.RemoveCommittedDataC(wrapper)

	// 3.propose
	dbpt.proposeC <- marshal

	// 4.wait committed return
	timeT := time.After(config.WaitCommitTimeout)
	select {
	case commitedErr := <-c:
		if commitedErr != nil {
			logger.GetLogger().Error("raftNode commitedErr err", zap.Error(commitedErr))
		}
		return commitedErr
	case <-timeT:
		return errno.NewError(errno.WriteToRaftTimeoutAfterPropose, wrapper.Identity, wrapper.ProposeId)
	}
}

func (e *EngineImpl) WriteRows(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte, snp *raftlog.SnapShotter) error {
	sh, err := e.getShard(db, ptId, shardID)
	if err != nil {
		return err
	}
	if snp != nil {
		sh.SetSnapShotter(snp)
	}

	if sh.GetEngineType() == config.COLUMNSTORE {
		err = e.updateColumnStoreMstInfoByRows(db, rp, rows)
		if err != nil {
			return err
		}
	}

	return sh.WriteRows(rows, binaryRows)
}

func (e *EngineImpl) updateColumnStoreMstInfoByRows(db, rp string, rows []influx.Row) error {
	ident := colstore.NewMeasurementIdent(db, rp)
	for i := range rows {
		mstName := rows[i].Name
		if mstName == ident.Name {
			continue
		}

		ident.SetName(mstName)
		err := e.updateColumnStoreMstInfo(ident)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *EngineImpl) updateColumnStoreMstInfo(ident colstore.MeasurementIdent) error {
	mm := colstore.MstManagerIns()
	if !mm.Exists(ident) {
		ident.SetSafeName(ident.Name)
		mi, err := e.metaClient.Measurement(ident.DB, ident.RP, influx.GetOriginMstName(ident.Name))
		if err != nil {
			return err
		}
		mm.Add(ident, mi)
	}
	return nil
}

// WriteRec only works for column store
func (e *EngineImpl) WriteRec(db, mst string, ptId uint32, shardID uint64, rec *record.Record, binaryRec []byte) error {
	sh, err := e.getShard(db, ptId, shardID)
	if err != nil {
		return err
	}

	ident := colstore.NewMeasurementIdent(db, sh.GetIdent().Policy)
	ident.SetName(mst)
	err = e.updateColumnStoreMstInfo(ident)
	if err != nil {
		return err
	}

	return sh.WriteCols(mst, rec, binaryRec)
}

func (e *EngineImpl) CreateShard(db, rp string, ptId uint32, shardID uint64, timeRangeInfo *meta2.ShardTimeRangeInfo, mstInfo *meta2.MeasurementInfo) error {
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
		if dbPTInfo.HasCoverShard(timeRangeInfo, rp, shardID) {
			e.log.Error("create a merged shard", zap.String("db", db), zap.String("rp", rp), zap.Uint32("ptId", ptId),
				zap.Uint64("shardId", shardID), zap.Time("startTime", timeRangeInfo.TimeRange.StartTime), zap.Time("endTime", timeRangeInfo.TimeRange.EndTime))
			return fmt.Errorf("create a merged shard %d", shardID)
		}
		sh, err := dbPTInfo.NewShard(rp, shardID, timeRangeInfo, e.metaClient, mstInfo)
		if err != nil {
			return err
		}
		if mstInfo != nil {
			sh.SetObsOption(mstInfo.ObsOptions)
		}
		dbPTInfo.shards[shardID] = sh
		newestShardID, ok := dbPTInfo.newestRpShard[rp]
		if !ok || newestShardID < shardID {
			dbPTInfo.newestRpShard[rp] = shardID
		}
	}
	return nil
}

func (e *EngineImpl) GetShardSplitPoints(db string, ptId uint32, shardID uint64, idxes []int64) ([]string, error) {
	e.mu.RLock()
	if !e.isDBPtExist(db, ptId) {
		e.mu.RUnlock()
		return nil, errno.NewError(errno.PtNotFound)
	}
	dbPtInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()

	sh := dbPtInfo.Shard(shardID)
	if sh == nil {
		return nil, errno.NewError(errno.ShardNotFound, shardID)
	}

	if err := e.openShardLazy(sh); err != nil {
		return nil, err
	}

	return sh.GetSplitPoints(idxes)
}

func (e *EngineImpl) isDBPtExist(db string, ptId uint32) bool {
	if dbPT, dbExist := e.DBPartitions[db]; dbExist {
		if _, dbPTExist := dbPT[ptId]; dbPTExist {
			return true
		}
	}
	return false
}

func (e *EngineImpl) addDBPTInfo(dbPTInfo *DBPTInfo) {
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

func (e *EngineImpl) dropDBPTInfo(database string, ptID uint32) {
	if dbPT, dbExist := e.DBPartitions[database]; dbExist {
		delete(dbPT, ptID)
		if len(dbPT) == 0 {
			delete(e.DBPartitions, database)
		}
	}
}

func (e *EngineImpl) CreateDBPT(db string, pt uint32, enableTagArray bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ptPath := path.Join(e.dataPath, config.DataDirectory, db, strconv.Itoa(int(pt)))
	walPath := path.Join(e.walPath, config.WalDirectory, db, strconv.Itoa(int(pt)))
	lockPath := path.Join(ptPath, "LOCK")

	options, _ := e.metaClient.DatabaseOption(db)
	if options != nil && options.Enabled {
		ptPath = fileops.GetRemoteDataPath(options, ptPath)
	}
	dbPTInfo := NewDBPTInfo(db, pt, ptPath, walPath, e.loadCtx, e.fileInfos, options)
	dbPTInfo.lockPath = &lockPath
	e.addDBPTInfo(dbPTInfo)
	dbPTInfo.SetOption(e.engOpt)
	dbPTInfo.enableReportShardLoad()
	dbPTInfo.enableTagArray = enableTagArray
}

func (e *EngineImpl) createDBPTIfNotExist(db string, pt uint32, enableTagArray bool) {
	e.mu.RLock()
	if e.isDBPtExist(db, pt) {
		e.mu.RUnlock()
		return
	}
	e.mu.RUnlock()
	e.CreateDBPT(db, pt, enableTagArray)
}

func (e *EngineImpl) startDrop(name string, droppingMap map[string]string) error {
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

func (e *EngineImpl) endDrop(name string, droppingMap map[string]string) {
	e.dropMu.Lock()
	delete(droppingMap, name)
	e.dropMu.Unlock()
}

func (e *EngineImpl) getPartition(db string, ptID uint32, isRef bool) (*DBPTInfo, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	dbPT, dbExist := e.DBPartitions[db]
	if !dbExist {
		return nil, errno.NewError(errno.DatabaseNotFound, db)
	}
	pt, ptExist := dbPT[ptID]
	if !ptExist {
		return nil, errno.NewError(errno.PtNotFound)
	}
	if !isRef {
		return pt, nil
	}
	if suc := pt.ref(); suc {
		return pt, nil
	}
	return nil, errno.NewError(errno.DBPTClosed)
}

func deleteDataAndWalPath(dataPath, walPath string, obsOpt *obs.ObsOptions, lockPath *string) error {
	logger.GetLogger().Info("deleteDataAndWalPath",
		zap.String("data", dataPath), zap.String("wal", walPath))

	// delete the remote dir
	if obsOpt != nil {
		if err := deleteDir(fileops.GetRemoteDataPath(obsOpt, dataPath), lockPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	// delete the local dir
	if err := deleteDir(dataPath, lockPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := deleteDir(walPath, lockPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func deleteDir(path string, lockPath *string) error {
	lock := fileops.FileLockOption(*lockPath)
	if err := fileops.RemoveAll(path, lock); err != nil {
		return err
	}

	return nil
}

func (e *EngineImpl) deleteShardsAndIndexes(dbPTInfo *DBPTInfo) error {
	dbPTInfo.mu.Lock()
	defer dbPTInfo.mu.Unlock()

	for id, shard := range dbPTInfo.shards {
		if err := shard.Close(); err != nil {
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
	var errs []error
	deleteRps := make([]string, 0, len(dbPTInfo.delIndexBuilderMap))
	for rp, iBuild := range dbPTInfo.delIndexBuilderMap {
		if err := iBuild.Close(); err != nil {
			e.log.Error("drop series failed", zap.Uint32("ptId", dbPTInfo.id), zap.String("rp", rp), zap.Error(err))
			errs = append(errs, err)
		} else {
			deleteRps = append(deleteRps, rp)
		}
	}
	for _, rp := range deleteRps {
		delete(dbPTInfo.delIndexBuilderMap, rp)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (e *EngineImpl) dropDBPt(db string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id := range e.DBPartitions[db] {
		e.dropDBPTInfo(db, id)
	}
}

func (e *EngineImpl) deleteIndexes(db string, pt uint32, rp string, fn func(dbPTInfo *DBPTInfo, shardID uint64, sh Shard) error) error {
	if config.IsLogKeeper() {
		return nil
	}
	resC := make(chan error)
	indexes := make(map[uint64]struct{})

	dbPTInfo := e.getDBPTInfo(db, pt)
	shardIDs := dbPTInfo.ShardIds(nil)

	var n int
	for _, shardId := range shardIDs {
		sh := dbPTInfo.Shard(shardId)
		if sh.GetRPName() != rp {
			continue
		}

		indexID := sh.GetIndexBuilder().GetIndexID()
		if _, ok := dbPTInfo.getIndexBuilder(indexID); ok {
			indexes[indexID] = struct{}{}
		}
		n++

		go func(info *DBPTInfo, id uint64, sh Shard) {
			err := fn(info, id, sh)
			if err != nil {
				resC <- fmt.Errorf("shard %d: %s", id, err)
				return
			}
			resC <- err
		}(dbPTInfo, shardId, sh)
	}

	var err error
	for i := 0; i < n; i++ {
		res := <-resC
		if res != nil {
			err = res
		}
	}
	close(resC)

	if err != nil {
		return err
	}

	for index := range indexes {
		if e := e.deleteOneIndex(dbPTInfo, index); e != nil {
			err = e
		}
	}

	if err != nil {
		return err
	}

	dbPTInfo.mu.Lock()
	defer dbPTInfo.mu.Unlock()
	delIndexBuilder := dbPTInfo.delIndexBuilderMap[rp]
	if delIndexBuilder == nil {
		return nil
	}
	if err = delIndexBuilder.Close(); err == nil {
		delete(dbPTInfo.delIndexBuilderMap, rp)
	}

	return err
}

func (e *EngineImpl) deleteOneIndex(dbPTInfo *DBPTInfo, indexId uint64) error {
	dbPTInfo.mu.Lock()
	iBuilder, ok := dbPTInfo.indexBuilder[indexId]
	if !ok {
		dbPTInfo.mu.Unlock()
		return nil
	}
	dbPTInfo.pendingIndexDeletes[indexId] = struct{}{}
	dbPTInfo.mu.Unlock()

	err := iBuilder.Close()
	dbPTInfo.mu.Lock()
	defer dbPTInfo.mu.Unlock()
	if err == nil {
		delete(dbPTInfo.indexBuilder, indexId)
	}
	delete(dbPTInfo.pendingIndexDeletes, indexId)
	return err
}

func (e *EngineImpl) SeriesExactCardinality(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr, tr influxql.TimeRange) (map[string]uint64, error) {
	keysMap, err := e.searchIndex(db, ptIDs, measurements, condition, tr, e.handleSeries)
	if err != nil {
		return nil, err
	}

	// Count all measurement series cardinality
	result := make(map[string]uint64, len(measurements))
	for _, nameBytesWithVer := range measurements {
		name := influx.GetOriginMstName(util.Bytes2str(nameBytesWithVer))
		result[name] = uint64(len(keysMap[name]))
	}
	return result, nil
}

func (e *EngineImpl) searchIndex(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr, tr influxql.TimeRange, fn func(key []byte, keysMap map[string]map[string]struct{}, mstName string)) (map[string]map[string]struct{}, error) {
	e.mu.RLock()
	var err error
	if ptIDs, err = e.checkAndAddRefPTSNoLock(db, ptIDs); err != nil {
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
	for _, nameWithVer := range measurements {
		name := influx.GetOriginMstName(util.Bytes2str(nameWithVer))
		keysMap[name] = make(map[string]struct{}, 64)
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
	for _, ptID := range ptIDs {
		pt, ok := pts[ptID]
		if !ok {
			continue
		}
		pt.mu.RLock()
		for _, iBuild := range pt.indexBuilder {
			if !iBuild.Overlaps(tr) {
				continue
			}
			for _, nameWithVer := range measurements {
				mstName := influx.GetOriginMstName(util.Bytes2str(nameWithVer))
				stime := time.Now()
				idx, ok := iBuild.GetPrimaryIndex().(*tsi.MergeSetIndex)
				if !ok {
					return nil, errors.New("idx nil,some thing wrong with GetPrimaryIndex")
				}
				series, err = idx.SearchSeriesKeys(series[:0], nameWithVer, condition)
				if len(series) > seriesLen {
					seriesLen = len(series)
				}
				log.Info("search series keys", zap.ByteString("nameWithVer", nameWithVer),
					zap.Duration("cost", time.Since(stime)))
				if err != nil {
					pt.mu.RUnlock()
					return nil, err
				}
				stime = time.Now()
				for _, key := range series {
					key = bytes.Replace(key, nameWithVer, []byte(mstName), 1)
					fn(key, keysMap, mstName)
				}
				log.Info("remove dupicate key", zap.String("nameWithVer", string(nameWithVer)),
					zap.Duration("cost", time.Since(stime)))
			}
		}
		pt.mu.RUnlock()
	}
	return keysMap, nil
}

func (e *EngineImpl) handleSeries(key []byte, keysMap map[string]map[string]struct{}, mstName string) {
	keysMap[mstName][string(key)] = struct{}{}
}

func (e *EngineImpl) handleTagKeys(key []byte, keysMap map[string]map[string]struct{}, mstName string) {
	arr := strings.Split(string(key), ",")

	for _, item := range arr[1:] {
		kv := strings.Split(item, "=")
		keysMap[mstName][kv[0]] = struct{}{}
	}
}

func (e *EngineImpl) DbPTRef(db string, ptId uint32) error {
	var err error
	e.mu.RLock()
	err = e.checkAndAddRefPTNoLock(db, ptId)
	e.mu.RUnlock()
	return err
}

func (e *EngineImpl) DbPTUnref(db string, ptId uint32) {
	e.mu.RLock()
	e.unrefDBPTNoLock(db, ptId)
	e.mu.RUnlock()
}

func (e *EngineImpl) GetShard(db string, ptId uint32, shardID uint64) (Shard, error) {
	pt := e.getDBPTInfo(db, ptId)
	if pt == nil {
		return nil, nil
	}

	sh := pt.Shard(shardID)
	if sh == nil {
		return nil, nil
	}
	if err := e.openShardLazy(sh); err != nil {
		return nil, err
	}
	return sh, nil
}

// getShardDownSampleLevel returns down sample level.
// If db pt or shard not found, return 0.
func (e *EngineImpl) getShardDownSampleLevel(db string, ptId uint32, shardID uint64) int {
	pt := e.getDBPTInfo(db, ptId)
	if pt == nil {
		return 0
	}

	sh := pt.Shard(shardID)
	if sh == nil {
		return 0
	}

	return sh.GetIdent().DownSampleLevel
}

func (e *EngineImpl) getDBPTInfo(db string, ptId uint32) *DBPTInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.DBPartitions[db][ptId]
}

func (e *EngineImpl) CreateLogicalPlan(ctx context.Context, db string, ptId uint32, shardID []uint64,
	sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error) {
	if len(shardID) == 1 {
		return e.CreateLogicalPlanOneShard(ctx, db, ptId, shardID, sources, schema)
	}
	return e.CreateLogicalPlanCrossShard(ctx, db, ptId, shardID, sources, schema)
}

func (e *EngineImpl) CreateLogicalPlanOneShard(ctx context.Context, db string, ptId uint32, shardID []uint64,
	sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error) {
	sh, err := e.GetShard(db, ptId, shardID[0])
	if err != nil {
		return nil, err
	}
	if sh == nil {
		return nil, nil
	}
	return sh.CreateLogicalPlan(ctx, sources, schema)
}

func (e *EngineImpl) CreateLogicalPlanCrossShard(ctx context.Context, db string, ptId uint32, shardID []uint64,
	sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error) {
	span := tracing.SpanFromContext(ctx)
	defer tracing.Finish(span)

	var seriesNum int
	var tagSetMergeInfos []*tsi.TagSetMergeInfo
	shards := make([]*shard, 0, len(shardID))
	for i := range shardID {
		sh, err := e.GetShard(db, ptId, shardID[i])
		if err != nil {
			return nil, err
		}
		if sh == nil {
			return nil, nil
		}
		shards = append(shards, sh.(*shard))
	}
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].GetStartTime().Before(shards[j].GetStartTime())
	})
	for i := range shards {
		if i > 0 && shards[i-1].IsSameIndex(shards[i]) {
			tagSetMergeInfos = tsi.ExtendMergeTagSetInfos(tagSetMergeInfos, shards[i].GetID())
			continue
		}
		tagSetInfos, _, err := shards[i].ScanWithInvertedIndex(span, ctx, sources, schema)
		if err != nil {
			return nil, err
		}
		if tagSetInfos == nil {
			continue
		}
		tagSetMergeInfos = tsi.SortMergeTagSetInfos(tagSetMergeInfos, tagSetInfos, shards[i].GetID(), len(shards))
	}
	tagSets := make([]tsi.TagSet, len(tagSetMergeInfos))
	for i := range tagSetMergeInfos {
		seriesNum += tagSetMergeInfos[i].Len()
		tagSets[i] = tagSetMergeInfos[i]
	}
	info, err := CreateCursor(ctx, schema, nil, shards, tagSets, seriesNum)
	if err != nil {
		return nil, err
	}
	if info == nil || info.IsEmpty() {
		log.Debug("no data in shard", zap.Uint64s("id", shardID))
		return nil, nil
	}
	return executor.NewLogicalDummyShard(info), nil
}

func (e *EngineImpl) ScanWithSparseIndex(ctx context.Context, db string, ptId uint32, shardIDs []uint64, schema *executor.QuerySchema) (executor.ShardsFragments, error) {
	shardFrags := executor.NewShardsFragments()
	for _, shardId := range shardIDs {
		s, err := e.GetShard(db, ptId, shardId)
		if err != nil {
			return nil, err
		}
		if s == nil {
			e.log.Warn(fmt.Sprintf("ScanWithSparseIndex shard is null. db: %s, ptId: %d, shardId: %d", db, ptId, shardId))
			continue
		}
		fileFrags, err := s.ScanWithSparseIndex(ctx, schema, resourceallocator.DefaultSeriesAllocateFunc)
		if err != nil {
			return nil, err
		}
		if fileFrags == nil {
			continue
		}
		shardFrags[shardId] = fileFrags
	}
	return shardFrags, nil
}

func (e *EngineImpl) GetIndexInfo(db string, ptId uint32, shardID uint64, schema *executor.QuerySchema) (*executor.AttachedIndexInfo, error) {
	s, err := e.GetShard(db, ptId, shardID)
	if err != nil {
		return nil, err
	}
	if s == nil {
		e.log.Warn(fmt.Sprintf("GetIndexInfo shard is null. db: %s, ptId: %d, shardId: %d", db, ptId, shardID))
		return executor.NewAttachedIndexInfo(nil, nil), nil
	}
	return s.GetIndexInfo(schema)
}

func (e *EngineImpl) RowCount(db string, ptId uint32, shardIDs []uint64, schema *executor.QuerySchema) (int64, error) {
	var rowCount int64
	for _, shardId := range shardIDs {
		s, err := e.GetShard(db, ptId, shardId)
		if err != nil {
			return 0, err
		}
		if s == nil {
			e.log.Warn(fmt.Sprintf("RowCount shard is null. db: %s, ptId: %d, shardId: %d", db, ptId, shardId))
			continue
		}
		shardRowCount, err := s.RowCount(schema)
		if err != nil {
			return 0, err
		}
		rowCount += shardRowCount
	}
	return rowCount, nil
}

func (e *EngineImpl) LogicalPlanCost(db string, ptId uint32, sources influxql.Sources, opt query.ProcessorOptions) (hybridqp.LogicalPlanCost, error) {
	panic("implement me")
}

func (e *EngineImpl) checkAndAddRefPTSNoLock(database string, ptIDs []uint32) ([]uint32, error) {
	delRefPtId := make([]uint32, 0, len(ptIDs))
	var err error
	for _, ptId := range ptIDs {
		if err1 := e.checkAndAddRefPTNoLock(database, ptId); err1 != nil {
			err = err1
			break
		} else {
			delRefPtId = append(delRefPtId, ptId)
		}
	}

	if err != nil {
		e.unrefDBPTSNoLock(database, delRefPtId)
	}
	return delRefPtId, err
}

func (e *EngineImpl) checkAndAddRefPTNoLock(database string, ptID uint32) error {
	if dbPT, dbExist := e.DBPartitions[database]; dbExist {
		if _, ok := dbPT[ptID]; ok {
			if suc := e.DBPartitions[database][ptID].ref(); suc {
				return nil
			} else {
				return errno.NewError(errno.DBPTClosed)
			}
		} else {
			return errno.NewError(errno.PtNotFound)
		}
	} else {
		return errno.NewError(errno.PtNotFound)
	}
}

func (e *EngineImpl) unrefDBPT(database string, ptID uint32) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	e.unrefDBPTNoLock(database, ptID)
}

func (e *EngineImpl) unrefDBPTs(database string, ptIDs []uint32) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, ptID := range ptIDs {
		e.unrefDBPTNoLock(database, ptID)
	}
}

func (e *EngineImpl) refDBPTsNoLock(ptInfo map[uint32]*DBPTInfo, db string) ([]uint32, error) {
	ptIds := make([]uint32, len(ptInfo))
	i := 0
	for id := range ptInfo {
		ptIds[i] = id
		i++
	}
	var err error
	if ptIds, err = e.checkAndAddRefPTSNoLock(db, ptIds); err != nil {
		return nil, err
	}

	return ptIds, nil
}

func (e *EngineImpl) unrefDBPTSNoLock(database string, ptIDs []uint32) {
	for _, ptID := range ptIDs {
		e.unrefDBPTNoLock(database, ptID)
	}
}

func (e *EngineImpl) unrefDBPTNoLock(database string, ptID uint32) {
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

func (e *EngineImpl) SysCtrl(req *msgservice.SysCtrlRequest) (map[string]string, error) {
	return e.processReq(req)
}

func (e *EngineImpl) Statistics(buffer []byte) ([]byte, error) {
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
				buffer, _ = sh.GetStatistics(buffer)
			}
			dbPTInfo.mu.RUnlock()
		}
	}
	return buffer, nil
}

func (e *EngineImpl) InitLogStoreCtx(querySchema *executor.QuerySchema) (*idKeyCursorContext, error) {
	ctx := &idKeyCursorContext{
		decs:         immutable.NewReadContext(querySchema.Options().IsAscending()),
		maxRowCnt:    querySchema.Options().ChunkSizeNum(),
		aggPool:      AggPool,
		seriesPool:   SeriesPool,
		tmsMergePool: TsmMergePool,
		querySchema:  querySchema,
	}
	err := newCursorSchema(ctx, querySchema)
	if err != nil {
		return nil, err
	}

	if ctx.schema.Len() <= 1 {
		return nil, errno.NewError(errno.NoFieldSelected, "initCtx")
	}
	ctx.tr.Min = querySchema.Options().GetStartTime()
	ctx.tr.Max = querySchema.Options().GetEndTime()
	ctx.decs.SetTr(ctx.tr)
	return ctx, nil
}

func GetCtx(querySchema *executor.QuerySchema) (*idKeyCursorContext, error) {
	ctx := &idKeyCursorContext{
		decs:         immutable.NewReadContext(querySchema.Options().IsAscending()),
		maxRowCnt:    querySchema.Options().ChunkSizeNum(),
		aggPool:      AggPool,
		seriesPool:   SeriesPool,
		tmsMergePool: TsmMergePool,
		querySchema:  querySchema,
	}
	err := newCursorSchema(ctx, querySchema)
	if err != nil {
		return nil, err
	}

	if ctx.schema.Len() <= 1 {
		return nil, errno.NewError(errno.NoFieldSelected, "initCtx")
	}
	ctx.tr.Min = querySchema.Options().GetStartTime()
	ctx.tr.Max = querySchema.Options().GetEndTime()
	ctx.decs.SetTr(ctx.tr)
	return ctx, nil
}

func (e *EngineImpl) HierarchicalStorage(db string, ptId uint32, shardID uint64) bool {
	e.log.Info("[hierarchical storage]", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("shard", shardID))
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		e.log.Error("[hierarchical storage] add pt ref err", zap.String("db", db), zap.Uint32("pt", ptId), zap.Error(err))
		return false
	}

	dbPTInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()
	defer e.unrefDBPT(db, ptId)
	dbPTInfo.mu.Lock()
	if dbPTInfo.doingOff {
		dbPTInfo.mu.Unlock()
		e.log.Error("[hierarchical storage] pt is doingMigrate err", zap.String("db", db), zap.Uint32("pt", ptId))
		return false
	} else {
		dbPTInfo.doingShardMoveNInc()
		defer dbPTInfo.doingShardMoveNDec()
	}
	sh := dbPTInfo.ShardNoLock(shardID)
	dbPTInfo.mu.Unlock()
	if sh == nil {
		e.log.Error("shard not found", zap.Error(errno.NewError(errno.ShardNotFound, shardID)))
		return false
	}

	if err := sh.OpenAndEnable(e.metaClient); err != nil {
		e.log.Error("[hierarchical storage] shard open err", zap.String("db", db),
			zap.Uint32("pt", ptId), zap.Uint64("shard", shardID), zap.Error(err))
		return false
	}

	if !syscontrol.IsWriteColdShardEnabled() {
		if err := sh.UpdateShardReadOnly(e.metaClient); err != nil {
			e.log.Error("[hierarchical storage] update shard read only fail", zap.String("db", db),
				zap.Uint32("pt", ptId), zap.Uint64("shard", shardID), zap.Error(err))
			return false
		}
	}

	if !sh.CanDoShardMove() {
		e.log.Info("shard have not finished full compact yet ", zap.Uint64("shard id", shardID))
		return false
	}

	// unregister cold shard
	sh.UnregisterShard()
	if err := sh.ExecShardMove(); err != nil {
		e.log.Error("[hierarchical storage] exec shard move fail", zap.String("db", db),
			zap.Uint32("pt", ptId), zap.Uint64("shard", shardID), zap.Error(err))
		return false
	}

	e.log.Info("[hierarchical storage] shard move success", zap.String("db", db),
		zap.Uint32("pt", ptId), zap.Uint64("shard", shardID))
	return true
}

func (e *EngineImpl) GetDBPtIds() map[string][]uint32 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	dbPts := make(map[string][]uint32, len(e.DBPartitions))
	for name, ptMap := range e.DBPartitions {
		pts := make([]uint32, 0, len(ptMap))
		for ptId := range ptMap {
			pts = append(pts, ptId)
		}
		dbPts[name] = pts
	}

	return dbPts
}

func (e *EngineImpl) ClearShardRepCold(database string, rp string, ptId uint32, shardPair *msgservice_data.ShardPair) error {
	key := genKey(database, rp, ptId, shardPair.GetToClearShard())
	e.clearShardMapLock.Lock()
	_, ok := e.clearRepColdShardMap[key]
	if ok {
		return errors.New("already exists" + key)
	}
	e.clearRepColdShardMap[key] = struct{}{}
	e.clearShardMapLock.Unlock()
	defer func() {
		e.clearShardMapLock.Lock()
		delete(e.clearRepColdShardMap, key)
		e.clearShardMapLock.Unlock()
	}()
	dbptInfo := e.DBPartitions[database][ptId]
	if dbptInfo == nil {
		return errors.New("nil dbpt")
	}
	sh := dbptInfo.Shard(shardPair.GetToClearShard())
	if sh == nil {
		return errors.New("nil shard")
	}
	// replace
	oldFiles, oldUnOrderFiles, oldTier, err := sh.ReplaceByNoClearShardId(shardPair.GetNoClearShard())
	if err != nil {
		return err
	}
	// update tier status to cleared
	if err := e.metaClient.UpdateShardInfoTier(sh.GetID(), util.Cleared, sh.GetIdent().OwnerDb, sh.GetIdent().Policy); err != nil {
		e.log.Error("update shard info tier err",
			zap.Int64("shard id", int64(sh.GetID())),
			zap.Int64("tier", int64(util.Cleared)), zap.Error(err))
		sh.RecoverTier(oldTier)
		return err
	}
	// clear
	err = sh.ClearOldTsspFiles(oldFiles, oldUnOrderFiles)
	if err != nil {
		return err
	}
	return nil
}

func genKey(database string, rp string, ptId uint32, shardId uint64) string {
	return database + ":" + rp + ":" + strconv.FormatUint(uint64(ptId), 10) + ":" + strconv.FormatUint(shardId, 10)
}

func (e *EngineImpl) IndexHierarchicalStorage(db string, ptId uint32, indexID uint64) bool {
	e.log.Info("[hierarchical index storage]", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("index", indexID))
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(db, ptId); err != nil {
		e.mu.RUnlock()
		e.log.Error("[hierarchical index storage] add pt ref err", zap.String("db", db), zap.Uint32("pt", ptId), zap.Error(err))
		return false
	}
	dbPTInfo := e.DBPartitions[db][ptId]
	e.mu.RUnlock()
	defer e.unrefDBPT(db, ptId)
	dbPTInfo.mu.Lock()
	if dbPTInfo.doingOff {
		dbPTInfo.mu.Unlock()
		e.log.Error("[hierarchical index storage] pt is doingMigrate err", zap.String("db", db), zap.Uint32("pt", ptId))
		return false
	} else {
		dbPTInfo.doingIndexMoveNInc()
		defer dbPTInfo.doingIndexMoveNDec()
	}
	index := dbPTInfo.IndexNoLock(indexID)
	dbPTInfo.mu.Unlock()
	if index == nil {
		e.log.Error("index not found", zap.Error(errno.NewError(errno.ShardNotFound, indexID)))
		return false
	}

	if err := index.ExecIndexMove(); err != nil {
		e.log.Error("[hierarchical index storage] exec index move fail", zap.String("db", db),
			zap.Uint32("pt", ptId), zap.Uint64("index", indexID), zap.Error(err))
		return false
	}

	e.log.Info("[hierarchical index storage] index move success", zap.String("db", db),
		zap.Uint32("pt", ptId), zap.Uint64("index", indexID))
	return true
}

func (e *EngineImpl) WriteBlobs(db string, ptId uint32, shardID uint64, group *shelf.BlobGroup) error {
	sh, err := e.getShard(db, ptId, shardID)
	if err != nil {
		return err
	}

	return sh.WriteBlobs(group)
}

// for a mergeShards list
func (e *EngineImpl) MergeShards(mergeShards meta2.MergeShards) error {
	if len(mergeShards.ShardIds) == 1 {
		return nil
	}

	// 1. sort mergeshards by time, get all mergeShards, generate mergeShardPath
	sort.Sort(mergeShards)
	var shards []Shard
	var err error
	var shard Shard
	var dbpt *DBPTInfo
	var firstValidShard Shard
	e.log.Info("[merge shards] start", zap.String("db", mergeShards.DbName), zap.Uint32("pt", mergeShards.PtId), zap.String("rp", mergeShards.RpName),
		zap.Any("shardIds", mergeShards.ShardIds), zap.Any("shardEndTimes", mergeShards.ShardEndTimes))

	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(mergeShards.DbName, mergeShards.PtId); err != nil {
		e.mu.RUnlock()
		e.log.Error("[merge shards] add pt ref err", zap.String("db", mergeShards.DbName), zap.Uint32("pt", mergeShards.PtId), zap.Error(err))
		return nil
	}

	dbpt = e.DBPartitions[mergeShards.DbName][mergeShards.PtId]
	e.mu.RUnlock()
	defer e.unrefDBPT(mergeShards.DbName, mergeShards.PtId)

	dbpt.mu.Lock()
	if dbpt.doingOff {
		dbpt.mu.Unlock()
		e.log.Error("[merge shards] pt is doingMigrate err", zap.String("db", dbpt.database), zap.Uint32("pt", dbpt.id))
		return nil
	} else {
		dbpt.doingShardMoveNInc()
		defer dbpt.doingShardMoveNDec()
	}
	dbpt.mu.Unlock()
	validShardsCount := 0
	for _, shardId := range mergeShards.ShardIds {
		shard = dbpt.Shard(shardId)
		if shard == nil {
			shards = append(shards, nil)
			continue
		}
		validShardsCount++
		if firstValidShard == nil {
			firstValidShard = shard
		}
		if err := e.openShardLazy(shard); err != nil {
			return err
		}
		shards = append(shards, shard)
	}
	// support merge all nil shards
	e.log.Info("[merge shards] nil shards", zap.String("db", mergeShards.DbName), zap.Uint32("ptId", mergeShards.PtId),
		zap.Uint64("firstShardId", mergeShards.ShardIds[0]), zap.Int("len(mergeShards)", len(mergeShards.ShardIds)),
		zap.Int("validShardCount", validShardsCount))

	var indexes []*tsi.IndexBuilder
	rpPath := path.Join(dbpt.path, mergeShards.RpName) + "/"
	mergeShardPath := rpPath // eg. ..../rp/_1_2_3_shardId..._merge
	mergeIndexIds := make(map[uint64]interface{})
	for i, shard := range shards {
		mergeShardPath += "_" + strconv.FormatUint(mergeShards.ShardIds[i], 10)
		if shard == nil {
			continue
		}
		index := shard.GetIndexBuilder()
		if index == nil {
			continue
		}
		if _, ok := mergeIndexIds[index.GetIndexID()]; !ok {
			mergeIndexIds[index.GetIndexID()] = nil
			indexes = append(indexes, index)
		}
	}
	mergeShardPath += config.MergeDriSuffix

	if len(indexes) > 1 {
		return fmt.Errorf("mergeshards has more than 1 indexes: %d", len(indexes))
	}

	// 2. init mergeShardPath dirs
	lock := fileops.FileLockOption(*dbpt.lockPath)
	err = fileops.RemoveAll(mergeShardPath, lock)
	if err != nil {
		e.log.Error("[merge shards] removeAll1 mergeShardPath failed", zap.String("mergeShardPath", mergeShardPath), zap.Error(err))
		return err
	}

	err = InitMergeShardDirs(mergeShardPath, lock, mergeShards.EngineType[0])
	if err != nil {
		err1 := fileops.RemoveAll(mergeShardPath, lock)
		if err1 != nil {
			e.log.Error("[merge shards] removeAll2 mergeShardPath failed", zap.String("mergeShardPath", mergeShardPath), zap.Error(err1))
		}
		return err
	}

	// 3. merge shards one by one
	var maxFileSeq uint64
	if firstValidShard != nil {
		maxFileSeq = firstValidShard.GetFileSeq()
	}

	for i := 0; i < len(shards); i++ {
		srcShard := shards[i]
		if srcShard == nil {
			continue
		}
		err = srcShard.MergeToDstShard(mergeShardPath, maxFileSeq, lock)
		if err != nil {
			e.log.Error("[merge shards] MergeToDstShard err", zap.Error(err), zap.String("db", mergeShards.DbName),
				zap.Uint32("ptId", mergeShards.PtId), zap.String("mergeShardPath", mergeShardPath), zap.Uint64("srcShardId", srcShard.GetID()))
			err1 := fileops.RemoveAll(mergeShardPath, lock)
			if err1 != nil {
				e.log.Error("[merge shards] removeAll3 mergeShardPath failed", zap.String("mergeShardPath", mergeShardPath), zap.Error(err1))
			}
			return err
		}
		maxFileSeq += srcShard.GetFileSeq()
	}

	// 4. replace shards in ts-meta to get newShardInfo
	err = e.metaClient.ReplaceMergeShards(mergeShards)
	if err != nil {
		e.log.Error("[merge shards] ReplaceMergeShards err", zap.Error(err), zap.String("db", mergeShards.DbName),
			zap.Uint32("ptId", mergeShards.PtId))
		err1 := fileops.RemoveAll(mergeShardPath, lock)
		if err1 != nil {
			e.log.Error("[merge shards] removeAll4 mergeShardPath failed", zap.String("mergeShardPath", mergeShardPath), zap.Error(err1))
		}
		return err
	}
	newShardTimeRangeInfo, err := e.metaClient.GetTimeRange(dbpt.database, mergeShards.RpName, mergeShards.ShardIds[0], mergeShards.ShardIds[len(mergeShards.ShardIds)-1])
	if err != nil {
		e.log.Error("[merge shards] GetTimeRange err", zap.Error(err), zap.String("db", mergeShards.DbName),
			zap.Uint32("ptId", mergeShards.PtId))
		err1 := fileops.RemoveAll(mergeShardPath, lock)
		if err1 != nil {
			e.log.Error("[merge shards] removeAll5 mergeShardPath failed", zap.String("mergeShardPath", mergeShardPath), zap.Error(err1))
		}
		return err
	}

	// 5. rename mergeShardPath by newShardInfo and newIndexInfo in ts-store
	newShardPath := GenMergeShardPath(rpPath, newShardTimeRangeInfo.TimeRange.StartTime.UnixNano(),
		newShardTimeRangeInfo.TimeRange.EndTime.UnixNano(), newShardTimeRangeInfo.ShardDuration.Ident.ShardID,
		newShardTimeRangeInfo.OwnerIndex.IndexID)

	err1 := fileops.RenameFile(mergeShardPath, newShardPath, lock)
	if err1 != nil {
		e.log.Error("[merge shards] rename file failed", zap.String("mergeShardPath", mergeShardPath),
			zap.String("newShardPath", newShardPath), zap.Error(err1))
		err = fileops.RemoveAll(mergeShardPath, lock)
		if err != nil {
			e.log.Error("[merge shards] removeAll6 mergeShardPath failed", zap.String("mergeShardPath", mergeShardPath), zap.Error(err))
		}
		return err1
	}

	// 6. new newShard and open
	mstInfo := &meta2.MeasurementInfo{EngineType: config.EngineType(newShardTimeRangeInfo.ShardDuration.Ident.EngineType)}
	newShard, err := dbpt.NewShard(newShardTimeRangeInfo.ShardDuration.Ident.Policy, newShardTimeRangeInfo.ShardDuration.Ident.ShardID,
		newShardTimeRangeInfo, e.metaClient, mstInfo)
	if err != nil {
		e.log.Error("[merge shards] NewShard err", zap.Error(err), zap.String("db", mergeShards.DbName),
			zap.Uint32("ptId", mergeShards.PtId), zap.Uint64("newShardId", newShardTimeRangeInfo.ShardDuration.Ident.ShardID))
		return err
	}
	// newShard.SetMstInfo(mstInfo) skip, only support tsstore engine
	newShard.SetObsOption(mstInfo.ObsOptions)

	// 7. replace shards in ts-store, carefully: indexes has dbpt.indexes and shard.index
	err = e.ReplaceShards(newShard, shards, newShardPath, dbpt, mergeShards)
	if err != nil {
		err1 = fileops.RemoveAll(mergeShardPath, lock)
		if err1 != nil {
			e.log.Error("[merge shards] removeAll7 mergeShardPath failed", zap.String("mergeShardPath", mergeShardPath), zap.Error(err1))
		}
	}
	return err
}

func (e *EngineImpl) ReplaceShards(newShard Shard, shards []Shard, mergeShardPath string, dbpt *DBPTInfo, mergeShards meta2.MergeShards) error {
	e.log.Info("[replace shard] start", zap.Int("len(delShards)", len(shards)), zap.Uint64("newShardId", newShard.GetID()),
		zap.String("db", dbpt.database), zap.String("rp", newShard.GetRPName()), zap.Uint32("ptId", dbpt.id))
	var err error
	defer func(err error) {
		e.log.Info("[replace shard] end", zap.Int("len(delShards)", len(shards)), zap.Uint64("newShardId", newShard.GetID()),
			zap.String("db", dbpt.database), zap.String("rp", newShard.GetRPName()), zap.Uint32("ptId", dbpt.id), zap.Error(err))
	}(err)

	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(dbpt.database, dbpt.id); err != nil {
		e.mu.RUnlock()
		logger.GetLogger().Error("[replace shard] checkAndAddRefPTNoLock err", zap.Error(err), zap.String("db", dbpt.database),
			zap.Uint32("ptId", dbpt.id))
		return err
	}
	e.mu.RUnlock()
	defer e.unrefDBPT(dbpt.database, dbpt.id)

	dbpt.mu.Lock()
	defer dbpt.mu.Unlock()
	for i, sh := range shards {
		delete(dbpt.shards, mergeShards.ShardIds[i])
		if sh == nil {
			continue
		}
		if err := sh.Close(); err != nil {
			e.log.Error("[replace shard] close shard fail", zap.Uint64("shardId", sh.GetID()), zap.Error(err))
			continue
		}
		lock := fileops.FileLockOption(*dbpt.lockPath)
		obsOption := sh.GetObsOption()
		if obsOption != nil {
			if err := fileops.RemoveAll(fileops.GetRemoteDataPath(obsOption, sh.GetDataPath()), lock); err != nil {
				e.log.Error("[replace shard] remove remote shard fail", zap.Uint64("shardId", sh.GetID()), zap.Error(err))
				continue
			}
		}
		if err := fileops.RemoveAll(sh.GetDataPath(), lock); err != nil {
			e.log.Error("[replace shard] remove shard datapath fail", zap.Uint64("shardId", sh.GetID()), zap.Error(err))
			continue
		}
		if err := fileops.RemoveAll(sh.GetWalPath(), lock); err != nil {
			e.log.Error("[replace shard] remove shard walpath fail", zap.Uint64("shardId", sh.GetID()), zap.Error(err))
			continue
		}
	}
	if newShardVal, ok := newShard.(*shard); ok {
		e.log.Info("[replace shard] registerNewShard to CompWorker", zap.Uint64("newShardId", newShard.GetID()),
			zap.String("db", dbpt.database), zap.String("rp", newShard.GetRPName()), zap.Uint32("ptId", dbpt.id))
		compWorker.RegisterShard(newShardVal)
	}

	dbpt.AddShard(newShard.GetID(), newShard)
	return nil
}

func (e *EngineImpl) ClearIndexRepCold(database string, rp string, ptId uint32, index *msgservice_data.IndexPair) error {
	key := genKey(database, rp, ptId, index.GetToClearIndex())
	e.clearIndexMapLock.Lock()
	_, ok := e.clearRepColdIndexMap[key]
	if ok {
		return errors.New("already exists" + key)
	}
	e.clearRepColdIndexMap[key] = struct{}{}
	e.clearIndexMapLock.Unlock()
	defer func() {
		e.clearIndexMapLock.Lock()
		delete(e.clearRepColdIndexMap, key)
		e.clearIndexMapLock.Unlock()
	}()
	dbptInfo := e.DBPartitions[database][ptId]

	idx := dbptInfo.indexBuilder[index.GetToClearIndex()]
	if idx == nil {
		return errors.New("nil idx")
	}
	// replace
	oldPath, lck, err := idx.ReplaceByNoClearIndexId(index.GetNoClearIndex())
	if err != nil {
		return err
	}
	err = e.ClearIndexCache(database, ptId, index.GetToClearIndex())
	if err != nil {
		return err
	}
	// update tier status to cleared
	if err := e.metaClient.UpdateIndexInfoTier(index.GetToClearIndex(), util.Cleared, database, rp); err != nil {
		e.log.Error("update shard info tier err",
			zap.Int64("shard id", int64(index.GetToClearIndex())),
			zap.Int64("tier", int64(util.Cleared)), zap.Error(err))
		return err
	}
	lock := fileops.FileLockOption(*lck)
	if err := fileops.RemoveAllWithOutDir(oldPath, lock); err != nil {
		logger.GetLogger().Error("remove local file index files error", zap.Error(err))
		return err
	}
	return nil
}

func (e *EngineImpl) GetDatabase(database string) map[uint32]*DBPTInfo {
	return e.DBPartitions[database]
}

func GenMergeShardPath(rpPath string, startTime int64, endTime int64, shardId uint64, indexId uint64) string {
	return rpPath + strconv.FormatUint(shardId, 10) + pathSeparator + strconv.FormatInt(startTime, 10) +
		pathSeparator + strconv.FormatInt(endTime, 10) + pathSeparator + strconv.FormatUint(indexId, 10)
}

func InitMergeShardDirs(mergeShardPath string, lock fileops.FileLockOption, engineType config.EngineType) error {
	err := fileops.RemoveAll(mergeShardPath, lock)
	if err != nil {
		return err
	}
	err = fileops.MkdirAll(mergeShardPath, 0700, lock)
	if err != nil {
		return err
	}
	mergeTsspPath := immutable.GetDir(engineType, mergeShardPath)
	return fileops.Mkdir(mergeTsspPath, 0700, lock)
}
