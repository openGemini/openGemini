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
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/raftconn"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

const (
	pathSeparator     = "_"
	DelIndexBuilderId = math.MaxUint64
	// DefaultDirPerm Default directory permissions (user can read, write, and execute; group can read and execute; others have no permissions)
	DefaultDirPerm = 0750
)

var (
	reportLoadFrequency = time.Second
)

type PtNNLock struct {
}

type DBPTInfo struct {
	//lint:ignore U1000 use for replication feature
	replicaInfo *message.ReplicaInfo

	node     raftNodeRequest
	proposeC chan<- []byte // proposed messages, only for raft replication
	ReplayC  chan *raftconn.Commit

	mu       sync.RWMutex
	database string
	id       uint32
	opt      EngineOptions

	logger     *logger.Logger
	exeCount   int64
	offloading bool
	preload    bool
	bgrEnabled bool
	ch         chan bool

	path    string
	walPath string

	// Maintains a set of shards that are in the process of deletion.
	// This prevents new shards from being created while old ones are being deleted.
	pendingShardDeletes map[uint64]struct{}
	shards              map[uint64]Shard
	pendingIndexDeletes map[uint64]struct{}
	indexBuilder        map[uint64]*tsi.IndexBuilder // [indexId, IndexBuilderer]
	delIndexBuilderMap  map[string]*tsi.IndexBuilder // [rpName, IndexBuilderer(for deleted tsids) ]
	pendingShardTiering map[uint64]struct{}
	closed              *interruptsignal.InterruptSignal
	newestRpShard       map[string]uint64
	loadCtx             *metaclient.LoadCtx
	unload              chan struct{}
	wg                  *sync.WaitGroup
	logicClock          uint64
	sequenceID          uint64
	lockPath            *string
	enableTagArray      bool
	fileInfos           chan []immutable.FileInfoExtend
	doingOff            bool
	doingShardMoveN     int32
	dbObsOptions        *obs.ObsOptions
}

func NewDBPTInfo(db string, id uint32, dataPath, walPath string, ctx *metaclient.LoadCtx, ch chan []immutable.FileInfoExtend, options *obs.ObsOptions) *DBPTInfo {
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
		delIndexBuilderMap:  make(map[string]*tsi.IndexBuilder),
		pendingShardDeletes: make(map[uint64]struct{}),
		pendingIndexDeletes: make(map[uint64]struct{}),
		pendingShardTiering: make(map[uint64]struct{}),
		loadCtx:             ctx,
		logger:              logger.NewLogger(errno.ModuleUnknown),
		wg:                  &sync.WaitGroup{},
		sequenceID:          uint64(time.Now().Unix()),
		bgrEnabled:          true,
		fileInfos:           ch,
		dbObsOptions:        options,
	}
}

func (dbPT *DBPTInfo) AddShard(id uint64, sh Shard) {
	dbPT.shards[id] = sh
}

func (dbPT *DBPTInfo) doingShardMoveNInc() {
	atomic.AddInt32(&dbPT.doingShardMoveN, 1)
}

func (dbPT *DBPTInfo) doingShardMoveNDec() {
	atomic.AddInt32(&dbPT.doingShardMoveN, -1)
}

func (dbPT *DBPTInfo) doingIndexMoveNInc() {
	atomic.AddInt32(&dbPT.doingShardMoveN, 1)
}

func (dbPT *DBPTInfo) doingIndexMoveNDec() {
	atomic.AddInt32(&dbPT.doingShardMoveN, -1)
}

func (dbPT *DBPTInfo) enableReportShardLoad() {
	dbPT.mu.Lock()
	defer dbPT.mu.Unlock()

	if dbPT.unload != nil && !dbPT.preload {
		return
	}
	if dbPT.unload == nil {
		dbPT.unload = make(chan struct{})
	}
	dbPT.wg.Add(1)
	go dbPT.reportLoad()
}

func (dbPT *DBPTInfo) SetDoingOff(df bool) {
	dbPT.doingOff = df
}

func (dbPT *DBPTInfo) reportLoad() {
	t := time.NewTicker(reportLoadFrequency)
	defer func() {
		dbPT.wg.Done()
		t.Stop()
		dbPT.logger.Info("dbpt reportLoad stopped", zap.Uint32("ptid", dbPT.id))
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

				seriesCount := dbPT.shards[shardID].GetSeriesCount()

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
				rpStats[len(rpStats)-1].ShardStats.ShardSize = proto.Uint64(dbPT.shards[shardID].GetRowCount())
				rpStats[len(rpStats)-1].ShardStats.MaxTime = proto.Int64(dbPT.shards[shardID].GetMaxTime())
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

	if dbPT.offloading {
		return false
	}
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

	if !dbPT.offloading {
		return
	}
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
	if dbPT.offloading || dbPT.preload {
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

func (dbPT *DBPTInfo) Shards() map[uint64]Shard {
	return dbPT.shards
}

// SetShards only used for mock test
func (dbPT *DBPTInfo) SetShards(shards map[uint64]Shard) {
	dbPT.shards = shards
}

// SetDelIndexBuilder only used for mock test
func (dbPT *DBPTInfo) SetDelIndexBuilder(delIndexBuilder map[string]*tsi.IndexBuilder) {
	dbPT.delIndexBuilderMap = delIndexBuilder
}

// SetNode only used for mock test
func (dbPT *DBPTInfo) SetNode(node raftNodeRequest) {
	dbPT.node = node
}

// SetDatabase only used for mock test
func (dbPT *DBPTInfo) SetDatabase(name string) {
	dbPT.database = name
}

// SetLockPath only used for mock test
func (dbPT *DBPTInfo) SetLockPath(lockPath *string) {
	dbPT.lockPath = lockPath
}

// SetPath only used for mock test
func (dbPT *DBPTInfo) SetPath(path string) {
	dbPT.path = path
}

// SetWalPath only used for mock test
func (dbPT *DBPTInfo) SetWalPath(walPath string) {
	dbPT.walPath = walPath
}

// SetIndexBuilder only used for mock test
func (dbPT *DBPTInfo) SetIndexBuilder(indexBuilder map[uint64]*tsi.IndexBuilder) {
	dbPT.indexBuilder = indexBuilder
}

func (dbPT *DBPTInfo) GetDelIndexBuilderByRp(rp string) *tsi.IndexBuilder {
	return dbPT.delIndexBuilderMap[rp]
}

func parseIndexDir(indexDirName string) (uint64, *meta.TimeRangeInfo, error) {
	indexDirName = strings.TrimRight(indexDirName, "/")
	indexDir := strings.Split(indexDirName, pathSeparator)
	if len(indexDir) != 3 {
		return 0, nil, errno.NewError(errno.InvalidDataDir)
	}

	indexID, err := strconv.ParseUint(indexDir[0], 10, 64)
	if err != nil {
		return 0, nil, errno.NewError(errno.InvalidDataDir)
	}

	startT, err := strconv.ParseInt(indexDir[1], 10, 64)
	if err != nil {
		return 0, nil, errno.NewError(errno.InvalidDataDir)
	}

	endT, err := strconv.ParseInt(indexDir[2], 10, 64)
	if err != nil {
		return 0, nil, errno.NewError(errno.InvalidDataDir)
	}

	startTime := meta.UnmarshalTime(startT)
	endTime := meta.UnmarshalTime(endT)
	return indexID, &meta.TimeRangeInfo{StartTime: startTime, EndTime: endTime}, nil
}

func (dbPT *DBPTInfo) loadShards(opId uint64, rp string, durationInfos map[uint64]*meta.ShardDurationInfo, loadStat int, client metaclient.MetaClient, engineType config.EngineType) error {
	if loadStat != immutable.PRELOAD {
		err := dbPT.OpenIndexes(opId, rp, engineType, client)
		if err != nil {
			return err
		}
	}
	return dbPT.OpenShards(opId, rp, durationInfos, loadStat, client)
}

func (dbPT *DBPTInfo) OpenIndexes(opId uint64, rp string, engineType config.EngineType, client metaclient.MetaClient) error {
	indexBasePath := path.Join(dbPT.path, rp, config.IndexFileDirectory)
	remoteIndexDirNames, remoteIndexPath, remoteErr := dbPT.getRemoteIndexPaths(indexBasePath, client, rp)
	if remoteErr != nil {
		dbPT.logger.Error("load remote index error.", zap.Error(remoteErr))
	}
	localIndexDirs, err := fileops.ReadDir(indexBasePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	// ignore index that has been transferred to cold
	for i := len(localIndexDirs) - 1; i >= 0; i-- {
		currentDir := localIndexDirs[i]
		for _, remoteIndexDir := range remoteIndexDirNames {
			if currentDir.Name() == remoteIndexDir {
				localIndexDirs = append(localIndexDirs[:i], localIndexDirs[i+1:]...)
				break
			}
		}
	}

	resC := make(chan *res, len(localIndexDirs)+len(remoteIndexDirNames))
	n := 0

	for _, remoteIndexDirName := range remoteIndexDirNames {
		n++
		openShardsLimit <- struct{}{}
		go dbPT.openIndex(opId, remoteIndexPath, remoteIndexDirName, rp, engineType, resC, util.Cold, client)
	}

	for indexIdx := range localIndexDirs {
		if !localIndexDirs[indexIdx].IsDir() {
			dbPT.logger.Warn("skip load index because it's not a dir", zap.String("dir", localIndexDirs[indexIdx].Name()))
			continue
		}
		n++

		openShardsLimit <- struct{}{}
		indexDirName := localIndexDirs[indexIdx].Name()
		go dbPT.openIndex(opId, indexBasePath, indexDirName, rp, engineType, resC, util.Hot, client)
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
		if res.i.GetIndexID() == DelIndexBuilderId {
			dbPT.delIndexBuilderMap[rp] = res.i
		} else {
			dbPT.indexBuilder[res.i.GetIndexID()] = res.i
		}
		dbPT.mu.Unlock()
	}
	close(resC)

	if err != nil {
		return err
	}

	if config.IsLogKeeper() {
		return nil
	}

	timeRangeInfo := &meta.ShardTimeRangeInfo{
		ShardDuration: &meta.ShardDurationInfo{
			DurationInfo: meta.DurationDescriptor{Duration: time.Second}},
		OwnerIndex: meta.IndexDescriptor{IndexID: DelIndexBuilderId},
	}
	dbPT.mu.Lock()
	defer dbPT.mu.Unlock()
	if _, _, _, _, err = dbPT.NewMergeSetIndex(rp, timeRangeInfo, client, engineType); err == nil {
		err = SetDelMergeSetForEachMergeSet(dbPT, rp)
	}

	return err
}

func SetDelMergeSetForEachMergeSet(dbPT *DBPTInfo, rp string) error {
	err := errors.New("delMergeSet must be *tsi.MergeSetIndex")
	if delMergeSet, ok := dbPT.GetDelIndexBuilderByRp(rp).GetPrimaryIndex().(*tsi.MergeSetIndex); ok {
		if err = delMergeSet.LoadDeletedTSIDs(); err != nil {
			return err
		}
		for _, v := range dbPT.indexBuilder {
			if curMerge, ok := v.GetPrimaryIndex().(*tsi.MergeSetIndex); ok {
				if curMerge.RpName() == rp {
					curMerge.SetDeleteMergeSet(delMergeSet)
				}
			} else {
				return errors.New("curMerge must be *tsi.MergeSetIndex")
			}
		}
	}
	return err
}

func (dbPT *DBPTInfo) NewMergeSetIndex(rp string, timeRangeInfo *meta.ShardTimeRangeInfo, client metaclient.MetaClient, engineType config.EngineType) (uint64, string, fileops.FileLockOption, *tsi.IndexBuilder, error) {
	var err error
	rpPath := path.Join(dbPT.path, rp)

	indexID := timeRangeInfo.OwnerIndex.IndexID
	lock := fileops.FileLockOption(*dbPT.lockPath)
	indexBuilder, ok := dbPT.indexBuilder[indexID]
	if !ok && !config.IsLogKeeper() {
		indexPath := strconv.FormatUint(indexID, 10) + pathSeparator + strconv.Itoa(int(meta.MarshalTime(timeRangeInfo.OwnerIndex.TimeRange.StartTime))) +
			pathSeparator + strconv.Itoa(int(meta.MarshalTime(timeRangeInfo.OwnerIndex.TimeRange.EndTime)))
		iPath := path.Join(rpPath, config.IndexFileDirectory, indexPath)
		ObsOptions, _ := client.DatabaseOption(dbPT.database)
		if ObsOptions != nil && ObsOptions.Enabled {
			iPath = fileops.GetRemoteDataPath(ObsOptions, iPath)
		}

		if err := fileops.MkdirAll(iPath, DefaultDirPerm, lock); err != nil {
			return indexID, rpPath, lock, indexBuilder, err
		}

		indexIdent := &meta.IndexIdentifier{OwnerDb: dbPT.database, OwnerPt: dbPT.id, Policy: rp}
		indexIdent.Index = &meta.IndexDescriptor{IndexID: indexID,
			IndexGroupID: timeRangeInfo.OwnerIndex.IndexGroupID, TimeRange: timeRangeInfo.OwnerIndex.TimeRange}

		opts := new(tsi.Options).
			Ident(indexIdent).
			Path(iPath).
			IndexType(index.MergeSet).
			EngineType(engineType).
			StartTime(timeRangeInfo.OwnerIndex.TimeRange.StartTime).
			EndTime(timeRangeInfo.OwnerIndex.TimeRange.EndTime).
			Duration(timeRangeInfo.ShardDuration.DurationInfo.Duration).
			CacheDuration(timeRangeInfo.OwnerIndex.TimeRange.EndTime.Sub(timeRangeInfo.OwnerIndex.TimeRange.StartTime)).
			LogicalClock(dbPT.logicClock).
			SequenceId(&dbPT.sequenceID).
			Lock(dbPT.lockPath).
			MergeDuration(timeRangeInfo.ShardDuration.DurationInfo.MergeDuration).
			ObsOpt(dbPT.dbObsOptions)

		// init indexBuilder and default indexRelation
		indexBuilder = tsi.NewIndexBuilder(opts)
		if dbPT.dbObsOptions == nil {
			dbPT.dbObsOptions, _ = client.DatabaseOption(dbPT.database)
		}
		indexBuilder.ObsOpt = dbPT.dbObsOptions
		indexBuilder.EnableTagArray = dbPT.enableTagArray
		primaryIndex, _ := tsi.NewIndex(opts)
		primaryIndex.SetIndexBuilder(indexBuilder)
		indexRelation, _ := tsi.NewIndexRelation(opts, primaryIndex, indexBuilder)
		if indexBuilder.GetIndexID() == DelIndexBuilderId {
			dbPT.delIndexBuilderMap[rp] = indexBuilder
			dbPT.delIndexBuilderMap[rp].Relations[uint32(index.MergeSet)] = indexRelation
		} else {
			dbPT.indexBuilder[indexID] = indexBuilder
			dbPT.indexBuilder[indexID].Relations[uint32(index.MergeSet)] = indexRelation
		}
		err = indexBuilder.Open()
	}

	return indexID, rpPath, lock, indexBuilder, err
}

func (dbPT *DBPTInfo) getRemoteIndexPaths(indexPath string, client metaclient.MetaClient, rp string) ([]string, string, error) {
	if dbPT.dbObsOptions == nil {
		return nil, "", nil
	}
	noPrefixPath := indexPath[len(obs.GetPrefixDataPath()):]
	obsBasePath := filepath.Join(dbPT.dbObsOptions.BasePath, noPrefixPath)
	completeObsPath := fmt.Sprintf("%s%s/%s/%s/%s/%s", fileops.ObsPrefix, dbPT.dbObsOptions.Endpoint, dbPT.dbObsOptions.Ak, dbPT.dbObsOptions.Sk, dbPT.dbObsOptions.BucketName, obsBasePath)

	indexDirs, err := fileops.ReadDir(completeObsPath)
	if err != nil {
		return nil, "", err
	}

	var subDirs []fs.FileInfo
	for _, indexDir := range indexDirs {
		if indexDir.IsDir() {
			subDirs = append(subDirs, indexDir)
		}
	}
	subPaths := fileops.GetSubDirNamesForObsReadDirs(subDirs, obsBasePath)
	res := changeSubPathToColdOne(subPaths, dbPT, client, rp)
	return res, completeObsPath, nil
}

func changeSubPathToColdOne(subPaths []string, dbPT *DBPTInfo, client metaclient.MetaClient, rp string) []string {
	var res []string
	for _, subPath := range subPaths {
		indexID, _, err := parseIndexDir(subPath)
		if err != nil {
			res = append(res, subPath)
			continue
		}
		cleared, err := dbPT.isIndexCleared(indexID, client, rp)
		if err != nil {
			res = append(res, subPath)
			continue
		}
		if cleared {
			noClearIndexId, err := client.GetNoClearIndexId(indexID, dbPT.database, rp)
			if err != nil {
				res = append(res, subPath)
				continue
			}
			splits := strings.Split(subPath, pathSeparator)
			if len(splits) < 3 {
				continue
			}
			res = append(res, strconv.FormatUint(noClearIndexId, 10)+pathSeparator+splits[1]+pathSeparator+splits[2])
		} else {
			res = append(res, subPath)
		}
	}
	return res
}

func containOtherIndexes(dirName string) bool {
	if dirName == "mergeset" || dirName == "kv" {
		return false
	}
	return true
}

func (dbPT *DBPTInfo) isIndexCleared(indexID uint64, client metaclient.MetaClient, rp string) (bool, error) {
	database, err := client.Database(dbPT.database)
	if err != nil {
		return false, err
	}
	groups := database.RetentionPolicies[rp].IndexGroups
	for _, group := range groups {
		for _, indexInfo := range group.Indexes {
			if indexInfo.ID == indexID && indexInfo.Tier == util.Cleared {
				return true, nil
			}
		}
	}
	return false, nil
}

func parseShardDir(shardDirName string) (uint64, uint64, *meta.TimeRangeInfo, error) {
	shardDirName = strings.TrimRight(shardDirName, "/")
	shardDir := strings.Split(shardDirName, pathSeparator)
	if len(shardDir) != 4 {
		return 0, 0, nil, errno.NewError(errno.InvalidDataDir)
	}
	shardID, err := strconv.ParseUint(shardDir[0], 10, 64)
	if err != nil {
		return 0, 0, nil, errno.NewError(errno.InvalidDataDir)
	}
	indexID, err := strconv.ParseUint(shardDir[3], 10, 64)
	if err != nil {
		return 0, 0, nil, errno.NewError(errno.InvalidDataDir)
	}

	startTime, err := strconv.ParseInt(shardDir[1], 10, 64)
	if err != nil {
		return 0, 0, nil, errno.NewError(errno.InvalidDataDir)
	}
	endTime, err := strconv.ParseInt(shardDir[2], 10, 64)
	if err != nil {
		return 0, 0, nil, errno.NewError(errno.InvalidDataDir)
	}
	tr := &meta.TimeRangeInfo{StartTime: meta.UnmarshalTime(startTime), EndTime: meta.UnmarshalTime(endTime)}
	return shardID, indexID, tr, nil
}

func (dbPT *DBPTInfo) thermalShards(client metaclient.MetaClient) map[uint64]struct{} {
	if dbPT.opt.LazyLoadShardEnable {
		start := dbPT.opt.ThermalShardStartDuration
		end := dbPT.opt.ThermalShardEndDuration
		return client.ThermalShards(dbPT.database, start, end)
	}
	return nil
}

type res struct {
	s   Shard
	i   *tsi.IndexBuilder
	err error
}

func (dbPT *DBPTInfo) OpenShards(opId uint64, rp string, durationInfos map[uint64]*meta.ShardDurationInfo, loadStat int, client metaclient.MetaClient) error {
	rpPath := path.Join(dbPT.path, rp)
	shardDirs, err := fileops.ReadDir(rpPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(shardDirs) == 0 {
		return nil
	}

	thermalShards := dbPT.thermalShards(client)

	resC := make(chan *res, len(shardDirs)-1)
	n := 0
	dirDepth := immutable.DirDepth(shardDirs[0].Name())
	for shIdx := range shardDirs {
		if strings.TrimSuffix(shardDirs[shIdx].Name(), "/") == config.IndexFileDirectory {
			continue
		}
		if dirDepth != 1 && immutable.DirDepth(shardDirs[shIdx].Name()) != dirDepth+1 {
			continue
		}
		if filepath.Base(filepath.Clean(shardDirs[shIdx].Name())) == config.IndexFileDirectory {
			continue
		}
		n++
		openShardsLimit <- struct{}{}
		go dbPT.openShard(opId, thermalShards, shardDirs[shIdx].Name(), rp, durationInfos, resC, loadStat, client)
	}

	err = dbPT.ptReceiveShard(resC, n, rp)
	close(resC)
	return err
}

func (dbPT *DBPTInfo) openIndex(opId uint64, indexPath, indexDirName, rp string, engineType config.EngineType, resC chan<- *res, tier uint64, client metaclient.MetaClient) {
	defer func() {
		openShardsLimit.Release()
	}()
	if config.IsLogKeeper() {
		resC <- &res{}
		return
	}
	indexDirName = filepath.Base(filepath.Clean(indexDirName))
	indexID, tr, err := parseIndexDir(indexDirName)
	if err != nil {
		dbPT.logger.Error("parse index dir failed", zap.Error(err))
		resC <- &res{}
		return
	}
	ipath := fileops.Join(indexPath, indexDirName)
	// FIXME reload index

	// todo:is it necessary to mkdir again??
	lock := fileops.FileLockOption(*dbPT.lockPath)
	if err := fileops.MkdirAll(ipath, DefaultDirPerm, lock); err != nil {
		resC <- &res{err: err}
		return
	}
	var allIndexDirNames []string
	allIndexDirs, err := fileops.ReadDir(ipath)
	if err != nil {
		resC <- &res{err: err}
		return
	}

	if tier == util.Cold || tier == util.Cleared {
		var remoteObsIndexDirs []fs.FileInfo
		for _, remoteObsIndexDir := range allIndexDirs {
			if remoteObsIndexDir.IsDir() {
				remoteObsIndexDirs = append(remoteObsIndexDirs, remoteObsIndexDir)
			}
		}
		obsPrefixPath := fmt.Sprintf("%s%s/%s/%s/%s", fileops.ObsPrefix, dbPT.dbObsOptions.Endpoint, dbPT.dbObsOptions.Ak, dbPT.dbObsOptions.Sk, dbPT.dbObsOptions.BucketName)
		allIndexDirNames = fileops.GetSubDirNamesForObsReadDirs(remoteObsIndexDirs, ipath[len(obsPrefixPath):])
	} else {
		for i := range allIndexDirs {
			allIndexDirNames = append(allIndexDirNames, allIndexDirs[i].Name())
		}
	}

	indexIdent := &meta.IndexIdentifier{OwnerDb: dbPT.database, OwnerPt: dbPT.id, Policy: rp}
	indexIdent.Index = &meta.IndexDescriptor{IndexID: indexID, TimeRange: *tr}
	opts := new(tsi.Options).
		OpId(opId).
		Ident(indexIdent).
		Path(ipath).
		IndexType(index.MergeSet).
		EngineType(engineType).
		StartTime(tr.StartTime).
		EndTime(tr.EndTime).
		LogicalClock(dbPT.logicClock).
		SequenceId(&dbPT.sequenceID).
		Lock(dbPT.lockPath).
		ObsOpt(dbPT.dbObsOptions)

	dbPT.mu.Lock()
	// init indexBuilder and default indexRelation
	indexBuilder := tsi.NewIndexBuilder(opts)
	indexBuilder.EnableTagArray = dbPT.enableTagArray
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
	indexBuilder.Relations[uint32(index.MergeSet)] = indexRelation
	indexBuilder.Tier = tier
	indexBuilder.IndexColdDuration = dbPT.getIndexColdDuration(rp, client)
	indexBuilder.ObsOpt = dbPT.dbObsOptions

	// init other indexRelations if exist
	for idx := range allIndexDirNames {
		if containOtherIndexes(strings.TrimSuffix(allIndexDirNames[idx], "/")) {
			idxType, _ := index.GetIndexTypeByName(allIndexDirNames[idx])
			opts := new(tsi.Options).
				Ident(indexIdent).
				Path(ipath).
				IndexType(idxType).
				EndTime(tr.EndTime).
				Lock(dbPT.lockPath)
			indexRelation, _ := tsi.NewIndexRelation(opts, primaryIndex, indexBuilder)
			indexBuilder.Relations[uint32(idxType)] = indexRelation
		}
	}

	dbPT.mu.Unlock()

	if err != nil {
		resC <- &res{err: err}
		return
	}
	resC <- &res{i: indexBuilder}
}

func (dbPT *DBPTInfo) getIndexColdDuration(rp string, client metaclient.MetaClient) time.Duration {
	dbs := client.Databases()
	db, dbExists := dbs[dbPT.database]
	if !dbExists {
		return 0
	}
	dbrp, dbrpExists := db.RetentionPolicies[rp]
	if !dbrpExists {
		return 0
	}
	return dbrp.IndexColdDuration
}

func (dbPT *DBPTInfo) openShard(opId uint64, thermalShards map[uint64]struct{}, shardDirName, rp string, durationInfos map[uint64]*meta.ShardDurationInfo,
	resC chan *res, loadStat int, client metaclient.MetaClient) {
	defer func() {
		openShardsLimit.Release()
	}()
	shardDirName = filepath.Base(filepath.Clean(shardDirName))
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
	rpPath := path.Join(dbPT.path, rp)
	shardPath := path.Join(rpPath, shardDirName)
	walPath := path.Join(dbPT.walPath, rp)
	shardWalPath := path.Join(walPath, shardDirName)

	var sh *shard
	if loadStat == immutable.LOAD {
		sh, err = dbPT.loadProcess(opId, thermalShards, shardPath, shardWalPath, indexID, shardId, durationInfos, tr, client)
	} else {
		sh, err = dbPT.preloadProcess(opId, thermalShards, shardPath, shardWalPath, shardId, durationInfos, tr, client)
	}
	sendShardResult(sh, err, resC)
}

func sendShardResult(sh *shard, err error, resC chan *res) {
	if err != nil {
		resC <- &res{err: err}
	} else {
		resC <- &res{s: sh}
	}
}

func (dbPT *DBPTInfo) preloadProcess(opId uint64, thermalShards map[uint64]struct{}, shardPath, shardWalPath string, shardId uint64, durationInfos map[uint64]*meta.ShardDurationInfo,
	tr *meta.TimeRangeInfo, client metaclient.MetaClient) (*shard, error) {

	engineType := config.EngineType(durationInfos[shardId].Ident.EngineType)

	sh := NewShard(shardPath, shardWalPath, dbPT.lockPath, &durationInfos[shardId].Ident, &durationInfos[shardId].DurationInfo, tr, dbPT.opt, engineType, dbPT.fileInfos)
	sh.opId = opId

	start := time.Now()
	statistics.ShardTaskInit(sh.opId, sh.GetIdent().OwnerDb, sh.GetIdent().OwnerPt, sh.GetRPName(), sh.GetID())

	if _, ok := thermalShards[sh.ident.ShardID]; ok || !dbPT.opt.LazyLoadShardEnable {
		sh.mu.Lock()
		if err := sh.Open(client); err != nil {
			sh.mu.Unlock()
			statistics.ShardStepDuration(sh.GetID(), sh.opId, "ShardOpenErr", time.Since(start).Nanoseconds(), true)
			_ = sh.Close()
			return nil, err
		}
		sh.mu.Unlock()
		statistics.ShardStepDuration(sh.GetID(), sh.opId, "shardOpenDone", 0, true)
	} else {
		statistics.ShardStepDuration(sh.GetID(), sh.opId, "shardSkipOpen", 0, true)
		dbPT.logger.Info("skipping open shard for preload", zap.String("path", shardPath))
	}
	return sh, nil
}

func (dbPT *DBPTInfo) loadProcess(opId uint64, thermalShards map[uint64]struct{}, shardPath, shardWalPath string, indexID, shardId uint64,
	durationInfos map[uint64]*meta.ShardDurationInfo, tr *meta.TimeRangeInfo, client metaclient.MetaClient) (*shard, error) {
	i, err := dbPT.getShardIndex(indexID, durationInfos[shardId].DurationInfo.Duration, durationInfos[shardId].DurationInfo.MergeDuration)
	if err != nil {
		logger.GetLogger().Warn("failed to get shard index", zap.Error(err))
		return nil, nil
	}
	dbPT.mu.RLock()
	sh, ok := dbPT.shards[shardId].(*shard)
	dbPT.mu.RUnlock()
	if !ok {
		engineType := config.EngineType(durationInfos[shardId].Ident.EngineType)
		sh = NewShard(shardPath, shardWalPath, dbPT.lockPath, &durationInfos[shardId].Ident, &durationInfos[shardId].DurationInfo, tr, dbPT.opt, engineType, dbPT.fileInfos)
		sh.opId = opId
	}
	if sh.indexBuilder != nil && sh.downSampleEnabled() {
		return sh, nil
	}
	statistics.ShardTaskInit(sh.opId, sh.GetIdent().OwnerDb, sh.GetIdent().OwnerPt, sh.GetRPName(), sh.GetID())
	defer func() {
		if err != nil {
			_ = sh.Close()
			return
		}
	}()
	start := time.Now()
	sh.SetIndexBuilder(i)
	sh.SetLockPath(dbPT.lockPath)
	if err = sh.NewShardKeyIdx(durationInfos[shardId].Ident.ShardType, shardPath, dbPT.lockPath); err != nil {
		statistics.ShardStepDuration(sh.GetID(), sh.opId, "ShardOpenErr", time.Since(start).Nanoseconds(), true)
		return nil, err
	}

	mstsInfo, err := client.GetMeasurements(&influxql.Measurement{Database: sh.ident.OwnerDb, RetentionPolicy: sh.ident.Policy})
	if err != nil {
		return nil, err
	}
	if sh.engineType == config.COLUMNSTORE {
		for _, mstInfo := range mstsInfo {
			if mstInfo.EngineType != config.COLUMNSTORE {
				continue
			}
			d := NewDetachedMetaInfo()
			if immutable.GetDetachedFlushEnabled() {
				err = checkAndTruncateDetachedFiles(d, mstInfo, sh)
				if err != nil {
					return nil, err
				}
			}
			ident := colstore.NewMeasurementIdent(sh.ident.OwnerDb, sh.ident.Policy)
			ident.SetSafeName(mstInfo.Name)
			colstore.MstManagerIns().Add(ident, mstInfo)
		}
		sh.pkIndexReader = sparseindex.NewPKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)
		sh.skIndexReader = sparseindex.NewSKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

		// Load the row count of each measurement.
		for _, mst := range mstsInfo {
			mstPath := path.Join(sh.dataPath, immutable.ColumnStoreDirName, mst.Name)
			_, err := os.Stat(mstPath)
			if os.IsNotExist(err) {
				continue
			}
			rowCount, err := mutable.LoadMstRowCount(path.Join(mstPath, immutable.CountBinFile))
			if err != nil {
				sh.log.Error("load row count failed", zap.Uint64("shard", sh.GetID()), zap.String("mst", mst.OriginName()), zap.Error(err))
			}
			rowCountPtr := int64(rowCount)
			sh.msRowCount.Store(mst.Name, &rowCountPtr)
		}
	}
	if len(mstsInfo) > 0 {
		sh.SetObsOption(mstsInfo[0].ObsOptions)
	}

	if _, ok = thermalShards[sh.ident.ShardID]; ok || !dbPT.opt.LazyLoadShardEnable {
		if err = sh.OpenAndEnable(client); err != nil {
			statistics.ShardStepDuration(sh.GetID(), sh.opId, "ShardOpenErr", time.Since(start).Nanoseconds(), true)
			return nil, err
		}
		statistics.ShardStepDuration(sh.GetID(), sh.opId, "shardOpenAndEnableDone", 0, true)
	} else {
		statistics.ShardStepDuration(sh.GetID(), sh.opId, "shardSkipOpen", 0, true)
		dbPT.logger.Info("skipping open shard for load", zap.String("path", shardPath))
	}

	return sh, nil
}

func checkAndTruncateDetachedFiles(d *DetachedMetaInfo, mstInfo *meta.MeasurementInfo, sh *shard) error {
	d.obsOpt = mstInfo.ObsOptions
	err := d.checkAndTruncateDetachedFiles(sh.filesPath, mstInfo.Name, mstInfo.IndexRelation.GetBloomFilterColumns(), logstore.IsFullTextIdx(&mstInfo.IndexRelation))
	if err != nil {
		return err
	}

	setAccumulateMetaIndex(d, mstInfo, sh)
	return nil
}

func setAccumulateMetaIndex(d *DetachedMetaInfo, mstInfo *meta.MeasurementInfo, sh *shard) {
	//update accumulate metaIndex after check detached files
	aMetaIndex := &immutable.AccumulateMetaIndex{}
	aMetaIndex.SetAccumulateMetaIndex(d.lastPkMetaOff+d.lastPkMetaSize, d.lastPkMetaEndBlockId,
		d.lastChunkMetaOff+int64(d.lastChunkMetaSize), d.lastMetaIdxOff+int64(d.lastMetaIdxSize))
	sh.storage.SetAccumulateMetaIndex(mstInfo.Name, aMetaIndex)
	sh.immTables.SetAccumulateMetaIndex(mstInfo.Name, aMetaIndex)
}

func (dbPT *DBPTInfo) getShardIndex(indexID uint64, duration time.Duration, mergeDuration time.Duration) (*tsi.IndexBuilder, error) {
	if config.IsLogKeeper() {
		return nil, nil
	}
	dbPT.mu.RLock()
	indexBuilder, ok := dbPT.indexBuilder[indexID]
	dbPT.mu.RUnlock()
	if !ok {
		return nil, errno.NewError(errno.IndexNotFound, dbPT.database, dbPT.id, indexID)
	}
	indexBuilder.SetDuration(duration)
	indexBuilder.SetMergeDuration(mergeDuration)
	return indexBuilder, nil
}

func (dbPT *DBPTInfo) getIndexBuilder(indexID uint64) (*tsi.IndexBuilder, bool) {
	dbPT.mu.RLock()
	indexBuilder, ok := dbPT.indexBuilder[indexID]
	dbPT.mu.RUnlock()
	return indexBuilder, ok
}

func (dbPT *DBPTInfo) ptReceiveShard(resC chan *res, n int, rp string) error {
	var err error
	for i := 0; i < n; i++ {
		r := <-resC
		if r.err != nil {
			err = r.err
			continue
		}
		if immutable.IsInterfaceNil(r.s) {
			continue
		}
		dbPT.mu.Lock()
		if dbPT.newestRpShard[rp] == 0 || dbPT.newestRpShard[rp] < r.s.GetID() {
			dbPT.newestRpShard[rp] = r.s.GetID()
		}
		dbPT.shards[r.s.GetID()] = r.s
		dbPT.mu.Unlock()
	}
	return err
}

func (dbPT *DBPTInfo) SetOption(opt EngineOptions) {
	dbPT.opt = opt
}

func (dbPT *DBPTInfo) SetParams(preload bool, lockPath *string, enableTagArray bool) {
	dbPT.unload = make(chan struct{})
	dbPT.preload = preload
	dbPT.lockPath = lockPath
	dbPT.enableTagArray = enableTagArray
}

func (dbPT *DBPTInfo) NewShard(rp string, shardID uint64, timeRangeInfo *meta.ShardTimeRangeInfo, client metaclient.MetaClient, mstInfo *meta.MeasurementInfo) (Shard, error) {

	walPath := path.Join(dbPT.walPath, rp)

	indexID, rpPath, lock, indexBuilder, err := dbPT.NewMergeSetIndex(rp, timeRangeInfo, client, mstInfo.EngineType)
	if err != nil {
		return nil, err
	}
	id := strconv.Itoa(int(shardID))
	shardPath := id + pathSeparator + strconv.Itoa(int(meta.MarshalTime(timeRangeInfo.TimeRange.StartTime))) +
		pathSeparator + strconv.Itoa(int(meta.MarshalTime(timeRangeInfo.TimeRange.EndTime))) +
		pathSeparator + strconv.Itoa(int(indexID))
	dataPath := path.Join(rpPath, shardPath)
	walPath = path.Join(walPath, shardPath)
	if mstInfo.ObsOptions != nil && mstInfo.ObsOptions.Enabled {
		dataPath = fileops.GetRemoteDataPath(mstInfo.ObsOptions, dataPath)
	}
	if err = fileops.MkdirAll(dataPath, DefaultDirPerm, lock); err != nil {
		return nil, err
	}
	shardIdent := &meta.ShardIdentifier{ShardID: shardID, Policy: rp, OwnerDb: dbPT.database, OwnerPt: dbPT.id}
	sh := NewShard(dataPath, walPath, dbPT.lockPath, shardIdent, &timeRangeInfo.ShardDuration.DurationInfo, &timeRangeInfo.TimeRange, dbPT.opt, mstInfo.EngineType, dbPT.fileInfos)
	sh.obsOpt = mstInfo.ObsOptions
	sh.SetIndexBuilder(indexBuilder)

	err = sh.NewShardKeyIdx(timeRangeInfo.ShardType, dataPath, dbPT.lockPath)
	if err != nil {
		return nil, err
	}
	if !dbPT.bgrEnabled {
		sh.mu.Lock()
		err = sh.Open(client)
		sh.mu.Unlock()
	} else {
		err = sh.OpenAndEnable(client)
	}
	if err != nil {
		_ = sh.Close()
		return nil, err
	}
	if sh.engineType == config.COLUMNSTORE {
		sh.pkIndexReader = sparseindex.NewPKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)
		sh.skIndexReader = sparseindex.NewSKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)
	}
	return sh, err
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

func (dbPT *DBPTInfo) ShardNoLock(id uint64) Shard {
	sh, ok := dbPT.shards[id]
	if !ok {
		return nil
	}

	return sh
}

func (dbPT *DBPTInfo) IndexNoLock(id uint64) *tsi.IndexBuilder {
	ib, ok := dbPT.indexBuilder[id]
	if !ok {
		return nil
	}

	return ib
}

func (dbPT *DBPTInfo) closeDBPt() error {
	dbPT.mu.Lock()

	dbPT.closed.Close()
	select {
	case <-dbPT.unload:
	default:
		close(dbPT.unload)
	}

	start := time.Now()
	dbPT.logger.Info("start close dbpt", zap.String("db", dbPT.database), zap.Uint32("pt", dbPT.id))
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
	for rp, iBuild := range dbPT.delIndexBuilderMap {
		if err := iBuild.Close(); err != nil {
			dbPT.mu.Unlock()
			dbPT.logger.Error("close index fail", zap.String("rp", rp), zap.Error(err))
			return err
		}
	}
	if dbPT.node != nil {
		log.Error("close dbpt trigger raft node stop!!!")
		dbPT.node.Stop()
	}
	dbPT.mu.Unlock()
	d = time.Since(start)
	dbPT.logger.Info("close dbpt success", zap.String("db", dbPT.database), zap.Uint32("pt", dbPT.id), zap.Duration("time used", d))

	dbPT.wg.Wait()

	return nil
}

func (dbPT *DBPTInfo) seriesCardinality(measurements [][]byte, measurementCardinalityInfos []meta.MeasurementCardinalityInfo,
	tr influxql.TimeRange) ([]meta.MeasurementCardinalityInfo, error) {
	for _, indexBuilder := range dbPT.indexBuilder {
		if !indexBuilder.Overlaps(tr) {
			continue
		}
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

func (dbPT *DBPTInfo) seriesCardinalityWithCondition(measurements [][]byte, condition influxql.Expr,
	measurementCardinalityInfos []meta.MeasurementCardinalityInfo, tr influxql.TimeRange) ([]meta.MeasurementCardinalityInfo, error) {
	for i := range measurements {
		var cardinalityInfo []meta.CardinalityInfo
		for _, indexBuilder := range dbPT.indexBuilder {
			if !indexBuilder.Overlaps(tr) {
				continue
			}
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
			cardinalityInfo = append(cardinalityInfo, meta.CardinalityInfo{
				TimeRange:   indexBuilder.Ident().Index.TimeRange,
				Cardinality: count})
		}
		if len(cardinalityInfo) != 0 {
			measurementCardinalityInfos = append(measurementCardinalityInfos, meta.MeasurementCardinalityInfo{
				Name:             string(measurements[i]),
				CardinalityInfos: cardinalityInfo,
			})
		}
	}
	return measurementCardinalityInfos, nil
}

func (dbPT *DBPTInfo) enableDBPtBgr() {
	dbPT.mu.Lock()
	defer dbPT.mu.Unlock()
	dbPT.bgrEnabled = true
}

func (dbPT *DBPTInfo) disableDBPtBgr() error {
	dbPT.mu.Lock()
	defer dbPT.mu.Unlock()
	if len(dbPT.pendingShardDeletes) > 0 {
		return errno.NewError(errno.ShardIsBeingDelete)
	}
	dbPT.bgrEnabled = false
	return nil
}

func (dbPT *DBPTInfo) setEnableShardsBgr(enabled bool) {
	shardIds := dbPT.ShardIds(nil)
	dbPT.logger.Info("set shard compaction merge and downsample tasks", zap.Bool("enabled", enabled), zap.Int("shards", len(shardIds)))

	for _, id := range shardIds {
		dbPT.mu.RLock()
		sh, ok := dbPT.shards[id]
		dbPT.mu.RUnlock()
		if !ok {
			continue
		}
		if enabled {
			sh.EnableCompAndMerge()
			sh.EnableDownSample()
		} else {
			sh.DisableCompAndMerge()
			sh.DisableDownSample()
		}
	}
}

func (dbPT *DBPTInfo) ShardIds(tr *influxql.TimeRange) []uint64 {
	var shardIds []uint64
	dbPT.mu.RLock()
	for id, sh := range dbPT.shards {
		if tr == nil || sh.Intersect(tr) {
			shardIds = append(shardIds, id)
		}
	}
	dbPT.mu.RUnlock()
	return shardIds
}

func (dbPT *DBPTInfo) walkShards(tr *influxql.TimeRange, callback func(sh Shard)) {
	shardIDs := dbPT.ShardIds(tr)
	for _, id := range shardIDs {
		dbPT.mu.RLock()
		sh, ok := dbPT.shards[id]
		dbPT.mu.RUnlock()
		if !ok {
			continue
		}

		callback(sh)
	}
}

func (dbpt *DBPTInfo) HasCoverShard(srcShardTimeRange *meta.ShardTimeRangeInfo, rp string, shardId uint64) bool {
	for _, sh := range dbpt.shards {
		if sh.GetDuration().MergeDuration > 0 && sh.GetRPName() == rp {
			if sh.GetStartTime().UnixNano() <= srcShardTimeRange.TimeRange.StartTime.UnixNano() &&
				sh.GetEndTime().UnixNano() >= srcShardTimeRange.TimeRange.EndTime.UnixNano() {
				log.Info("HasCoverShard", zap.String("db", dbpt.database), zap.String("rp", rp), zap.Uint32("ptId", dbpt.id),
					zap.Uint64("srcShardId", shardId), zap.Time("srcStartTime", srcShardTimeRange.TimeRange.StartTime),
					zap.Time("srcEndTime", srcShardTimeRange.TimeRange.EndTime), zap.Uint64("dstShardId", sh.GetID()),
					zap.Time("dstStartTime", sh.GetStartTime()),
					zap.Time("dstEndTime", sh.GetEndTime()))
				return true
			}
		}
	}
	return false
}
