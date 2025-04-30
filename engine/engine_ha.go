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
	"path"
	"strconv"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/raftconn"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

func (e *Engine) PreOffload(opId uint64, db string, ptId uint32) error {
	if !e.trySetDbPtMigrating(db, ptId) {
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	defer e.clearDbPtMigrating(db, ptId)
	e.log.Info("prepare offload pt start", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
	dbPt, err := e.getPartition(db, ptId, true)
	if err != nil {
		if errno.Equal(err, errno.PtNotFound) || errno.Equal(err, errno.DatabaseNotFound) {
			return nil
		}
		return err
	}
	dbPt.mu.Lock()
	if dbPt.doingShardMoveN > 0 {
		dbPt.mu.Unlock()
		dbPt.unref()
		return errno.NewError(errno.PtIsDoingSomeShardMove)
	} else {
		dbPt.doingOff = true
	}
	dbPt.mu.Unlock()
	if err = dbPt.disableDBPtBgr(); err != nil {
		dbPt.unref()
		return err
	}
	start := time.Now()
	dbPt.setEnableShardsBgr(false)
	dbPt.unref()
	e.log.Info("prepare offload pt success", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Duration("time used", time.Since(start)))
	return nil
}

func (e *Engine) RollbackPreOffload(opId uint64, db string, ptId uint32) error {
	if !e.trySetDbPtMigrating(db, ptId) {
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	defer e.clearDbPtMigrating(db, ptId)
	start := time.Now()
	e.log.Info("rollback prepare offload pt start", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
	dbPt, err := e.getPartition(db, ptId, true)
	if err != nil {
		// src node reset
		if errno.Equal(err, errno.PtNotFound) || errno.Equal(err, errno.DatabaseNotFound) {
			return nil
		}
		return err
	}
	dbPt.enableDBPtBgr()
	dbPt.setEnableShardsBgr(true)
	dbPt.unref()
	dbPt.mu.Lock()
	if dbPt.doingShardMoveN > 0 {
		e.log.Error("doingShardMoveN > 0 when RollbackPreOffload", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
	}
	dbPt.doingOff = false
	dbPt.mu.Unlock()
	e.log.Info("rollback prepare offload pt success", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Duration("time used", time.Since(start)))
	return nil
}

func (e *Engine) PreAssign(opId uint64, db string, ptId uint32, durationInfos map[uint64]*meta2.ShardDurationInfo, dbBriefInfo *meta2.DatabaseBriefInfo, client metaclient.MetaClient) error {
	if !e.trySetDbPtMigrating(db, ptId) {
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	defer e.clearDbPtMigrating(db, ptId)
	if IsMemUsageExceeded() {
		return errno.NewError(errno.MemUsageExceeded, GetMemUsageLimit())
	}
	e.log.Info("prepare load pt start", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("opId", opId))
	_, err := e.getPartition(db, ptId, false)
	if err == nil {
		e.log.Info("prepare load pt already success", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("opId", opId))
		return nil
	}
	ptPath := path.Join(e.dataPath, config.DataDirectory, db, strconv.Itoa(int(ptId)))
	walPath := path.Join(e.walPath, config.WalDirectory, db, strconv.Itoa(int(ptId)))

	options, _ := e.metaClient.DatabaseOption(db)

	lockPath := ""
	dbPt := NewDBPTInfo(db, ptId, ptPath, walPath, e.loadCtx, e.fileInfos, options)
	dbPt.SetOption(e.engOpt)
	dbPt.SetParams(true, &lockPath, dbBriefInfo.EnableTagArray)
	start := time.Now()
	if err = e.loadDbPtShards(opId, dbPt, durationInfos, immutable.PRELOAD, client); err != nil {
		if rbErr := e.offloadDbPT(dbPt); rbErr != nil {
			e.log.Error("both preload pt and rollback failed", zap.Uint32("pt", ptId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Error(rbErr))
			panic(rbErr.Error())
		}
		e.log.Error("preload pt failed and rollback success", zap.Uint32("pt", ptId), zap.Uint64("opId", opId), zap.String("db", db), zap.Error(err))
		return err
	}

	e.mu.Lock()
	e.addDBPTInfo(dbPt)
	e.mu.Unlock()
	e.log.Info("prepare load pt success", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Duration("time used", time.Since(start)))
	return nil
}

func (e *Engine) Offload(opId uint64, db string, ptId uint32) error {
	if !e.trySetDbPtMigrating(db, ptId) {
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	defer e.clearDbPtMigrating(db, ptId)
	e.log.Info("offload pt start", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
	dbPt, err := e.getPartition(db, ptId, false)
	if err != nil {
		if errno.Equal(err, errno.PtNotFound) || errno.Equal(err, errno.DatabaseNotFound) {
			return nil
		}
		return err
	}
	start := time.Now()
	err = e.offloadDbPT(dbPt)
	if err != nil {
		e.log.Error("offload pt failed", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Duration("time used", time.Since(start)), zap.Error(err))
		return err
	}
	e.log.Info("offload pt success", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Duration("time used", time.Since(start)))
	return nil
}

func (e *Engine) Assign(opId uint64, nodeId uint64, db string, ptId uint32, ver uint64, durationInfos map[uint64]*meta2.ShardDurationInfo, dbBriefInfo *meta2.DatabaseBriefInfo, client metaclient.MetaClient, storage netstorage.StorageService) error {
	e.log.Info("[ASSIGN]engine start assign", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
	if !e.trySetDbPtMigrating(db, ptId) {
		e.log.Error("[ASSIGN]engine start assign failed", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	defer e.clearDbPtMigrating(db, ptId)
	e.SetMetaClient(client)
	if e.metaClient.IsSQLiteEnabled() && e.fileInfos == nil {
		e.fileInfos = make(chan []immutable.FileInfoExtend, MaxFileInfoSize)
		go func() {
			e.uploadFileInfos()
		}()
	}
	e.log.Info("[ASSIGN]engine start to load all shards", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
	start := time.Now()
	ptPath := path.Join(e.dataPath, config.DataDirectory, db, strconv.Itoa(int(ptId)))
	walPath := path.Join(e.walPath, config.WalDirectory, db, strconv.Itoa(int(ptId)))
	options, _ := e.metaClient.DatabaseOption(db)
	lockPath := path.Join(ptPath, "LOCK")
	dbPt, err := e.getPartition(db, ptId, false)
	if err != nil {
		if errno.Equal(err, errno.DBPTClosed) {
			e.log.Error("[ASSIGN]assign failed db pt is closed", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Error(err))
			return err
		}
		dbPt = NewDBPTInfo(db, ptId, ptPath, walPath, e.loadCtx, e.fileInfos, options)
	} else if !dbPt.preload {
		e.log.Info("[ASSIGN]engine already load all shards", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
		return nil
	}
	dbPt.dbObsOptions = options
	if !dbPt.preload && IsMemUsageExceeded() {
		e.log.Error("[ASSIGN]assign failed, mem usage is exceeded", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
		return errno.NewError(errno.MemUsageExceeded, GetMemUsageLimit())
	}
	statistics.DBPTTaskInit(opId, db, ptId)
	// Fence to prevent split-brain.
	fc := NewFencer(e.dataPath, e.walPath, db, ptId)
	if err = fc.Fence(); err != nil {
		e.log.Error("[ASSIGN]fence failed", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("opId", opId), zap.Error(err))
		if err1 := fc.ReleaseFence(); err1 != nil {
			e.log.Error("[ASSIGN]release fence failed", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Error(err1))
		}
		statistics.DBPTStepDuration(opId, "DBPTFenceError", time.Since(start).Nanoseconds(), statistics.DBPTLoadErr, err.Error())
		return err
	}
	statistics.DBPTStepDuration(opId, "DBPTFenceDuration", time.Since(start).Nanoseconds(), statistics.DBPTLoading, "")
	// normalize createIndex with unique tsid in indexbuilder
	// make logicClock which used for Generate UUID monotonically increasing
	if !config.IsSharedStorage() {
		ver = metaclient.LogicClock
	}
	dbPt.logicClock = ver
	dbPt.SetOption(e.engOpt)
	dbPt.SetParams(dbPt.preload, &lockPath, dbBriefInfo.EnableTagArray)

	e.log.Info("[ASSIGN]start load dbpt shards", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
	if err = e.loadDbPtShards(opId, dbPt, durationInfos, immutable.LOAD, client); err != nil {
		e.log.Error("[ASSIGN]engine load all shards failed", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Error(err))
		if rbErr := e.offloadDbPT(dbPt); rbErr != nil {
			e.log.Error("[ASSIGN]both load pt and rollback failed", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Error(rbErr))
			panic(rbErr.Error())
		}
		if err1 := fc.ReleaseFence(); err1 != nil {
			e.log.Error("[ASSIGN]release fence failed", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Error(err1))
		}
		statistics.DBPTStepDuration(opId, "DBPTLoadError", time.Since(start).Nanoseconds(), statistics.DBPTLoadErr, err.Error())
		return err
	}

	if config.IsReplication() && dbBriefInfo.Replicas > 1 {
		e.log.Info("[ASSIGN]assign repDBPT", zap.String("db", db), zap.Uint32("pt", ptId), zap.Int("replicasN", dbBriefInfo.Replicas))
		err = e.startRaftNode(opId, nodeId, dbPt, client, storage)
		if err != nil {
			e.log.Error("[ASSIGN]engine start raft node failed", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId), zap.Error(err))
			return err
		}
	}

	start = time.Now()
	e.mu.Lock()
	e.addDBPTInfo(dbPt)
	e.mu.Unlock()
	e.log.Info("[ASSIGN]start replay for replication", zap.String("db", db), zap.Uint32("pt", ptId), zap.Int("replicasN", dbBriefInfo.Replicas))
	// replay is complete before 'assign' operation is complete.
	readReplayForReplication(dbPt.ReplayC, client, storage)
	e.log.Info("[ASSIGN]finish replay for replication", zap.String("db", db), zap.Uint32("pt", ptId), zap.Int("replicasN", dbBriefInfo.Replicas))
	dbPt.enableReportShardLoad()
	dbPt.preload = false
	e.log.Info("[ASSIGN]engine load all shards success", zap.Uint64("opId", opId), zap.String("db", db), zap.Uint32("pt", ptId))
	statistics.DBPTStepDuration(opId, "DBPTLoadFinishDuration", time.Since(start).Nanoseconds(), statistics.DBPTLoaded, "")
	return nil
}

func (e *Engine) RegisterOnPTOffload(id uint64, f func(ptID uint32)) {
	e.onPTOffload[id] = f
}

func (e *Engine) UninstallOnPTOffload(id uint64) {
	delete(e.onPTOffload, id)
}

func (e *Engine) offloadDbPT(pt *DBPTInfo) error {
	var err error
	done := make(chan bool, 1)
	if offloaded := pt.markOffload(done); !offloaded {
		select {
		case <-done:
			log.Debug("all io back", zap.String("db", pt.database), zap.Uint32("ptId", pt.id))
		case <-pt.unload:
			log.Warn("pt has unloaded", zap.String("db", pt.database), zap.Uint32("ptId", pt.id))
			pt.unMarkOffload()
			err = errno.NewError(errno.DBPTClosed)
		case <-time.After(15 * time.Second):
			log.Warn("offload pt timeout", zap.String("db", pt.database), zap.Uint32("ptId", pt.id))
			pt.unMarkOffload()
			err = meta2.ErrConflictWithIo
		}
	}
	if err != nil {
		return err
	}
	pt.setEnableShardsBgr(false)
	err = pt.closeDBPt()
	if err != nil {
		pt.unMarkOffload()
		return err
	}
	e.mu.Lock()
	e.dropDBPTInfo(pt.database, pt.id)
	for _, f := range e.onPTOffload {
		f(pt.id)
	}
	e.mu.Unlock()
	return nil
}

func (e *Engine) loadDbPtShards(opId uint64, dbPt *DBPTInfo, durations map[uint64]*meta2.ShardDurationInfo, loadStat int, client metaclient.MetaClient) error {
	start := time.Now()
	errChan := make(chan error)
	n := 0
	dbRpLock := make(map[string]string)
	for _, sdi := range durations {
		dbRpPath := path.Join(sdi.Ident.OwnerDb, strconv.Itoa(int(sdi.Ident.OwnerPt)), sdi.Ident.Policy)
		if _, ok := dbRpLock[dbRpPath]; ok {
			continue
		}
		dbRpLock[dbRpPath] = ""
		n++
		go func(db string, pt uint32, rp string, engineType config.EngineType) {
			err := dbPt.loadShards(opId, rp, durations, loadStat, client, engineType)
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
	if loadStat == immutable.LOAD {
		statistics.DBPTStepDuration(opId, "DBPTAllShardsOpenDuration", time.Since(start).Nanoseconds(), statistics.DBPTLoading, "")
	}
	return nil
}

func (e *Engine) trySetDbPtMigrating(database string, ptId uint32) bool {
	e.mgtLock.Lock()
	defer e.mgtLock.Unlock()
	db, dbExist := e.migratingDbPT[database]
	if !dbExist {
		pt := make(map[uint32]struct{})
		pt[ptId] = struct{}{}
		e.migratingDbPT[database] = pt
		return true
	}
	if _, ptExist := db[ptId]; !ptExist {
		db[ptId] = struct{}{}
		return true
	}
	return false
}

func (e *Engine) clearDbPtMigrating(db string, ptId uint32) {
	e.mgtLock.Lock()
	defer e.mgtLock.Unlock()
	if _, dbExist := e.migratingDbPT[db]; !dbExist {
		return
	}
	delete(e.migratingDbPT[db], ptId)
	e.log.Info("[ASSIGN]clearDbPtMigrating", zap.String("db", db), zap.Uint32("pt", ptId))
}
func (e *Engine) CheckPtsRemovedDone() bool {
	return len(e.DBPartitions) == 0
}

func (e *Engine) TransferLeadership(database string, nodeId uint64, oldMasterPtId, newMasterPtId uint32) error {
	dbpt, err := e.getPartition(database, oldMasterPtId, false)
	if err != nil {
		return err
	}
	return dbpt.node.TransferLeadership(raftconn.GetRaftNodeId(newMasterPtId))
}

type fencer interface {
	Fence() error

	ReleaseFence() error
}

func NewFencer(dataPath, walPath, db string, pt uint32) fencer {
	return newFencer(dataPath, walPath, db, pt)
}
