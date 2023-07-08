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
	"path"
	"strconv"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

func (e *Engine) PreOffload(db string, ptId uint32) error {
	if !e.trySetDbPtMigrating(db, ptId) {
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	defer e.clearDbPtMigrating(db, ptId)
	e.log.Info("prepare offload pt start", zap.String("db", db), zap.Uint32("pt", ptId))
	dbPt, err := e.getPartition(db, ptId, true)
	if err != nil {
		if errno.Equal(err, errno.PtNotFound) || errno.Equal(err, errno.DatabaseNotFound) {
			return nil
		}
		return err
	}
	if err = dbPt.disableDBPtBgr(); err != nil {
		dbPt.unref()
		return err
	}
	dbPt.setEnableShardsBgr(false)
	dbPt.unref()
	e.log.Info("prepare offload pt success", zap.String("db", db), zap.Uint32("pt", ptId))
	return nil
}

func (e *Engine) RollbackPreOffload(db string, ptId uint32) error {
	if !e.trySetDbPtMigrating(db, ptId) {
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	defer e.clearDbPtMigrating(db, ptId)
	e.log.Info("rollback prepare offload pt start", zap.String("db", db), zap.Uint32("pt", ptId))
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
	e.log.Info("rollback prepare offload pt success", zap.String("db", db), zap.Uint32("pt", ptId))
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
	lockPath := ""
	dbPt := NewDBPTInfo(db, ptId, ptPath, walPath, e.loadCtx)
	dbPt.SetOption(e.engOpt)
	dbPt.unload = make(chan struct{})
	dbPt.preload = true
	dbPt.lockPath = &lockPath
	dbPt.enableTagArray = dbBriefInfo.EnableTagArray
	if err = e.loadDbPtShards(opId, dbPt, durationInfos, immutable.PRELOAD, client); err != nil {
		if rbErr := e.offloadDbPT(dbPt); rbErr != nil {
			e.log.Error("both preload pt and rollback failed", zap.String("db", db), zap.Uint32("pt", ptId))
			panic(rbErr.Error())
		}
		e.log.Info("preload pt failed and rollback success", zap.String("db", db), zap.Uint32("pt", ptId))
		return err
	}

	e.mu.Lock()
	e.addDBPTInfo(dbPt)
	e.mu.Unlock()
	e.log.Info("prepare load pt success", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("opId", opId))
	return nil
}

func (e *Engine) Offload(db string, ptId uint32) error {
	if !e.trySetDbPtMigrating(db, ptId) {
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	defer e.clearDbPtMigrating(db, ptId)
	e.log.Info("offload pt start", zap.String("db", db), zap.Uint32("pt", ptId))
	dbPt, err := e.getPartition(db, ptId, false)
	if err != nil {
		if errno.Equal(err, errno.PtNotFound) || errno.Equal(err, errno.DatabaseNotFound) {
			return nil
		}
		return err
	}
	err = e.offloadDbPT(dbPt)
	if err != nil {
		e.log.Info("offload pt failed", zap.String("db", db), zap.Uint32("pt", ptId))
		return err
	}
	e.log.Info("offload pt success", zap.String("db", db), zap.Uint32("pt", ptId))
	return nil
}

func (e *Engine) Assign(opId uint64, db string, ptId uint32, ver uint64, durationInfos map[uint64]*meta2.ShardDurationInfo, dbBriefInfo *meta2.DatabaseBriefInfo, client metaclient.MetaClient) error {
	if !e.trySetDbPtMigrating(db, ptId) {
		return errno.NewError(errno.PtIsAlreadyMigrating)
	}
	defer e.clearDbPtMigrating(db, ptId)
	e.setMetaClient(client)
	e.log.Info("engine start to load all shards", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("opId", opId))
	start := time.Now()
	ptPath := path.Join(e.dataPath, config.DataDirectory, db, strconv.Itoa(int(ptId)))
	walPath := path.Join(e.walPath, config.WalDirectory, db, strconv.Itoa(int(ptId)))
	lockPath := path.Join(ptPath, "LOCK")
	dbPt, err := e.getPartition(db, ptId, false)
	if err != nil {
		if errno.Equal(err, errno.DBPTClosed) {
			return err
		}
		dbPt = NewDBPTInfo(db, ptId, ptPath, walPath, e.loadCtx)
	} else if !dbPt.preload {
		e.log.Info("engine already load all shards", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("opId", opId))
		return nil
	}
	if !dbPt.preload && IsMemUsageExceeded() {
		return errno.NewError(errno.MemUsageExceeded, GetMemUsageLimit())
	}
	statistics.DBPTTaskInit(opId, db, ptId)
	// Fence to prevent split-brain.
	fc := NewFencer(e.dataPath, e.walPath, db, ptId)
	if err = fc.Fence(); err != nil {
		e.log.Error("fence failed", zap.Error(err), zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("opId", opId))
		if err1 := fc.ReleaseFence(); err1 != nil {
			e.log.Error("release fence failed", zap.Error(err1))
		}
		statistics.DBPTStepDuration(opId, "DBPTFenceError", time.Since(start).Nanoseconds(), statistics.DBPTLoadErr, err.Error())
		return err
	}
	statistics.DBPTStepDuration(opId, "DBPTFenceDuration", time.Since(start).Nanoseconds(), statistics.DBPTLoading, "")
	dbPt.logicClock = ver
	dbPt.lockPath = &lockPath
	dbPt.enableTagArray = dbBriefInfo.EnableTagArray
	dbPt.SetOption(e.engOpt)

	if err = e.loadDbPtShards(opId, dbPt, durationInfos, immutable.LOAD, client); err != nil {
		e.log.Error("engine load all shards failed", zap.Error(err), zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("opId", opId))
		if rbErr := e.offloadDbPT(dbPt); rbErr != nil {
			e.log.Error("both load pt and rollback failed", zap.String("db", db), zap.Uint32("pt", ptId))
			panic(rbErr.Error())
		}
		if err1 := fc.ReleaseFence(); err1 != nil {
			e.log.Error("release fence failed", zap.Error(err1))
		}
		statistics.DBPTStepDuration(opId, "DBPTLoadError", time.Since(start).Nanoseconds(), statistics.DBPTLoadErr, err.Error())
		return err
	}

	start = time.Now()
	e.mu.Lock()
	e.addDBPTInfo(dbPt)
	e.mu.Unlock()
	dbPt.enableReportShardLoad()
	dbPt.preload = false
	e.log.Info("engine load all shards success", zap.String("db", db), zap.Uint32("pt", ptId), zap.Uint64("opId", opId))
	statistics.DBPTStepDuration(opId, "DBPTLoadFinishDuration", time.Since(start).Nanoseconds(), statistics.DBPTLoaded, "")
	return nil
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
		go func(db string, pt uint32, rp string) {
			err := dbPt.loadShards(opId, rp, durations, loadStat, client)
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
}

type fencer interface {
	Fence() error

	ReleaseFence() error
}

func NewFencer(dataPath, walPath, db string, pt uint32) fencer {
	return newFencer(dataPath, walPath, db, pt)
}
