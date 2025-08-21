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

package retention

import (
	"fmt"
	"sync"
	"time"

	log "github.com/influxdata/influxdb/logger"
	_ "github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

const (
	InitPending uint32 = 0
	ExitPending uint32 = 1

	ShardDelete int = 1
	IndexDelete int = 2
)

type PendingInfo struct {
	pendingId map[uint64]struct{}
	lock      sync.RWMutex
}

func NewPendingInfo() PendingInfo {
	return PendingInfo{
		pendingId: make(map[uint64]struct{}, 0),
	}
}

func (s *PendingInfo) EnteringPendingState(id uint64, pendingState *uint32) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if *pendingState == InitPending {
		s.pendingId[id] = struct{}{}
	}
}

func (s *PendingInfo) ExitingPendingState(id uint64, pendingState *uint32) {
	s.lock.Lock()
	defer s.lock.Unlock()
	*pendingState = ExitPending
	_, ok := s.pendingId[id]
	if ok {
		delete(s.pendingId, id)
	}
}

func (s *PendingInfo) IsInPendingState(id uint64) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, ok := s.pendingId[id]
	if ok {
		return fmt.Errorf("id{%d} deletion is still in pending state", id)
	}
	return nil
}

// include Shard retention polices and Index retention polices
type Service struct {
	services.Base

	MetaClient interface {
		PruneGroupsCommand(shardGroup bool, id uint64) error
		GetShardDurationInfo(index uint64) (*meta.ShardDurationResponse, error)
		GetIndexDurationInfo(index uint64) (*meta.IndexDurationResponse, error)
		DeleteShardGroup(database, policy string, id uint64, deleteType int32) error
		DeleteIndexGroup(database, policy string, id uint64) error
		DelayDeleteShardGroup(database, policy string, id uint64, deletedAt time.Time, deleteType int32) error
		GetExpiredShards() ([]meta.ExpiredShardInfos, []meta.ExpiredShardInfos)
		GetExpiredIndexes() []meta.ExpiredIndexInfos
	}

	Engine interface {
		DeleteIndex(db string, ptId uint32, indexID uint64) error
		UpdateShardDurationInfo(info *meta.ShardDurationInfo, nilShardMap *map[uint64]*meta.ShardDurationInfo) error
		UpdateIndexDurationInfo(info *meta.IndexDurationInfo, nilIndexMap *map[uint64]*meta.IndexDurationInfo) error
		ExpiredShards(nilShardMap *map[uint64]*meta.ShardDurationInfo) []*meta.ShardIdentifier
		ExpiredIndexes(nilIndexMap *map[uint64]*meta.IndexDurationInfo) []*meta.IndexIdentifier
		ExpiredCacheIndexes() []*meta.IndexIdentifier
		DeleteShard(db string, ptId uint32, shardID uint64) error
		ClearIndexCache(db string, ptId uint32, indexID uint64) error
	}

	pendingShard PendingInfo
	pendingIndex PendingInfo

	index uint64
}

var shardDeletionTimeout time.Duration = 120 * time.Second

func SetShardDeletionDelay(delay time.Duration) {
	shardDeletionTimeout = delay
}

func NewService(interval time.Duration) *Service {
	s := &Service{
		pendingShard: NewPendingInfo(),
		pendingIndex: NewPendingInfo(),
	}
	s.Init("retention", interval, s.handle)
	return s
}

func (s *Service) HandleSharedStorage(logger *zap.Logger) bool {
	t := time.Now().UTC()
	var retryNeeded bool
	markDelSgInfos, deletedSgInfos := s.MetaClient.GetExpiredShards()
	// mark shardGroup as deleted
	for _, sginfo := range markDelSgInfos {
		err := s.MetaClient.DelayDeleteShardGroup(sginfo.Database, sginfo.Policy, sginfo.ShardGroupId, t, meta.MarkDelete)
		if err != nil {
			logger.Error("Failed to delete shard group", zap.String("retentionPolicy", sginfo.Policy),
				zap.Uint64("id", sginfo.ShardGroupId),
				zap.Error(err))
			retryNeeded = true
		}
	}
	// delete the shard data and metaData
	var err error
	for _, sginfo := range deletedSgInfos {
		for i := range sginfo.ShardPaths {
			// delete the the files of obs-storage
			err = fileops.DeleteObsPath(sginfo.ShardPaths[i], sginfo.ObsOpts)
			if err != nil {
				logger.Error("Failed to delete shard", zap.String("retentionPolicy", sginfo.Policy),
					zap.String("path", sginfo.ShardPaths[i]),
					zap.Error(err))
				retryNeeded = true
				continue
			}
			// delete the the files of local-storage
			err = fileops.DeleteObsPath(sginfo.ShardPaths[i], nil)
			if err != nil {
				logger.Error("Failed to delete shard", zap.String("retentionPolicy", sginfo.Policy),
					zap.String("path", sginfo.ShardPaths[i]),
					zap.Error(err))
				retryNeeded = true
				continue
			}
			if err := s.MetaClient.PruneGroupsCommand(true, sginfo.ShardIds[i]); err != nil {
				logger.Error("Fail to pruning shard groups", zap.Error(err), zap.Uint64("id", sginfo.ShardIds[i]))
				retryNeeded = true
				continue
			}
			logger.Info("delete shard successfully", zap.String("retentionPolicy", sginfo.Policy), zap.String("path", sginfo.ShardPaths[i]))
		}
	}

	expiredIndexes := s.MetaClient.GetExpiredIndexes()
	for i := range expiredIndexes {
		if err := s.MetaClient.DeleteIndexGroup(expiredIndexes[i].Database, expiredIndexes[i].Policy, expiredIndexes[i].IndexGroupID); err != nil {
			logger.Error("Failed to mark delete index group", log.Database(expiredIndexes[i].Database),
				log.RetentionPolicy(expiredIndexes[i].Policy),
				zap.Uint64("index group id", expiredIndexes[i].IndexGroupID),
				zap.Error(err))
			retryNeeded = true
			continue
		}

		for j := range expiredIndexes[i].IndexIDs {
			if err := s.MetaClient.PruneGroupsCommand(false, expiredIndexes[i].IndexIDs[j]); err != nil {
				logger.Error("Problem pruning index groups", zap.Error(err), zap.Uint64("id", expiredIndexes[i].IndexIDs[j]))
				retryNeeded = true
				continue
			}
			logger.Info("delete index successfully", zap.String("retentionPolicy", expiredIndexes[i].Policy), zap.Uint64("id", expiredIndexes[i].IndexIDs[j]))
		}
	}

	return retryNeeded
}

func (s *Service) getPendingInfo(delType int) *PendingInfo {
	if delType == ShardDelete {
		return &s.pendingShard
	} else {
		return &s.pendingIndex
	}
}

func (s *Service) DeleteByEngine(db string, ptId uint32, id uint64, delType int) error {
	if delType == ShardDelete {
		return s.Engine.DeleteShard(db, ptId, id)
	} else {
		return s.Engine.DeleteIndex(db, ptId, id)
	}
}

func (s *Service) DeleteShardOrIndex(db string, ptId uint32, id uint64, delType int) error {
	pdInfo := s.getPendingInfo(delType)
	if err := pdInfo.IsInPendingState(id); err != nil {
		return err
	}

	pendingState := InitPending
	done := make(chan error, 1)
	go func() {
		err := s.DeleteByEngine(db, ptId, id, delType)
		if err != nil {
			s.Logger.Error("delete shard or index failed", zap.Int("delete type", delType), zap.Uint64("id", id), zap.Error(err))
		}
		done <- err
		pdInfo.ExitingPendingState(id, &pendingState)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(shardDeletionTimeout):
		pdInfo.EnteringPendingState(id, &pendingState)
		return fmt.Errorf("id{%d} deletion entered pending status", id)
	}
}

// ShardGroup Retention Policy check and IndexGroup Retention Policy check.
func (s *Service) HandleLocalStorage(logger *zap.Logger, nilShardMap *map[uint64]*meta.ShardDurationInfo, nilIndexMap *map[uint64]*meta.IndexDurationInfo) bool {
	var retryNeeded bool
	expiredShards := s.Engine.ExpiredShards(nilShardMap)
	for i := range expiredShards {
		if err := s.MetaClient.DeleteShardGroup(expiredShards[i].OwnerDb, expiredShards[i].Policy, expiredShards[i].ShardGroupID, meta.MarkDelete); err != nil {
			logger.Info("Failed to delete shard group",
				log.Database(expiredShards[i].OwnerDb),
				log.ShardGroup(expiredShards[i].ShardGroupID),
				log.RetentionPolicy(expiredShards[i].Policy),
				zap.Error(err))
			retryNeeded = true
		}

		if err := s.DeleteShardOrIndex(expiredShards[i].OwnerDb, expiredShards[i].OwnerPt, expiredShards[i].ShardID, ShardDelete); err != nil {
			logger.Error("Failed to delete shard",
				log.Database(expiredShards[i].OwnerDb),
				log.Shard(expiredShards[i].ShardID),
				zap.Error(err))
			if !errno.Equal(err, errno.ShardNotFound) && !errno.Equal(err, errno.IndexNotFound) {
				retryNeeded = true
			}
		}

		if err := s.MetaClient.PruneGroupsCommand(true, expiredShards[i].ShardID); err != nil {
			logger.Error("fail to pruning shard groups", zap.Error(err), zap.Uint64("id", expiredShards[i].ShardID))
		}
	}

	expiredIndexes := s.Engine.ExpiredIndexes(nilIndexMap)
	for i := range expiredIndexes {
		if err := s.MetaClient.DeleteIndexGroup(expiredIndexes[i].OwnerDb, expiredIndexes[i].Policy, expiredIndexes[i].Index.IndexGroupID); err != nil {
			logger.Error("Failed to mark delete index group", log.Database(expiredIndexes[i].OwnerDb),
				log.RetentionPolicy(expiredIndexes[i].Policy),
				zap.Uint64("index group id", expiredIndexes[i].Index.IndexGroupID),
				zap.Error(err))

			retryNeeded = true
		}

		if err := s.DeleteShardOrIndex(expiredIndexes[i].OwnerDb, expiredIndexes[i].OwnerPt, expiredIndexes[i].Index.IndexID, IndexDelete); err != nil {
			logger.Error("Failed to delete index",
				log.Database(expiredIndexes[i].OwnerDb),
				zap.Uint64("indexID", expiredIndexes[i].Index.IndexID),
				zap.Error(err))

			retryNeeded = true
		}

		// send request to clear deleted shard group from PyMeta.
		if err := s.MetaClient.PruneGroupsCommand(false, expiredIndexes[i].Index.IndexID); err != nil {
			logger.Error("Problem pruning index groups", zap.Error(err), zap.Uint64("id", expiredIndexes[i].Index.IndexID))
		}
	}

	expiredCacheIndexes := s.Engine.ExpiredCacheIndexes()
	for i := range expiredCacheIndexes {
		if err := s.Engine.ClearIndexCache(expiredCacheIndexes[i].OwnerDb, expiredCacheIndexes[i].OwnerPt, expiredCacheIndexes[i].Index.IndexID); err != nil {
			logger.Error("Failed to clear index cache",
				log.Database(expiredCacheIndexes[i].OwnerDb),
				zap.Uint64("indexID", expiredCacheIndexes[i].Index.IndexID),
				zap.Error(err))

			retryNeeded = true
		}
	}

	return retryNeeded
}

func (s *Service) updateShardDurationInfo(nilShardMap *map[uint64]*meta.ShardDurationInfo) error {
	res, err := s.MetaClient.GetShardDurationInfo(s.index)
	if err != nil {
		return err
	}
	if res.DataIndex > s.index {
		s.index = res.DataIndex
	}

	for i := range res.Durations {
		err = s.Engine.UpdateShardDurationInfo(&res.Durations[i], nilShardMap)
		if errno.Equal(err, errno.PtNotFound) || errno.Equal(err, errno.DBPTClosed) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) UpdateIndexDurationInfo(nilIndexMap *map[uint64]*meta.IndexDurationInfo) error {
	res, err := s.MetaClient.GetIndexDurationInfo(s.index)
	if err != nil {
		return err
	}
	if res.DataIndex > s.index {
		s.index = res.DataIndex
	}

	for i := range res.Durations {
		err = s.Engine.UpdateIndexDurationInfo(&res.Durations[i], nilIndexMap)
		if errno.Equal(err, errno.PtNotFound) || errno.Equal(err, errno.DBPTClosed) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) updateDurationInfo(nilShardMap *map[uint64]*meta.ShardDurationInfo, nilIndexMap *map[uint64]*meta.IndexDurationInfo) error {
	err1 := s.updateShardDurationInfo(nilShardMap)
	err2 := s.UpdateIndexDurationInfo(nilIndexMap)
	if err1 != nil {
		return err1
	}
	return err2
}

func (s *Service) handle() {
	logger, logEnd := log.NewOperation(s.Logger.GetZapLogger(), "retention policy deletion check", "retention_delete_check")
	nilShardMap := make(map[uint64]*meta.ShardDurationInfo)
	nilIndexMap := make(map[uint64]*meta.IndexDurationInfo)
	if err := s.updateDurationInfo(&nilShardMap, &nilIndexMap); err != nil {
		logger.Warn("update duration info failed", zap.Error(err))
		return
	}

	// Mark down if an error occurred during this function so we can inform the
	// user that we will try again on the next interval.
	// Without the message, they may see the error message and assume they
	// have to do it manually.
	var retryNeeded bool
	if config.IsLogKeeper() {
		retryNeeded = s.HandleSharedStorage(logger)
	} else {
		retryNeeded = s.HandleLocalStorage(logger, &nilShardMap, &nilIndexMap)
	}

	if retryNeeded {
		logger.Info("One or more errors occurred during index deletion and will be retried on the next check",
			zap.Duration("check_interval", s.Interval))
	}

	logEnd()
}
