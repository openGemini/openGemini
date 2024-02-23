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

package retention

import (
	"time"

	log "github.com/influxdata/influxdb/logger"
	_ "github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

// include Shard retention polices and Index retention polices
type Service struct {
	services.Base

	MetaClient interface {
		PruneGroupsCommand(shardGroup bool, id uint64) error
		GetShardDurationInfo(index uint64) (*meta.ShardDurationResponse, error)
		DeleteShardGroup(database, policy string, id uint64) error
		DeleteIndexGroup(database, policy string, id uint64) error
	}

	Engine interface {
		DeleteIndex(db string, ptId uint32, indexID uint64) error
		UpdateShardDurationInfo(info *meta.ShardDurationInfo) error
		ExpiredShards() []*meta.ShardIdentifier
		ExpiredIndexes() []*meta.IndexIdentifier
		ExpiredCacheIndexes() []*meta.IndexIdentifier
		DeleteShard(db string, ptId uint32, shardID uint64) error
		ClearIndexCache(db string, ptId uint32, indexID uint64) error
	}

	index uint64
}

func NewService(interval time.Duration) *Service {
	s := &Service{}
	s.Init("retention", interval, s.handle)
	return s
}

// ShardGroup Retention Policy check and IndexGroup Retention Policy check.
func (s *Service) handle() {
	logger, logEnd := log.NewOperation(s.Logger.GetZapLogger(), "retention policy deletion check", "retention_delete_check")
	if err := s.updateDurationInfo(); err != nil {
		logger.Warn("update duration info failed", zap.Error(err))
		return
	}

	// Mark down if an error occurred during this function so we can inform the
	// user that we will try again on the next interval.
	// Without the message, they may see the error message and assume they
	// have to do it manually.
	var retryNeeded bool
	expiredShards := s.Engine.ExpiredShards()
	for i := range expiredShards {
		if err := s.MetaClient.DeleteShardGroup(expiredShards[i].OwnerDb, expiredShards[i].Policy, expiredShards[i].ShardGroupID); err != nil {
			logger.Info("Failed to delete shard group",
				log.Database(expiredShards[i].OwnerDb),
				log.ShardGroup(expiredShards[i].ShardGroupID),
				log.RetentionPolicy(expiredShards[i].Policy),
				zap.Error(err))
			retryNeeded = true
			continue
		}

		if err := s.Engine.DeleteShard(expiredShards[i].OwnerDb, expiredShards[i].OwnerPt, expiredShards[i].ShardID); err != nil {
			logger.Error("Failed to delete shard",
				log.Database(expiredShards[i].OwnerDb),
				log.Shard(expiredShards[i].ShardID),
				zap.Error(err))

			retryNeeded = true
			continue
		}

		if err := s.MetaClient.PruneGroupsCommand(true, expiredShards[i].ShardID); err != nil {
			logger.Error("fail to pruning shard groups", zap.Error(err), zap.Uint64("id", expiredShards[i].ShardID))
		}
	}

	expiredIndexes := s.Engine.ExpiredIndexes()
	for i := range expiredIndexes {
		if err := s.MetaClient.DeleteIndexGroup(expiredIndexes[i].OwnerDb, expiredIndexes[i].Policy, expiredIndexes[i].Index.IndexGroupID); err != nil {
			logger.Error("Failed to mark delete index group", log.Database(expiredIndexes[i].OwnerDb),
				log.RetentionPolicy(expiredIndexes[i].Policy),
				zap.Uint64("index group id", expiredIndexes[i].Index.IndexGroupID),
				zap.Error(err))

			retryNeeded = true
			continue
		}

		if err := s.Engine.DeleteIndex(expiredIndexes[i].OwnerDb, expiredIndexes[i].OwnerPt, expiredIndexes[i].Index.IndexID); err != nil {
			logger.Error("Failed to delete index",
				log.Database(expiredIndexes[i].OwnerDb),
				zap.Uint64("indexID", expiredIndexes[i].Index.IndexID),
				zap.Error(err))

			retryNeeded = true
			continue
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
			continue
		}
	}

	if retryNeeded {
		logger.Info("One or more errors occurred during index deletion and will be retried on the next check",
			zap.Duration("check_interval", s.Interval))
	}

	logEnd()
}

func (s *Service) updateDurationInfo() error {
	res, err := s.MetaClient.GetShardDurationInfo(s.index)
	if err != nil {
		return err
	}
	if res.DataIndex > s.index {
		s.index = res.DataIndex
	}

	for i := range res.Durations {
		err = s.Engine.UpdateShardDurationInfo(&res.Durations[i])
		if errno.Equal(err, errno.PtNotFound) || errno.Equal(err, errno.DBPTClosed) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}
