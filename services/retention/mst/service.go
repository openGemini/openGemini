// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package mst

import (
	"time"

	log "github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

type Service struct {
	services.Base

	MetaClient interface {
		GetAllMstTTLInfo() map[string]map[string][]*meta.MeasurementTTLTnfo
	}

	Engine interface {
		ExpiredShardsForMst(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.ShardIdentifier
		ExpiredIndexesForMst(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.IndexIdentifier
		DeleteMstInShard(db string, ptId uint32, shardID uint64, mst string) error
		DeleteMstInIndex(db string, ptId uint32, indexID uint64, msts []string, onlyUseDiskThreshold uint64) error
	}

	deleteMstIndexOnlyUstDiskThreshold uint64
}

func NewDeleteMstInShardService(interval time.Duration) *Service {
	s := &Service{}
	s.Init("retention mst in shard", interval, s.deleteMstInShardHandle)
	return s
}

func (s *Service) deleteMstInShardHandle() {
	logger, logEnd := log.NewOperation(s.Logger.GetZapLogger(), "retention mst policy deletion in shard check", "retention_mst_delete_in_shard_check")

	retryNeeded := s.HandleLocalStorageInShard(logger)

	if retryNeeded {
		logger.Info("One or more errors occurred during mst deletion in shard and will be retried on the next check",
			zap.Duration("check_interval", s.Interval))
	}

	logEnd()
}

func (s *Service) HandleLocalStorageInShard(logger *zap.Logger) bool {
	var retryNeeded bool
	measurementTTLInfos := s.MetaClient.GetAllMstTTLInfo()
	for dbName, rpMap := range measurementTTLInfos {
		for rpName, ttlInfos := range rpMap {
			if ttlInfos == nil {
				continue
			}
			retryNeeded = deleteMstInShards(logger, ttlInfos, s, dbName, rpName, retryNeeded)
		}
	}
	return retryNeeded
}

func deleteMstInShards(logger *zap.Logger, ttlInfos []*meta.MeasurementTTLTnfo, s *Service, dbName string, rpName string, retryNeeded bool) bool {
	for _, ttlInfo := range ttlInfos {
		expiredShards := s.Engine.ExpiredShardsForMst(dbName, rpName, ttlInfo)
		for _, expiredShard := range expiredShards {
			if err := s.Engine.DeleteMstInShard(dbName, expiredShard.OwnerPt, expiredShard.ShardID, ttlInfo.Name); err != nil {
				logger.Error("Failed to delete mst in shard",
					log.Database(dbName),
					log.Shard(expiredShard.ShardID),
					zap.Error(err),
					zap.String("mst", ttlInfo.Name))
				retryNeeded = true
			}
		}
	}
	return retryNeeded
}

func NewDeleteMstInIndexService(interval time.Duration, deleteMstIndexOnlyUstDiskThreshold uint64) *Service {
	s := &Service{}
	s.deleteMstIndexOnlyUstDiskThreshold = deleteMstIndexOnlyUstDiskThreshold
	s.Init("retention mst in index", interval, s.deleteMstInIndexHandle)
	return s
}

func (s *Service) deleteMstInIndexHandle() {
	logger, logEnd := log.NewOperation(s.Logger.GetZapLogger(), "retention mst policy deletion in index check", "retention_mst_delete_in_index_check")

	retryNeeded := s.HandleLocalStorageInIndex(logger)

	if retryNeeded {
		logger.Info("One or more errors occurred during mst deletion in index and will be retried on the next check",
			zap.Duration("check_interval", s.Interval))
	}

	logEnd()
}

func (s *Service) HandleLocalStorageInIndex(logger *zap.Logger) bool {
	var retryNeeded bool
	measurementTTLInfos := s.MetaClient.GetAllMstTTLInfo()
	for dbName, rpMap := range measurementTTLInfos {
		for rpName, ttlInfos := range rpMap {
			if ttlInfos == nil {
				continue
			}
			retryNeeded = deleteMstInIndexes(logger, ttlInfos, s, dbName, rpName, retryNeeded)
		}
	}
	return retryNeeded
}

func deleteMstInIndexes(logger *zap.Logger, ttlInfos []*meta.MeasurementTTLTnfo, s *Service, dbName string, rpName string, retryNeeded bool) bool {
	var expiredIndexForMsts = make(map[uint32]map[uint64][]string)

	for _, ttlInfo := range ttlInfos {
		expiredIndexes := s.Engine.ExpiredIndexesForMst(dbName, rpName, ttlInfo)
		for _, expiredIndex := range expiredIndexes {
			ptID := expiredIndex.OwnerPt
			indexID := expiredIndex.Index.IndexID

			if _, ok := expiredIndexForMsts[ptID]; !ok {
				expiredIndexForMsts[ptID] = make(map[uint64][]string)
			}

			expiredIndexForMsts[ptID][indexID] = append(
				expiredIndexForMsts[ptID][indexID],
				ttlInfo.Name)
		}
	}

	for ptID, indexForMsts := range expiredIndexForMsts {
		for indexID, msts := range indexForMsts {
			if err := s.Engine.DeleteMstInIndex(dbName, ptID, indexID, msts, s.deleteMstIndexOnlyUstDiskThreshold); err != nil {
				logger.Error("Failed to delete msts in index",
					log.Database(dbName),
					zap.Error(err),
					zap.Uint32("ptID", ptID),
					zap.Uint64("indexID", indexID),
					zap.Strings("msts", msts))
				retryNeeded = true
			}
		}
	}

	return retryNeeded
}
