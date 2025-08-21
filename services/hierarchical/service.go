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

package hierarchical

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

type metaClient interface {
	UpdateShardInfoTier(shardID uint64, tier uint64, dbName, rpName string) error
	UpdateIndexInfoTier(indexId uint64, tier uint64, dbName, rpName string) error
}

type engine interface {
	FetchShardsNeedChangeStore() (shardsToWarm, shardsToCold []*meta.ShardIdentifier)
	FetchIndexesNeedChangeStore() (indexesToCold []*meta.IndexIdentifier)
	ChangeShardTierToWarm(db string, ptId uint32, shardID uint64) error
	HierarchicalStorage(db string, ptId uint32, shardID uint64) bool
	IndexHierarchicalStorage(db string, ptId uint32, shardID uint64) bool
}

type WaitGroup struct {
	workChan chan int
	wg       *sync.WaitGroup
}

func NewWaitGroup(maxN int) *WaitGroup {
	return &WaitGroup{
		workChan: make(chan int, maxN),
		wg:       &sync.WaitGroup{},
	}
}

func (wg *WaitGroup) Add(num int) {
	wg.workChan <- num
	wg.wg.Add(1)
}

func (wg *WaitGroup) Done() {
	_, ok := <-wg.workChan
	if !ok { //channel closed
		return
	}
	wg.wg.Done()
}

func (wg *WaitGroup) Wait() {
	if wg.wg != nil {
		wg.wg.Wait()
	}
}

func (wg *WaitGroup) Close() {
	close(wg.workChan)
	wg.wg = nil
}

type Service struct {
	MetaClient metaClient

	Logger *logger.Logger

	Config *config.HierarchicalConfig

	base services.Base

	Engine engine

	closing chan struct{}

	// max concurrency number for move shards
	HsWaitGroup *WaitGroup
}

func NewService(c config.HierarchicalConfig) *Service {
	s := &Service{
		Logger:      logger.NewLogger(errno.ModuleHierarchical),
		Config:      &c,
		HsWaitGroup: NewWaitGroup(c.MaxProcessN),
		closing:     make(chan struct{}),
	}
	s.base.Init("hierarchical", time.Duration(c.RunInterval), s.handle)
	return s
}

func NewIndexToColdService(c config.HierarchicalConfig) *Service {
	s := &Service{
		Logger:      logger.NewLogger(errno.ModuleIndexHierarchical),
		Config:      &c,
		HsWaitGroup: NewWaitGroup(c.MaxProcessN),
		closing:     make(chan struct{}),
	}
	s.base.Init("hierarchical index", time.Duration(c.IndexRunInterval), s.handleIndex)
	return s
}

func (s *Service) Open() error {
	s.Logger.Info("service open", zap.Duration("RunInterval", time.Duration(s.Config.RunInterval)),
		zap.Int("MaxProcessN", s.Config.MaxProcessN))
	if err := s.base.Open(); err != nil {
		return err
	}
	return nil
}

func (s *Service) Close() error {
	if s.closing == nil {
		return nil
	}
	close(s.closing)
	if err := s.base.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Service) handle() {
	shardsToWarm, shardsToCold := s.Engine.FetchShardsNeedChangeStore()
	for _, sh := range shardsToWarm {
		// change shard from hot to warm
		if err := s.Engine.ChangeShardTierToWarm(sh.OwnerDb, sh.OwnerPt, sh.ShardID); err != nil {
			s.Logger.Error("fail to change shard tier to warm", zap.Uint64("shardID", sh.ShardID), zap.Error(err))
			continue
		}

		if err := s.MetaClient.UpdateShardInfoTier(sh.ShardID, util.Warm, sh.OwnerDb, sh.Policy); err != nil {
			s.Logger.Error("fail to update shard tier to warm", zap.Uint64("shardID", sh.ShardID), zap.Error(err))
		}
	}

	if syscontrol.IsHierarchicalStorageEnabled() {
		s.runShardHierarchicalStorage(shardsToCold)
		s.HsWaitGroup.Wait()
	}
}

func (s *Service) handleIndex() {
	if syscontrol.IsHierarchicalIndexStorageEnabled() {
		indexesToCold := s.Engine.FetchIndexesNeedChangeStore()
		s.runIndexHierarchicalStorage(indexesToCold)
		s.HsWaitGroup.Wait()
	}
}

func (s *Service) runShardHierarchicalStorage(shardsToCold []*meta.ShardIdentifier) {
	toObsCold := statistics.GetToObsCold()
	for _, shard := range shardsToCold {
		s.HsWaitGroup.Add(1)
		select {
		case <-s.closing:
			s.HsWaitGroup.Done()
			s.closing = nil
			return
		default:
		}
		toObsCold.AddShardToColdSum()
		go func(s *Service, shard *meta.ShardIdentifier) {
			defer func() {
				s.HsWaitGroup.Done()
			}()

			// update tier status to moving
			s.Logger.Info("UpdateShardInfoTier to moving", zap.Uint64("shardId", shard.ShardID), zap.String("db", shard.OwnerDb))
			if err := s.MetaClient.UpdateShardInfoTier(shard.ShardID, util.Moving, shard.OwnerDb, shard.Policy); err != nil {
				s.Logger.Error("update shard info tier err",
					zap.Int64("shard id", int64(shard.ShardID)),
					zap.Int64("tier", int64(util.Moving)), zap.Error(err))
				return
			}

			if ok := s.Engine.HierarchicalStorage(shard.OwnerDb, shard.OwnerPt, shard.ShardID); !ok {
				toObsCold.AddShardToColdFailSum()
				return
			}

			s.Logger.Info("UpdateShardInfoTier to cold", zap.Uint64("shardId", shard.ShardID), zap.String("db", shard.OwnerDb))
			if err := s.MetaClient.UpdateShardInfoTier(shard.ShardID, util.Cold, shard.OwnerDb, shard.Policy); err != nil {
				s.Logger.Error("update shard info tier err",
					zap.Int64("shard id", int64(shard.ShardID)),
					zap.Uint64("tier", util.Cold), zap.Error(err))
				toObsCold.AddShardToColdFailSum()
			}
			toObsCold.AddShardToColdSuccessSum()
		}(s, shard)
	}
}

func (s *Service) runIndexHierarchicalStorage(indexToCold []*meta.IndexIdentifier) {
	toObsCold := statistics.GetToObsCold()
	for _, indexBuilder := range indexToCold {
		select {
		case <-s.closing:
			s.closing = nil
			return
		default:
		}
		toObsCold.IndexToColdSum.Incr()
		// update tier status to moving
		if err := s.MetaClient.UpdateIndexInfoTier(indexBuilder.Index.IndexID, util.Moving, indexBuilder.OwnerDb, indexBuilder.Policy); err != nil {
			s.Logger.Error("update index info tier err",
				zap.Int64("index id", int64(indexBuilder.Index.IndexID)),
				zap.Int64("tier", int64(util.Moving)), zap.Error(err))
			return
		}
		if ok := s.Engine.IndexHierarchicalStorage(indexBuilder.OwnerDb, indexBuilder.OwnerPt, indexBuilder.Index.IndexID); !ok {
			toObsCold.AddIndexToColdFailSum()
			return
		}
		if err := s.MetaClient.UpdateIndexInfoTier(indexBuilder.Index.IndexID, util.Cold, indexBuilder.OwnerDb, indexBuilder.Policy); err != nil {
			toObsCold.AddIndexToColdFailSum()
			s.Logger.Error("update index info tier err",
				zap.Int64("index id", int64(indexBuilder.Index.IndexID)),
				zap.Uint64("tier", util.Cold), zap.Error(err))
		}
		toObsCold.AddIndexToColdSuccessSum()
	}
}
