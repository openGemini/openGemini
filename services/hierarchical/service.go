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

package hierarchical

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

type metaClient interface {
	UpdateShardInfoTier(shardID uint64, tier uint64, dbName, rpName string) error
}

type engine interface {
	FetchShardsNeedChangeStore() (shardsToWarm, shardsToCold []*meta.ShardIdentifier)
	ChangeShardTierToWarm(db string, ptId uint32, shardID uint64) error
	HierarchicalStorage(db string, ptId uint32, shardID uint64) error
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
	wg.wg.Wait()
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

	// max concurrency number for move shards
	HsWaitGroup *WaitGroup
}

func NewService(c config.HierarchicalConfig) *Service {
	s := &Service{
		Logger:      logger.NewLogger(errno.ModuleHierarchical),
		Config:      &c,
		HsWaitGroup: NewWaitGroup(c.MaxProcessN),
	}
	s.base.Init("hierarchical", time.Duration(c.RunInterval), s.handle)
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
	if err := s.base.Close(); err != nil {
		return err
	}

	s.HsWaitGroup.Close()
	s.HsWaitGroup = nil
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

func (s *Service) runShardHierarchicalStorage(shardsToCold []*meta.ShardIdentifier) {
	for _, shard := range shardsToCold {
		s.HsWaitGroup.Add(1)
		go func(s *Service, shard *meta.ShardIdentifier) {
			defer func() {
				s.HsWaitGroup.Done()
			}()

			// update tier status to moving
			if err := s.MetaClient.UpdateShardInfoTier(shard.ShardID, util.Moving, shard.OwnerDb, shard.Policy); err != nil {
				s.Logger.Error("update shard info tier err",
					zap.Int64("shard id", int64(shard.ShardID)),
					zap.Int64("tier", int64(util.Moving)), zap.Error(err))
				return
			}

			if err := s.Engine.HierarchicalStorage(shard.OwnerDb, shard.OwnerPt, shard.ShardID); err != nil {
				s.Logger.Error("HierarchicalStorage run error", zap.Error(err))
				return
			}

			if err := s.MetaClient.UpdateShardInfoTier(shard.ShardID, util.Cold, shard.OwnerDb, shard.Policy); err != nil {
				s.Logger.Error("update shard info tier err",
					zap.Int64("shard id", int64(shard.ShardID)),
					zap.Uint64("tier", util.Cold), zap.Error(err))
			}
		}(s, shard)
	}
}
