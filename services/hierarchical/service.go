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
	"time"

	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

type Service struct {
	services.Base

	MetaClient interface {
		UpdateShardInfoTier(shardID uint64, tier uint64, dbName, rpName string) error
	}

	Engine interface {
		FetchShardsNeedChangeStore() (shardsToWarm, shardsToCold []*meta.ShardIdentifier)
		ChangeShardTierToWarm(db string, ptId uint32, shardID uint64) error
	}
}

func NewService(interval time.Duration) *Service {
	s := &Service{}
	s.Init("hierarchical", interval, s.handle)
	return s
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

	for _, sh := range shardsToCold {
		// change shard from warm to hot

		if err := s.MetaClient.UpdateShardInfoTier(sh.ShardID, util.Cold, sh.OwnerDb, sh.Policy); err != nil {
			s.Logger.Error("fail to update shard tier to cold", zap.Uint64("shardID", sh.ShardID), zap.Error(err))
		}
	}
}
