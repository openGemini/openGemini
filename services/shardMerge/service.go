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

package shardMerge

import (
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

type metaClient interface {
	UpdateShardInfoTier(shardID uint64, tier uint64, dbName, rpName string) error
	GetMergeShardsList() []meta.MergeShards
}

type engine interface {
	MergeShards(meta.MergeShards) error
}

type Service struct {
	MetaClient metaClient

	Logger *logger.Logger

	Config *config.ShardMergeConfig

	base services.Base

	Engine engine
}

func NewService(c config.ShardMergeConfig) *Service {
	s := &Service{
		Logger: logger.NewLogger(errno.ModuleShardMerge),
		Config: &c,
	}
	s.base.Init("shardMerge", time.Duration(c.RunInterval), s.handle)
	return s
}

func (s *Service) Open() error {
	s.Logger.Info("[shardMerge] service open", zap.Duration("RunInterval", time.Duration(s.Config.RunInterval)))
	if err := s.base.Open(); err != nil {
		return err
	}
	return nil
}

func (s *Service) Close() error {
	if err := s.base.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Service) handle() {
	var err error
	mergeShardss := s.MetaClient.GetMergeShardsList()
	for _, mergeShards := range mergeShardss {
		err = s.Engine.MergeShards(mergeShards)
		if err != nil {
			s.Logger.Error("[shardMerge] mergeShards err", zap.Error(err), zap.String("db", mergeShards.DbName),
				zap.Uint32("ptId", mergeShards.PtId), zap.String("rpName", mergeShards.RpName), zap.Uint64s("shardIds", mergeShards.ShardIds),
				zap.Int64s("shardEndTimes", mergeShards.ShardEndTimes))
		}
	}
}
