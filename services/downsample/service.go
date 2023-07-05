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

package downsample

import (
	"fmt"
	"time"

	log "github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

// include Shard retention polices and Index retention polices
type Service struct {
	services.Base

	MetaClient interface {
		GetDownSamplePolicies() (*meta.DownSamplePoliciesInfoWithDbRp, error)
		GetMstInfoWithInRp(dbName, rpName string, dataTypes []int64) (*meta.RpMeasurementsFieldsInfo, error)
		UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
	}

	Engine interface {
		StartDownSampleTask(sdsp *meta.ShardDownSamplePolicyInfo, schema []hybridqp.Catalog, log *zap.Logger, meta interface {
			UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
		}) error
		GetShardDownSamplePolicyInfos(meta interface {
			UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
		}) ([]*meta.ShardDownSamplePolicyInfo, error)
		GetDownSamplePolicy(key string) *meta.StoreDownSamplePolicy
		UpdateDownSampleInfo(policies *meta.DownSamplePoliciesInfoWithDbRp)
	}
}

func NewService(interval time.Duration) *Service {
	s := &Service{}
	s.Init("downSample", interval, s.handle)
	return s
}

func (s *Service) handle() {
	logger, logEnd := log.NewOperation(s.Logger.GetZapLogger(), "downSample deletion check", "downSample_check")
	defer logEnd()
	policies, err := s.MetaClient.GetDownSamplePolicies()
	if err != nil {
		logger.Warn("update downSample info failed", zap.Error(err))
		return
	}
	s.Engine.UpdateDownSampleInfo(policies)

	infos, err := s.Engine.GetShardDownSamplePolicyInfos(s.MetaClient)
	if err != nil {
		logger.Warn("get shard downSample info failed:", zap.Error(err))
	}
	for _, sdsp := range infos {
		logger.Info(fmt.Sprintf("start run downsample task with shardID:%d,downsample level:%d", sdsp.ShardId, sdsp.DownSamplePolicyLevel))
		policy := s.Engine.GetDownSamplePolicy(sdsp.DbName + "." + sdsp.RpName)
		infos, err := s.MetaClient.GetMstInfoWithInRp(sdsp.DbName, sdsp.RpName, policy.Info.GetTypes())
		if err != nil {
			logger.Warn("GetMstInfoWithInRp Failed", zap.Error(err))
			return
		}
		policy.Schemas = downSampleQuerySchemaGen(sdsp, infos, policy.Info)
		if e := s.Engine.StartDownSampleTask(sdsp, policy.Schemas[sdsp.DownSamplePolicyLevel-1], logger, s.MetaClient); e != nil {
			logger.Warn("update shard downsample information Failed", zap.Error(e))
		}
	}
}
