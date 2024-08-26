// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"github.com/openGemini/openGemini/engine/hybridqp"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

func (e *Engine) GetShardDownSampleLevel(db string, ptId uint32, shardID uint64) int {
	return e.getShardDownSampleLevel(db, ptId, shardID)
}

func (e *Engine) resetDownSampleFlag() {
	for _, policy := range e.DownSamplePolicies {
		policy.Alive = false
	}
}

func (e *Engine) removeDeadDownSamplePolicy() {
	for name, policy := range e.DownSamplePolicies {
		if !policy.Alive {
			delete(e.DownSamplePolicies, name)
		}
	}
}

func (e *Engine) GetShardDownSamplePolicyInfos(meta interface {
	UpdateShardDownSampleInfo(Ident *meta2.ShardIdentifier) error
}) ([]*meta2.ShardDownSamplePolicyInfo, error) {
	policies := make([]*meta2.ShardDownSamplePolicyInfo, 0)
	e.mu.RLock()
	defer e.mu.RUnlock()
	for dbName, dbInfo := range e.DBPartitions {
		for ptId, ptInfo := range dbInfo {
			if err := e.checkAndAddRefPTNoLock(dbName, ptId); err != nil {
				return nil, err
			}
			ptInfo.mu.RLock()
			for shardId, shardData := range ptInfo.shards {
				dsp, ok := e.DownSamplePolicies[dbName+"."+shardData.GetRPName()]
				if !ok {
					continue
				}
				p := shardData.GetShardDownSamplePolicy(dsp.Info)
				if p == nil {
					continue
				}
				if err := e.openShardLazy(shardData); err != nil {
					ptInfo.mu.RUnlock()
					return nil, err
				}
				if err := shardData.UpdateShardReadOnly(meta); err != nil {
					ptInfo.mu.RUnlock()
					return nil, err
				}
				shardData.WaitWriteFinish()
				shardData.ForceFlush()
				if shardData.IsOutOfOrderFilesExist() {
					continue
				}
				p.Ident = shardData.GetIdent()
				p.DbName = dbName
				p.PtId = ptId
				p.ShardId = shardId
				policies = append(policies, p)
			}
			ptInfo.mu.RUnlock()
			e.unrefDBPTNoLock(dbName, ptId)
		}
	}
	return policies, nil
}

func (e *Engine) GetDownSamplePolicy(key string) *meta2.StoreDownSamplePolicy {
	return e.DownSamplePolicies[key]
}

func (e *Engine) StartDownSampleTask(sdsp *meta2.ShardDownSamplePolicyInfo, schema []hybridqp.Catalog, log *zap.Logger,
	meta interface {
		UpdateShardDownSampleInfo(Ident *meta2.ShardIdentifier) error
	}) error {
	e.mu.RLock()
	if err := e.checkAndAddRefPTNoLock(sdsp.DbName, sdsp.PtId); err != nil {
		e.mu.RUnlock()
		return err
	}
	e.mu.RUnlock()
	defer e.unrefDBPT(sdsp.DbName, sdsp.PtId)
	sh, err := e.GetShard(sdsp.DbName, sdsp.PtId, sdsp.ShardId)
	if err != nil {
		return err
	}
	if sh == nil {
		return nil
	}
	id := sh.GetIdent().ShardID
	sh.NewDownSampleTask(sdsp, schema, log)
	if sh.CanDoDownSample() {
		if err = sh.StartDownSample(id, sh.GetIdent().DownSampleLevel, sdsp, meta); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) UpdateDownSampleInfo(policies *meta2.DownSamplePoliciesInfoWithDbRp) {
	e.resetDownSampleFlag()
	defer func() {
		e.removeDeadDownSamplePolicy()
	}()
	for _, p := range policies.Infos {
		e.UpdateStoreDownSamplePolicies(p.Info, p.DbName+"."+p.RpName)
	}
}

func (e *Engine) UpdateShardDownSampleInfo(infos *meta2.ShardDownSampleUpdateInfos) {
	info := infos.Infos
	for i := range info {
		shards := e.DBPartitions[info[i].Ident.OwnerDb][info[i].Ident.OwnerPt].shards
		shards[info[i].Ident.ShardID].SetShardDownSampleLevel(info[i].DownSampleLvl)
	}
}

func (e *Engine) UpdateStoreDownSamplePolicies(info *meta2.DownSamplePolicyInfo, ident string) {
	policy, dbExist := e.DownSamplePolicies[ident]
	if !dbExist || !policy.Info.Equal(info, true) {
		e.DownSamplePolicies[ident] = &meta2.StoreDownSamplePolicy{
			Info:  info,
			Alive: true,
		}
		return
	}
	policy.Alive = true
}
