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

package meta

import (
	"errors"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

const RpPtBalanceInterval = 1 * time.Hour

type RpPtBalanceManager struct {
	services.Base
	enabled bool
	// db -> rp -> mst -> shardingPlan
	shardingPlans DBShardingPlans
	mu            sync.RWMutex
}

func NewRpPtBalanceManager(interval time.Duration, enabled bool) *RpPtBalanceManager {
	effectiveInterval := interval
	if effectiveInterval <= 0 {
		effectiveInterval = RpPtBalanceInterval
	}

	bm := &RpPtBalanceManager{
		enabled: enabled,
	}
	bm.Init("RpPtBalanceManager", effectiveInterval, bm.balanceRPPTWithLoads)
	return bm
}

// Start rpPtBalance goroutine
func (b *RpPtBalanceManager) Start() {
	if !b.enabled {
		return
	}
	b.Logger.Info("[balanceRPPTWithLoads] algo start")
	if err := b.Open(); err != nil {
		b.Logger.Error("[RpPtBalanceManager] Failed to start service", zap.Error(err))
	}
}

func (b *RpPtBalanceManager) balanceRPPTWithLoads() {
	shardingPlans, err := globalService.store.balanceRPPTWithLoads()
	if err != nil {
		b.Logger.Error("[balanceRPPTWithLoads] update sharding plans failed", zap.Error(err))
		b.mu.Lock()
		defer b.mu.Unlock()
		b.shardingPlans = nil
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.shardingPlans = shardingPlans
}

func (b *RpPtBalanceManager) GetShardingPlans(dbName string, rpName string) ([]byte, error) {
	if !b.enabled {
		return nil, errors.New("RpPtBalanceManager disabled")
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	rpShardingPlans, ok := b.shardingPlans[dbName]
	if !ok {
		return nil, nil
	}
	shardingPlans, ok := rpShardingPlans[rpName]
	if !ok {
		return nil, nil
	}
	plan := meta.RPPTShardingPlan{
		DB:    dbName,
		RP:    rpName,
		Idxes: shardingPlans,
	}
	return plan.MarshalBinaryPlans()
}

// Stop rpPtBalance goroutine
func (b *RpPtBalanceManager) Stop() {
	if err := b.Close(); err != nil {
		b.Logger.Error("[RpPtBalanceManager] Failed to stop service", zap.Error(err))
	}
}
