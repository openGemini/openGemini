// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

var MasterPtBalanceInterval = 10 * time.Second

type MasterPtBalanceManager struct {
	wg      sync.WaitGroup
	stopped int32
	algoFn  func()
}

func NewMasterPtBalanceManager() *MasterPtBalanceManager {
	bm := &MasterPtBalanceManager{}
	bm.algoFn = bm.balanceNodeHard
	return bm
}

// Start masterPtBalance goroutine
func (b *MasterPtBalanceManager) Start() {
	atomic.StoreInt32(&b.stopped, 0)
	b.wg.Add(1)
	go b.algoFn()
}

func (b *MasterPtBalanceManager) balanceNodeHard() {
	logger.GetLogger().Info("[masterPtBalanceNodeHard] algo start")
	defer logger.GetLogger().Info("[masterPtBalanceNodeHard] algo end")
	defer b.wg.Done()
	for {
		if atomic.LoadInt32(&b.stopped) == 1 || config.GetHaPolicy() != config.Replication {
			return
		}
		eventDbs, eventRgs, eventPts, eventPeers := globalService.store.selectUpdateRGEvents()

		for i, db := range eventDbs {
			logger.GetLogger().Info("[masterPtBalanceNodeHard] select event", zap.String("db", db), zap.Uint32("rgId", eventRgs[i]),
				zap.Uint32("ptId", eventPts[i]), zap.Any("peers", eventPeers[i]))
			err := globalService.store.updateReplication(db, eventRgs[i], eventPts[i], eventPeers[i])
			if err != nil {
				logger.GetLogger().Error("[masterPtBalanceNodeHard] updateReplication failed", zap.String("db", db),
					zap.Uint32("rgId", eventRgs[i]), zap.Uint32("ptId", eventPts[i]), zap.Any("peers", eventPeers[i]))
			}
		}
		time.Sleep(MasterPtBalanceInterval)
	}
}

// Stop masterPtBalance goroutine
func (b *MasterPtBalanceManager) Stop() {
	if !atomic.CompareAndSwapInt32(&b.stopped, 0, 1) {
		return
	}
	b.wg.Wait()
}
