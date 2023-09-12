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

package meta

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

var balanceInterval = 10 * time.Second

type BalanceManager struct {
	wg      sync.WaitGroup
	stopped int32
}

func NewBalanceManager() *BalanceManager {
	return &BalanceManager{}
}

// Start balance goroutine
func (b *BalanceManager) Start() {
	atomic.StoreInt32(&b.stopped, 0)
	b.wg.Add(1)
	go b.balanceIfNeeded()
}

func (b *BalanceManager) balanceIfNeeded() {
	defer b.wg.Done()
	for {
		if atomic.LoadInt32(&b.stopped) == 1 || config.GetHaPolicy() != config.SharedStorage {
			return
		}
		events := globalService.store.selectDbPtsToMove()
		for _, e := range events {
			err := globalService.msm.executeEvent(e)
			if err != nil {
				logger.GetLogger().Error("[balancer] balance failed", zap.Any("event", e))
			}
		}

		time.Sleep(balanceInterval)
	}
}

// Stop balance goroutine
func (b *BalanceManager) Stop() {
	if !atomic.CompareAndSwapInt32(&b.stopped, 0, 1) {
		return
	}
	b.wg.Wait()
}

func (b *BalanceManager) assignDbPt(dbPt *meta.DbPtInfo, target uint64, aliveConnId uint64, userCommand bool) error {
	me := NewAssignEvent(dbPt, target, aliveConnId, userCommand)
	return globalService.msm.executeEvent(me)
}
