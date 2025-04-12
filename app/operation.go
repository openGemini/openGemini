/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package app

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/memory"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

const (
	killCntOnce        = int(5)
	periodOfInspection = 100 * time.Millisecond
)

type ProactiveManager struct {
	mu            sync.RWMutex
	wg            sync.WaitGroup
	started       bool
	closing       chan struct{}
	logger        *logger.Logger
	checkInterval time.Duration
}

func NewProactiveManager() *ProactiveManager {
	pm := &ProactiveManager{
		started:       false,
		closing:       make(chan struct{}),
		logger:        logger.NewLogger(errno.ModuleStorageEngine),
		checkInterval: periodOfInspection,
	}
	return pm
}

func (pm *ProactiveManager) SetInspectInterval(interval time.Duration) {
	if interval == time.Duration(0) {
		pm.logger.Info("ignore set, invalid interval value 0")
		return
	}
	pm.checkInterval = interval
}

func (pm *ProactiveManager) Open() error {
	if pm.started {
		return errors.New("proactive manager service already starting")
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.started {
		return errors.New("proactive manager service already starting")
	}

	pm.started = true
	pm.wg.Add(1)
	go pm.runInspection()
	return nil
}

func (pm *ProactiveManager) isClosed() bool {
	select {
	case <-pm.closing:
		return true
	default:
	}
	return false
}

func (pm *ProactiveManager) Close() error {
	if pm.isClosed() {
		return errors.New("proactive manager service is already closed")
	}

	pm.logger.Info("Closing proactive manager service")
	close(pm.closing)
	pm.wg.Wait()
	return nil
}

func (pm *ProactiveManager) WithLogger(logger *logger.Logger) {
	pm.logger = logger
}

func (pm *ProactiveManager) KillQuery(id uint64) {
	pm.logger.Info("proactive manager, execute kill query", zap.Uint64("id", id))
	var isExist bool
	var abortSuccess bool
	killQueryByIDFn := func(manager *query.Manager) {
		// qid is not in current manager, or it has been aborted successfully
		if len(manager.Get(id)) == 0 || abortSuccess {
			return
		}
		isExist = true
		manager.Kill(id)
		manager.Crash(id)
		abortSuccess = true
	}
	query.VisitManagers(killQueryByIDFn)
	if !isExist {
		pm.logger.Error("kill query err, not exist ", zap.Uint64("id", id))
	}
}

func (pm *ProactiveManager) GetQueryList(cnt int) []uint64 {
	qryLst := query.GetAllQueries()

	sort.SliceStable(qryLst, func(i, j int) bool {
		return qryLst[i].BeginTime < qryLst[j].BeginTime
	})

	ids := make([]uint64, 0, cnt)
	for i := range qryLst {
		if cnt <= 0 {
			break
		}
		if qryLst[i].RunState != netstorage.Running {
			continue
		}
		ids = append(ids, qryLst[i].QueryID)
		pm.logger.Info("will execute kill", zap.Uint64("query id", qryLst[i].QueryID),
			zap.String("sql", qryLst[i].Stmt), zap.Duration("sql duration", time.Duration(time.Now().UnixNano()-qryLst[i].BeginTime)))
		cnt--
	}
	return ids
}

func (pm *ProactiveManager) interruptQuery(memPct float64) {
	if !sysconfig.GetInterruptQuery() {
		return
	}

	// set-mem-use-percent: set the percentage of memory used by the system
	failpoint.Inject("set-mem-use-percent", func(val failpoint.Value) {
		if n, ok := val.(int); ok {
			memPct = float64(n)
		} else if m, ok := val.(float64); ok {
			memPct = m
		}
	})

	upperMemPct := sysconfig.GetUpperMemPct()
	if int64(memPct) <= upperMemPct {
		return
	}

	// kill the xx queries that take the longest execution elapsed time each time.
	idLst := pm.GetQueryList(killCntOnce)
	for _, id := range idLst {
		pm.KillQuery(id)
	}
}

func (pm *ProactiveManager) inspectMem() {
	if !sysconfig.GetInterruptQuery() {
		return
	}
	currMemPct := memory.MemUsedPct()
	pm.interruptQuery(currMemPct)
}

func (pm *ProactiveManager) runInspection() {
	defer pm.wg.Done()
	pm.logger.Info("start inspect", zap.Duration("interval", pm.checkInterval))
	ticker := time.NewTicker(pm.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-pm.closing:
			return
		case <-ticker.C:
			pm.inspectMem()
		}
	}
}
