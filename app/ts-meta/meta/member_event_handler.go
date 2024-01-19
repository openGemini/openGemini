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
	"fmt"
	"strconv"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

type memberEventHandler interface {
	handle(e *memberEvent) error
}

type baseHandler struct {
	cm *ClusterManager
}

func (bh *baseHandler) handleEvent(m *serf.Member, e *serf.MemberEvent, id uint64, from eventFrom) error {
	// do not take over meta
	if m.Tags["role"] == "meta" || uint64(e.EventTime) == 0 {
		return nil
	}

	bh.cm.addEventMap(m.Name, e)
	logger.GetLogger().Info("handle event", zap.String("type", e.String()), zap.String("addr", m.Addr.String()),
		zap.String("name", m.Name), zap.Int("status", int(m.Status)), zap.Uint64("lTime", uint64(e.EventTime)),
		zap.Any("from", from))
	if bh.cm.isStopped() {
		return nil
	}

	err := bh.cm.store.updateNodeStatus(id, int32(m.Status), uint64(e.EventTime), fmt.Sprintf("%d", m.Port))
	if errno.Equal(err, errno.OlderEvent) || errno.Equal(err, errno.DataNodeSplitBrain) {
		logger.GetLogger().Error("ignore handle event", zap.String("name", m.Name), zap.String("event", e.String()), zap.Error(err))
		return nil
	}
	return err
}

func (bh *baseHandler) takeoverDbPts(id uint64) {
	// get pts and add to failed dbPts
	dbPtInfos := globalService.store.getFailedDbPts(id, meta.Offline)
	nodePtNumMap := globalService.store.getDbPtNumPerAliveNode()
	var err error
	for i := range dbPtInfos {
		err = bh.cm.processFailedDbPt(dbPtInfos[i], nodePtNumMap, false)
		if err != nil {
			// if process event failed migrate state machine will retry event when retry needed
			logger.NewLogger(errno.ModuleHA).Error("fail to take over db pt",
				zap.String("db", dbPtInfos[i].Db), zap.Uint32("pt", dbPtInfos[i].Pti.PtId), zap.Error(err))
		}
	}
}

func (bh *baseHandler) failOverForRep(id uint64) error {
	// get pts and add to failed dbPts
	start := time.Now()
	dbPtInfos := globalService.store.getFailedDbPts(id, meta.Offline)
	logger.NewLogger(errno.ModuleHA).Info("fail over for replication start", zap.Any("dbpt", dbPtInfos))

	var err error
	for i := range dbPtInfos {
		err = bh.cm.processReplication(dbPtInfos[i])
		if err != nil {
			// if process event failed migrate state machine will retry event when retry needed
			logger.NewLogger(errno.ModuleHA).Error("fail to process replication",
				zap.String("db", dbPtInfos[i].Db), zap.Uint32("pt", dbPtInfos[i].Pti.PtId))
			return err
		}
	}
	logger.NewLogger(errno.ModuleHA).Info("fail over for replication finish", zap.Duration("cost", time.Since(start)))
	return nil
}

type joinHandler struct {
	baseHandler
}

func (jh *joinHandler) handle(e *memberEvent) error {
	for i := range e.event.Members {
		id, err := strconv.ParseUint(e.event.Members[i].Name, 10, 64)
		if err != nil {
			panic(err)
		}
		if e.event.Members[i].Tags["role"] == "meta" {
			continue
		}
		err = jh.handleEvent(&e.event.Members[i], &e.event, id, e.from)
		if err != nil {
			return err
		}
		jh.cm.handleClusterMember(id, &e.event)
		failpoint.Label("retry")
		failpoint.Inject("stuckOnMemberJoinEvent", func(val failpoint.Value) {
			for {
				if v, ok := val.(bool); ok {
					if v {
						time.Sleep(time.Second)
						failpoint.Goto("retry")
						logger.GetLogger().Debug("stuck on store member join event")
					} else {
						logger.GetLogger().Debug("jump out of stuck on store member join event")
						failpoint.Break()
					}
				}
			}
		})
		jh.takeoverDbPts(id)
	}
	return nil
}

type failHandle func(fh *failedHandler, id uint64, event *serf.MemberEvent, member *serf.Member, from eventFrom) error

var failHandler []failHandle

func init() {
	failHandler = make([]failHandle, config.PolicyEnd)
	failHandler[config.WriteAvailableFirst] = failHandlerForSSOrWAF
	failHandler[config.SharedStorage] = failHandlerForSSOrWAF
	failHandler[config.Replication] = failHandlerForRep
}

type failedHandler struct {
	baseHandler
}

func failHandlerForRep(fh *failedHandler, id uint64, event *serf.MemberEvent, member *serf.Member, from eventFrom) error {
	err := fh.handleEvent(member, event, id, from)
	if err != nil {
		logger.GetLogger().Error("handle event failed", zap.String("name", member.Name), zap.String("event", event.String()),
			zap.Int("status", int(member.Status)), zap.Uint64("lTime", uint64(event.EventTime)),
			zap.Any("from", from), zap.Error(err))
		return err
	}
	failpoint.Label("retry")
	failpoint.Inject("stuckOnMemberFailEvent", func(val failpoint.Value) {
		for {
			if v, ok := val.(bool); ok {
				if v {
					time.Sleep(time.Second)
					logger.GetLogger().Debug("stuck on store member fail event for replication")
					failpoint.Goto("retry")
				} else {
					logger.GetLogger().Debug("jump out of stuck on store member fail event for replication")
					failpoint.Break()
				}
			}
		}
	})
	fh.cm.handleClusterMember(id, event)
	return fh.failOverForRep(id)
}

func failHandlerForSSOrWAF(fh *failedHandler, id uint64, event *serf.MemberEvent, member *serf.Member, from eventFrom) error {
	err := fh.handleEvent(member, event, id, from)
	if err != nil {
		logger.GetLogger().Error("handle event failed", zap.String("name", member.Name), zap.String("event", event.String()),
			zap.Int("status", int(member.Status)), zap.Uint64("lTime", uint64(event.EventTime)),
			zap.Any("from", from), zap.Error(err))
		return err
	}
	failpoint.Label("retry")
	failpoint.Inject("stuckOnMemberFailEvent", func(val failpoint.Value) {
		for {
			if v, ok := val.(bool); ok {
				if v {
					time.Sleep(time.Second)
					logger.GetLogger().Debug("stuck on store member fail event for sso or waf")
					failpoint.Goto("retry")
				} else {
					logger.GetLogger().Debug("jump out of stuck on store member fail event for sso or waf")
					failpoint.Break()
				}
			}
		}
	})
	fh.cm.handleClusterMember(id, event)
	fh.takeoverDbPts(id)
	return nil
}

func (fh *failedHandler) handle(e *memberEvent) error {
	for i := range e.event.Members {
		id, err := strconv.ParseUint(e.event.Members[i].Name, 10, 64)
		if err != nil {
			panic(err)
		}
		err = failHandler[config.GetHaPolicy()](fh, id, &e.event, &e.event.Members[i], e.from)
		if err != nil {
			return err
		}
	}
	return nil
}

type leaveHandler struct {
	baseHandler
}

func (lh *leaveHandler) handle(e *memberEvent) error {
	return nil
}
