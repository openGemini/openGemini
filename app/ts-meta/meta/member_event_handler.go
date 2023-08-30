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

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

type memberEventHandler interface {
	handle(e *memberEvent) error
}

type baseHandler struct {
	cm *ClusterManager
}

func (bh *baseHandler) handleEvent(m *serf.Member, e *serf.MemberEvent, id uint64) error {
	// do not take over meta
	if m.Tags["role"] == "meta" || uint64(e.EventTime) == 0 {
		return nil
	}

	bh.cm.addEventMap(m.Name, e)
	logger.GetLogger().Info("handle event", zap.String("type", e.String()), zap.String("addr", m.Addr.String()),
		zap.String("name", m.Name), zap.Int("status", int(m.Status)), zap.Uint64("lTime", uint64(e.EventTime)))
	if bh.cm.isStopped() {
		return nil
	}

	err := bh.cm.store.updateNodeStatus(id, int32(m.Status), uint64(e.EventTime), fmt.Sprintf("%d", m.Port))
	if errno.Equal(err, errno.OlderEvent) || errno.Equal(err, errno.DataNodeSplitBrain) {
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
				zap.String("db", dbPtInfos[i].Db), zap.Uint32("pt", dbPtInfos[i].Pti.PtId))
		}
	}
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
		err = jh.handleEvent(&e.event.Members[i], &e.event, id)
		if err != nil {
			return err
		}
		jh.cm.addClusterMember(id)
		jh.takeoverDbPts(id)
	}
	return nil
}

type failedHandler struct {
	baseHandler
}

func (fh *failedHandler) handle(e *memberEvent) error {
	for i := range e.event.Members {
		id, err := strconv.ParseUint(e.event.Members[i].Name, 10, 64)
		if err != nil {
			panic(err)
		}
		err = fh.handleEvent(&e.event.Members[i], &e.event, id)
		if err != nil {
			logger.GetLogger().Error("handle event failed", zap.Error(err))
			return err
		}
		fh.cm.removeClusterMember(id)
		fh.takeoverDbPts(id)
	}
	return nil
}

type leaveHandler struct {
	baseHandler
}

func (lh *leaveHandler) handle(e *memberEvent) error {
	return nil
}
