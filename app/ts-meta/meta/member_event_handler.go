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
	"net"
	"strconv"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type memberEventHandler interface {
	handle(e *memberEvent) error
}

type baseHandler struct {
	cm *ClusterManager
}

func (bh *baseHandler) handleEvent(m *serf.Member, e *serf.MemberEvent) error {
	// do not take over meta
	if m.Tags["role"] == "meta" || uint64(e.EventTime) == 0 {
		return nil
	}

	bh.cm.addEventMap(m.Name, e)
	logger.NewLogger(errno.ModuleHA).Error("handle event", zap.String("eventType", e.String()), zap.String("addr", m.Addr.String()),
		zap.String("name", m.Name), zap.Int("status", int(m.Status)))
	if bh.cm.isStopped() {
		return nil
	}
	// change store status to alive
	id, err := strconv.Atoi(m.Name)
	if err != nil {
		panic(err)
	}
	addr := net.TCPAddr{IP: m.Addr, Port: int(m.Port)}
	err = bh.cm.store.updateNodeStatus(uint64(id), int32(m.Status), uint64(e.EventTime), addr.String())
	return err
}

type joinHandler struct {
	baseHandler
}

func (jh *joinHandler) handle(e *memberEvent) error {
	for i := range e.event.Members {
		err := jh.handleEvent(&e.event.Members[i], &e.event)
		if err != nil {
			return err
		}
	}
	return nil
}

type failedHandler struct {
	baseHandler
}

func (fh *failedHandler) handle(e *memberEvent) error {
	for i := range e.event.Members {
		err := fh.handleEvent(&e.event.Members[i], &e.event)
		if err != nil {
			logger.GetLogger().Error("handle event failed", zap.Error(err))
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
