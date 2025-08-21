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

package handler

import (
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/spdy"
	"go.uber.org/zap"
)

type ClearRepColdEvent struct {
	store storage.StoreEngine
	log   *logger.Logger
}

func NewClearRepColdEvent(store *storage.Storage) *ClearRepColdEvent {
	return &ClearRepColdEvent{
		store: store,
		log:   logger.NewLogger(errno.ModuleHA),
	}
}

func (mp *ClearRepColdEvent) Handle(w spdy.Responser, data interface{}) error {
	req, ok := data.(*msgservice.SendClearEventsRequest)
	if !ok {
		return errno.NewInvalidTypeError("*msgservice.SendClearEventsRequest", data)
	}
	mp.log.Info("Start clear rep cold event", zap.Uint64("nodeId", *req.NodeId), zap.Uint32("pt", *req.PtId),
		zap.String("db", *req.Database))
	rsp := msgservice.NewSendClearEventsResponse()
	err := mp.store.ClearRepCold(req)
	if err != nil {
		mp.log.Error("clear rep cold event error", zap.Uint64("nodeId", *req.NodeId), zap.Uint32("pt", *req.PtId), zap.String("db", *req.Database), zap.Error(err))
		rsp.Err = msgservice.MarshalError(err)
	}
	mp.log.Info("End ClearRepColdEvent", zap.Uint64("nodeId", *req.NodeId), zap.Uint32("oldPt", *req.PtId),
		zap.String("db", *req.Database))
	return w.Response(rsp, true)
}
