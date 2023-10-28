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

package handler

import (
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"go.uber.org/zap"
)

type SegregateProcessor struct {
	store storage.StoreEngine
	log   *logger.Logger
}

func NewSegregateNodeProcessor(store *storage.Storage) *SegregateProcessor {
	return &SegregateProcessor{
		store: store,
		log:   logger.NewLogger(errno.ModuleHA),
	}
}

func (mp *SegregateProcessor) Handle(w spdy.Responser, data interface{}) error {
	req, ok := data.(*netstorage.SegregateNodeRequest)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.SegregateNodeRequest", data)
	}
	connId := mp.store.GetConnId()
	mp.log.Info("Start CheckPtsRemovedDone", zap.Uint64("connId", connId), zap.Uint64("nodeid", *req.NodeId))
	rsp := netstorage.NewSegregateNodeResponse()
	err := mp.store.CheckPtsRemovedDone()
	if err != nil {
		mp.log.Info("CheckPtsRemovedDone fail", zap.Uint64("connId", connId), zap.Uint64("nodeid", *req.NodeId))
		rsp.Err = netstorage.MarshalError(err)
	}
	mp.log.Info("CheckPtsRemovedDone succuss", zap.Uint64("connId", connId), zap.Uint64("nodeid", *req.NodeId))
	return w.Response(rsp, true)
}
