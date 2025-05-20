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

package handler

import (
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/spdy"
	"go.uber.org/zap"
)

type TransferLeadershipProcessor struct {
	store storage.StoreEngine
	log   *logger.Logger
}

func NewTransferLeadershipProcessor(store *storage.Storage) *TransferLeadershipProcessor {
	return &TransferLeadershipProcessor{
		store: store,
		log:   logger.NewLogger(errno.ModuleHA),
	}
}

func (mp *TransferLeadershipProcessor) Handle(w spdy.Responser, data interface{}) error {
	req, ok := data.(*netstorage.TransferLeadershipRequest)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.TransferLeadershipRequest", data)
	}
	mp.log.Info("Start TransferLeadership", zap.Uint64("nodeId", *req.NodeId), zap.Uint32("oldPt", *req.PtId),
		zap.Uint32("newPt", *req.NewMasterPtId), zap.String("db", *req.Database))
	rsp := netstorage.NewTransferLeadershipResponse()
	err := mp.store.TransferLeadership(*req.Database, *req.NodeId, *req.PtId, *req.NewMasterPtId)
	if err != nil {
		mp.log.Info("TransferLeadership fail", zap.Uint64("nodeId", *req.NodeId), zap.Uint32("oldPt", *req.PtId),
			zap.Uint32("newPt", *req.NewMasterPtId), zap.String("db", *req.Database))
		rsp.Err = netstorage.MarshalError(err)
	}
	mp.log.Info("End TransferLeadership", zap.Uint64("nodeId", *req.NodeId), zap.Uint32("oldPt", *req.PtId),
		zap.Uint32("newPt", *req.NewMasterPtId), zap.String("db", *req.Database))
	return w.Response(rsp, true)
}
