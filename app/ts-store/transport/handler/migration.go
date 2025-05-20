// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
)

type MigrationProcessor struct {
	store storage.StoreEngine
	log   *logger.Logger
}

func NewPtProcessor(store *storage.Storage) *MigrationProcessor {
	return &MigrationProcessor{
		store: store,
		log:   logger.NewLogger(errno.ModuleHA),
	}
}

func (mp *MigrationProcessor) Handle(w spdy.Responser, data interface{}) error {
	req, ok := data.(*netstorage.PtRequest)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.PtRequest", data)
	}
	ptInfo := &meta2.DbPtInfo{}
	ptInfo.Unmarshal(req.GetPt())
	if len(ptInfo.Db) == 0 {
		return errno.NewError(errno.ErrMigrationRequestDB)
	}
	if ptInfo.Pti == nil {
		return errno.NewError(errno.ErrMigrationRequestPt)
	}
	connId := mp.store.GetConnId()
	aliveConnId := req.GetAliveConnId()
	mp.log.Info("Start MigrationProcessor", zap.Uint64("opId", req.GetOpId()), zap.String("type", meta2.MoveState(req.GetMigrateType()).String()),
		zap.String("db", ptInfo.Db), zap.Uint32("pt", ptInfo.Pti.PtId), zap.Uint64("ver", ptInfo.Pti.Ver),
		zap.Uint64("connId", connId), zap.Uint64("aliveConnId", aliveConnId))
	var err error
	rsp := netstorage.NewPtResponse()
	switch meta2.MoveState(req.GetMigrateType()) {
	case meta2.MovePreOffload:
		err = mp.store.PreOffload(req.GetOpId(), ptInfo)
	case meta2.MoveRollbackPreOffload:
		err = mp.store.RollbackPreOffload(req.GetOpId(), ptInfo)
	case meta2.MovePreAssign:
		err = errno.NewError(errno.DataNoAlive)
		if connId == aliveConnId {
			err = mp.store.PreAssign(req.GetOpId(), ptInfo)
		}
	case meta2.MoveOffload:
		err = mp.store.Offload(req.GetOpId(), ptInfo)
	case meta2.MoveAssign:
		err = errno.NewError(errno.DataNoAlive)
		if connId == aliveConnId {
			err = mp.store.Assign(req.GetOpId(), ptInfo)
		}
	default:
		mp.log.Error("error migrate type", zap.Int32("type", req.GetMigrateType()))
		err = errno.NewError(errno.ErrMigrationRequestPt)
	}

	rsp.Err = netstorage.MarshalError(err)
	return w.Response(rsp, true)
}
