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

package handler

import (
	"fmt"
	"reflect"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/rpc"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	netdata "github.com/openGemini/openGemini/lib/netstorage/data"
	"go.uber.org/zap"
)

//go:generate tmpl -data=@handlers.tmpldata handlers.go.tmpl

type RPCHandler interface {
	SetMessage(binaryCodec codec.BinaryCodec) error
	SetStore(*storage.Storage)
	Process() (codec.BinaryCodec, error)
}

type BaseHandler struct {
	store *storage.Storage
}

func (h *BaseHandler) SetStore(store *storage.Storage) {
	h.store = store
}

func (h *BaseHandler) SetMessage(msg codec.BinaryCodec) error {
	return nil
}

func (h *BaseHandler) Abort() {
}

type DDLProcessor struct {
	store *storage.Storage
}

func NewDDLProcessor(store *storage.Storage) *DDLProcessor {
	return &DDLProcessor{
		store: store,
	}
}

func (p *DDLProcessor) Handle(w spdy.Responser, data interface{}) error {
	msg, ok := data.(*netstorage.DDLMessage)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.DDLMessage", data)
	}

	h := newHandler(msg.Typ)
	if h == nil {
		return fmt.Errorf("unsupported message type: %d", msg.Typ)
	}

	if err := h.SetMessage(msg.Data); err != nil {
		return err
	}

	h.SetStore(p.store)
	rspMsg, err := h.Process()
	if err != nil {
		return err
	}

	rspTyp := netstorage.GetResponseMessageType(msg.Typ)
	if rspTyp == netstorage.UnknownMessage {
		return fmt.Errorf("no response message type: %d", msg.Typ)
	}

	rsp := netstorage.NewDDLMessage(rspTyp, rspMsg)
	return w.Response(rsp, true)
}

type SysProcessor struct {
	store *storage.Storage
}

func NewSysProcessor(store *storage.Storage) *SysProcessor {
	return &SysProcessor{
		store: store,
	}
}

func (p *SysProcessor) Handle(w spdy.Responser, data interface{}) error {
	msg, ok := data.(*netstorage.SysCtrlRequest)
	if !ok {
		return executor.NewInvalidTypeError("*netstorage.SysCtrlRequest", data)
	}

	h := &SysCtrlCmd{req: msg, w: w}
	h.SetStore(p.store)
	return h.Process()
}

type SelectProcessor struct {
	store *storage.Storage
}

func NewSelectProcessor(store *storage.Storage) *SelectProcessor {
	return &SelectProcessor{
		store: store,
	}
}

func (p *SelectProcessor) Handle(w spdy.Responser, data interface{}) error {
	msg, ok := data.(*rpc.Message)
	if !ok {
		return executor.NewInvalidTypeError("*executor.RPCMessage", data)
	}

	req, ok := msg.Data().(*executor.RemoteQuery)
	if !ok {
		logger.GetLogger().Error("invalid message type",
			zap.String("exp", "*executor.RemoteQuery"),
			zap.String("got", reflect.TypeOf(msg.Data()).String()))
		return executor.NewInvalidTypeError("*executor.RemoteQuery", msg.Data())
	}

	logger.GetLogger().Info("SelectProcessor.Handle",
		zap.Uint64("clientID", msg.ClientID()),
		zap.Uint64("qid", req.QueryId),
		zap.String("database", req.Database),
		zap.Uint64s("ShardIDs", req.ShardIDs))

	go func() {
		qm := query.NewManager(msg.ClientID())
		if qm.Aborted(req.QueryId) {
			logger.GetLogger().Info("[SelectProcessor.Handle] aborted")
			_ = w.Response(executor.NewFinishMessage(), true)
			return
		}

		s := NewSelect(p.store, w, req)
		qm.Add(req.QueryId, s)

		query.MapQueryToClint(req.QueryId, msg.ClientID())

		w.Session().EnableDataACK()
		defer func() {
			w.Session().DisableDataACK()
			qm.Finish(req.QueryId)
		}()

		err := s.Process()
		s.Release()
		if err != nil {
			logger.GetLogger().Error("failed to process the query request", zap.Error(err))
			switch stderr := err.(type) {
			case *errno.Error:
				_ = w.Response(executor.NewErrorMessage(stderr.Errno(), stderr.Error()), true)
			default:
				_ = w.Response(executor.NewErrorMessage(0, stderr.Error()), true)
			}
			return
		}

		err = w.Response(executor.NewFinishMessage(), true)
		if err != nil {
			logger.GetLogger().Error("failed to response finish message", zap.Error(err))
		}
	}()

	return nil
}

type AbortProcessor struct {
}

func NewAbortProcessor() *AbortProcessor {
	return &AbortProcessor{}
}

func (p *AbortProcessor) Handle(w spdy.Responser, data interface{}) error {
	msg, ok := data.(*executor.Abort)
	if !ok {
		return executor.NewInvalidTypeError("*executor.Abort", data)
	}

	logger.GetLogger().Info("AbortProcessor.Handle",
		zap.Uint64("qid", msg.QueryID),
		zap.Uint64("clientID", msg.ClientID))

	query.NewManager(msg.ClientID).Abort(msg.QueryID)
	err := w.Response(executor.NewFinishMessage(), true)
	if err != nil {
		logger.GetLogger().Error("failed to response finish message", zap.Error(err))
	}
	return nil
}

type ShowQueriesProcessor struct {
}

func NewShowQueriesProcessor() *ShowQueriesProcessor {
	return &ShowQueriesProcessor{}
}

func (s *ShowQueriesProcessor) Handle(w spdy.Responser, _ interface{}) error {
	var queries []*netdata.QueryExeInfo

	// get all query exe info from all query managers
	getAllQueries := func(manager *query.Manager) {
		queries = append(queries, manager.GetAll()...)
	}
	query.VisitManagers(getAllQueries)

	rsp := netstorage.NewShowQueriesResponse()
	rsp.QueryExeInfos = queries
	err := w.Response(rsp, true)
	if err != nil {
		logger.GetLogger().Error("failed to response show queries", zap.Error(err))
	}
	return nil
}
