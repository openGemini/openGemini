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
	"sync"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/open_src/influx/meta"
)

//go:generate tmpl -data=@../tmpldata handlers.gen.go.tmpl

type MetaStoreInterface interface {
	leader() string
	peers() []string
	createDataNode(httpAddr, tcpAddr string) ([]byte, error)
	afterIndex(index uint64) <-chan struct{}
	getSnapshot(role metaclient.Role) []byte
	isCandidate() bool
	Join(n *meta.NodeInfo) (*meta.NodeInfo, error)
	apply(b []byte) error
	index() uint64
	UpdateLoad(b []byte) error
	getShardAuxInfo(body []byte) ([]byte, error)
	GetDownSampleInfo() ([]byte, error)
	GetRpMstInfos(db, rp string, dataTypes []int64) ([]byte, error)
	GetUserInfo() ([]byte, error)
	getStreamInfo() ([]byte, error)
	getMeasurementInfo(dbName, rpName, mstName string) ([]byte, error)
	getMeasurementsInfo(dbName, rpName string) ([]byte, error)
	getDBBriefInfo(dbName string) ([]byte, error)
	getDataNodeAliveConnId(nodeId uint64) (uint64, error)
	registerQueryIDOffset(host meta.SQLHost) (uint64, error)
}

type RPCHandler interface {
	SetRequestMsg(transport.Codec) error
	Process() (transport.Codec, error)
	Instance() RPCHandler
	InitHandler(MetaStoreInterface, *config.Meta, chan struct{})
}

type BaseHandler struct {
	mu     sync.RWMutex
	config *config.Meta
	store  MetaStoreInterface

	logger *logger.Logger

	closing chan struct{}
}

func (h *BaseHandler) Process() (transport.Codec, error) {
	return nil, nil
}

func (h *BaseHandler) InitHandler(store MetaStoreInterface, conf *config.Meta, closing chan struct{}) {
	h.store = store
	h.config = conf

	h.logger = logger.NewLogger(errno.ModuleMeta)

	h.closing = closing
}

func (h *BaseHandler) isClosed() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	select {
	case <-h.closing:
		return true
	default:
		return false
	}
}

type Processor struct {
	config *config.Meta
	store  MetaStoreInterface

	closing chan struct{}
}

func (p *Processor) Clone() transport.Handler {
	return &Processor{}
}

func NewProcessor(conf *config.Meta, store MetaStoreInterface) *Processor {
	return &Processor{
		config: conf,
		store:  store,

		closing: make(chan struct{}),
	}
}

func (p *Processor) Handle(w spdy.Responser, data interface{}) error {
	msg, ok := data.(*message.MetaMessage)
	if !ok {
		return executor.NewInvalidTypeError("message.MetaMessage", data)
	}
	h := New(msg.Type())
	h.InitHandler(p.store, p.config, p.closing)
	err := h.SetRequestMsg(msg.Data())
	if err != nil {
		return fmt.Errorf("unsupported message type: %d", msg.Type())
	}

	resp, err := h.Process()
	if err != nil {
		return err
	}
	respTyp := message.GetResponseMessageType(msg.Type())
	return w.Response(message.NewMetaMessage(respTyp, resp), true)
}
