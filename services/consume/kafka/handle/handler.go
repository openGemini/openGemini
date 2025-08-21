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

package handle

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
	"go.uber.org/zap"
)

const (
	Fetch        = 1
	ListOffsets  = 2
	Metadata     = 3
	OffsetCommit = 8
	HeartBeat    = 12
	Versions     = 18

	V1 = 1
	V2 = 2
)

var defaultHandlerFactory = &HandlerFactory{creators: make(map[uint32]HandlerCreator)}

type HandlerCreator func() Handler
type OnMessage func(protocol.Marshaler) error

func DefaultHandlerFactory() *HandlerFactory {
	return defaultHandlerFactory
}

func handlerKey(id, ver uint16) uint32 {
	return uint32(id)<<16 | uint32(ver)
}

type Handler interface {
	Handle(header protocol.RequestHeader, buf []byte, onMessage OnMessage) error
}

type HandlerFactory struct {
	creators map[uint32]HandlerCreator
}

func (hf *HandlerFactory) RegisterV1(id uint16, creator HandlerCreator) {
	hf.register(id, V1, creator)
}

func (hf *HandlerFactory) RegisterV2(id uint16, creator HandlerCreator) {
	hf.register(id, V2, creator)
}

func (hf *HandlerFactory) register(id uint16, ver uint16, creator HandlerCreator) {
	hf.creators[handlerKey(id, ver)] = creator
}

func (hf *HandlerFactory) Create(id uint16, ver uint16) Handler {
	key := handlerKey(id, ver)
	creator, ok := hf.creators[key]
	if !ok || creator == nil {
		return nil
	}

	return creator()
}

type HandlerManager struct {
	lg      *logger.Logger
	handles map[uint32]Handler
}

func NewHandlerManager() *HandlerManager {
	hm := &HandlerManager{
		lg:      logger.NewLogger(errno.ModuleConsume),
		handles: make(map[uint32]Handler),
	}

	hm.regDef(Fetch, V2)
	hm.regDef(ListOffsets, V1)
	hm.regDef(Metadata, V1)
	hm.regDef(Versions, V1)
	hm.regDef(OffsetCommit, V2)
	hm.regDef(HeartBeat, V1)

	return hm
}

func (hm *HandlerManager) regDef(id, ver uint16) {
	hm.handles[handlerKey(id, ver)] = DefaultHandlerFactory().Create(id, ver)
}

func (hm *HandlerManager) Call(header protocol.RequestHeader, body []byte, onMessage OnMessage) error {
	ver := max(1, header.ApiVersion)
	key := header.ApiKey

	hm.lg.Debug("handle request", zap.Uint16("api key", key), zap.Uint16("version", ver))

	h, ok := hm.handles[handlerKey(key, ver)]
	if !ok || h == nil {
		return fmt.Errorf("unsupported API: %d, version: %d", key, ver)
	}
	return h.Handle(header, body, onMessage)
}

func (hm *HandlerManager) MustClose() {

}
