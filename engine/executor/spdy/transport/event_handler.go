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

package transport

import (
	"github.com/openGemini/openGemini/engine/executor/spdy"
)

type EventHandler struct {
	spdy.BaseEventHandler
	handler Handler
	codec   Codec
	typ     uint8
}

func NewEventHandler(session *spdy.MultiplexedSession, typ uint8, handler Handler, codec Codec) *EventHandler {
	eh := &EventHandler{
		handler: handler,
		codec:   codec,
		typ:     typ,
	}
	eh.InitDerive(session, eh)
	return eh
}

func (eh *EventHandler) CreateRequester(seq uint64) (spdy.Requester, error) {
	requester := NewRequester(eh.Session(), eh.typ, seq, eh.codec)
	return requester, nil
}

func (eh *EventHandler) Handle(r spdy.Requester, w spdy.Responser) error {
	return eh.handler.Handle(w, r.Data())
}

type EventHandlerFactory struct {
	handler Handler
	typ     uint8
	codec   Codec
}

func NewEventHandlerFactory(typ uint8, handler Handler, codec Codec) EventHandlerFactory {
	return EventHandlerFactory{
		typ:     typ,
		handler: handler,
		codec:   codec,
	}
}

func (f EventHandlerFactory) CreateEventHandler(session *spdy.MultiplexedSession) spdy.EventHandler {
	return NewEventHandler(session, f.typ, f.handler, f.codec)
}

func (f EventHandlerFactory) EventHandlerType() uint8 {
	return f.typ
}
