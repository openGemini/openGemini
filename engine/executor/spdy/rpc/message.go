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

package rpc

import (
	"encoding/binary"

	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
)

type MessageNewHandler func(typ uint8) transport.Codec

type Message struct {
	typ      uint8
	data     transport.Codec
	clientID uint64

	handler MessageNewHandler
}

func NewMessage(typ uint8, data transport.Codec) *Message {
	return &Message{typ: typ, data: data}
}

func NewMessageWithHandler(h MessageNewHandler) *Message {
	return &Message{handler: h}
}

func (m *Message) SetData(typ uint8, data transport.Codec) {
	m.typ = typ
	m.data = data
}

func (m *Message) SetClientID(id uint64) {
	m.clientID = id
}

func (m *Message) SetHandler(h MessageNewHandler) {
	m.handler = h
}

func (m *Message) Type() uint8 {
	return m.typ
}

func (m *Message) Data() transport.Codec {
	return m.data
}

func (m *Message) ClientID() uint64 {
	return m.clientID
}

func (m *Message) Instance() transport.Codec {
	return &Message{handler: m.handler, clientID: m.clientID}
}

func (m *Message) Size() int {
	return m.data.Size() + 1 + codec.SizeOfUint64()
}

func (m *Message) Marshal(buf []byte) ([]byte, error) {
	buf = append(buf, m.typ)
	buf = codec.AppendUint64(buf, m.clientID)
	return m.data.Marshal(buf)
}

func (m *Message) Unmarshal(buf []byte) error {
	if len(buf) < 1+codec.SizeOfUint64() {
		return errno.NewError(errno.ShortBufferSize, 1+codec.SizeOfUint64(), len(buf))
	}

	m.typ = buf[0]
	m.data = m.handler(m.typ)
	if m.data == nil {
		return errno.NewError(errno.UnknownMessageType, m.typ)
	}
	m.clientID = binary.BigEndian.Uint64(buf[1:9])

	return m.data.Unmarshal(buf[9:])
}
