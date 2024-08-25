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

package spdy

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/tracing"
)

const (
	ProtocolVersion        uint8 = 0
	SizeOfProtocolVersion        = 1
	SizeOfProtocolType           = 1
	SizeOfProtocolFlags          = 2
	SizeOfProtocolSequence       = 8
	SizeOfProtocolLength         = 4
	ProtocolHeaderSize           = SizeOfProtocolVersion + SizeOfProtocolType +
		SizeOfProtocolFlags + SizeOfProtocolSequence + SizeOfProtocolLength
)

const (
	ReqFlag uint16 = 1 << iota
	RspFlag
	FullFlag
)

const (
	Prototype uint8 = iota
	Echo
	Partial
	FaultPartial
	SelectRequest
	AbortRequest
	DDLRequest
	SysCtrlRequest
	MetaRequest
	WritePointsRequest
	PtRequest
	WriteStreamPointsRequest
	SegregateNodeRequest
	CrashRequest
	Unknown
)

type ProtocolHeader []byte

func (h ProtocolHeader) Version() uint8 {
	return h[0]
}

func (h ProtocolHeader) Type() uint8 {
	return h[1]
}

func (h ProtocolHeader) Flags() uint16 {
	return binary.BigEndian.Uint16(h[2:4])
}

func (h ProtocolHeader) Sequence() uint64 {
	return binary.BigEndian.Uint64(h[4:12])
}

func (h ProtocolHeader) Length() uint32 {
	return binary.BigEndian.Uint32(h[12:16])
}

func (h ProtocolHeader) String() string {
	return fmt.Sprintf("Vsn:%d Type:%d Flags:%d Sequence:%d Length:%d",
		h.Version(), h.Type(), h.Flags(), h.Sequence(), h.Length())
}

func (h ProtocolHeader) encode(typ uint8, flags uint16, sequence uint64, length uint32) {
	h[0] = ProtocolVersion
	h[1] = typ
	binary.BigEndian.PutUint16(h[2:4], flags)
	binary.BigEndian.PutUint64(h[4:12], sequence)
	binary.BigEndian.PutUint32(h[12:16], length)
}

type Selector interface {
	Select() ([]byte, bool)
}

type Closer interface {
	Close() error
}

type Codec interface {
	Encode([]byte, interface{}) ([]byte, error)
	Decode([]byte) (interface{}, error)
}

type Requester interface {
	Codec
	Request(interface{}) error
	Data() interface{}
	SetData(interface{})
	WarpResponser() Responser
	Type() uint8
	Session() *MultiplexedSession
	Sequence() uint64
	StartAnalyze(span *tracing.Span)
	FinishAnalyze()
}

type Responser interface {
	Codec
	Response(interface{}, bool) error
	Callback(interface{}) error
	Apply() error
	Type() uint8
	Session() *MultiplexedSession
	Sequence() uint64
	StartAnalyze(span *tracing.Span)
	FinishAnalyze()
}

type BaseRequester struct {
	session  *MultiplexedSession
	sequence uint64
	derive   Requester

	encodeSpan *tracing.Span
	sendSpan   *tracing.Span
}

func (base *BaseRequester) InitDerive(session *MultiplexedSession, sequence uint64, derive Requester) {
	base.session = session
	base.sequence = sequence
	base.derive = derive
}

func (base *BaseRequester) sendFlags() uint16 {
	return ReqFlag
}

func (base *BaseRequester) Request(request interface{}) error {
	begin := time.Now()
	buf := base.session.conn.AllocData(ProtocolHeaderSize)
	buf, err := base.derive.Encode(buf, request)
	if err != nil {
		return err
	}

	ProtocolHeader(buf).encode(base.derive.Type(), base.sendFlags(), base.sequence, uint32(len(buf)-ProtocolHeaderSize))
	tracing.AddPP(base.encodeSpan, begin)

	begin = time.Now()
	if err := base.session.Send(buf); err != nil {
		return err
	}
	tracing.AddPP(base.sendSpan, begin)

	return nil
}

func (base *BaseRequester) Session() *MultiplexedSession {
	return base.session
}

func (base *BaseRequester) Sequence() uint64 {
	return base.sequence
}

func (base *BaseRequester) StartAnalyze(span *tracing.Span) {
	if span == nil {
		return
	}

	base.sendSpan = span.StartSpan("spdy_send_message")
	base.encodeSpan = span.StartSpan("spdy_encode_message")
}

func (base *BaseRequester) FinishAnalyze() {
	tracing.Finish(base.sendSpan, base.encodeSpan)
}

type BaseResponser struct {
	session  *MultiplexedSession
	sequence uint64
	derive   Responser

	encodeSpan *tracing.Span
	decodeSpan *tracing.Span
	sendSpan   *tracing.Span
	waitSpan   *tracing.Span
}

func (base *BaseResponser) InitDerive(session *MultiplexedSession, sequence uint64, derive Responser) {
	base.session = session
	base.sequence = sequence
	base.derive = derive
}

func (base *BaseResponser) Close() error {
	tracing.Finish(base.sendSpan, base.encodeSpan)
	return nil
}

func (base *BaseResponser) IsFull() bool {
	return true
}

func (base *BaseResponser) sendFlags(full bool) uint16 {
	flags := RspFlag
	if full {
		flags |= FullFlag
	}
	return flags
}

func (base *BaseResponser) Response(response interface{}, full bool) error {
	begin := time.Now()
	buf := base.session.conn.AllocData(ProtocolHeaderSize)
	buf, err := base.derive.Encode(buf, response)
	if err != nil {
		return err
	}

	ProtocolHeader(buf).encode(base.derive.Type(), base.sendFlags(full), base.sequence, uint32(len(buf)-ProtocolHeaderSize))
	tracing.AddPP(base.encodeSpan, begin)

	begin = time.Now()
	if err := base.session.Send(buf); err != nil {
		return err
	}
	tracing.AddPP(base.sendSpan, begin)

	return nil
}

func (base *BaseResponser) Apply() error {
	defer func() {
		tracing.Finish(base.waitSpan, base.decodeSpan)
	}()

	for {
		tracing.StartPP(base.waitSpan)
		data, err := base.session.Select()
		tracing.EndPP(base.waitSpan)

		if err != nil {
			return err
		}
		if data == nil {
			return errno.NewError(errno.ResponserClosed)
		}
		header := ProtocolHeader(data)
		if header.Sequence() != base.sequence {
			continue
		}
		if header.Version() != ProtocolVersion {
			return ErrorInvalidProtocolVersion
		}
		if header.Type() != base.derive.Type() {
			return ErrorInvalidProtocolType
		}
		if header.Flags()&RspFlag != RspFlag {
			return ErrorUnexpectedResponse
		}
		completes := false
		if header.Flags()&FullFlag == FullFlag {
			completes = true
		}

		tracing.StartPP(base.decodeSpan)
		response, err := base.derive.Decode(data[ProtocolHeaderSize:])
		base.session.conn.FreeData(data)
		tracing.EndPP(base.decodeSpan)

		if err != nil {
			return err
		}

		if err := base.derive.Callback(response); err != nil {
			return err
		}

		if completes {
			return nil
		}
	}
}

func (base *BaseResponser) Session() *MultiplexedSession {
	return base.session
}

func (base *BaseResponser) Sequence() uint64 {
	return base.sequence
}

func (base *BaseResponser) StartAnalyze(span *tracing.Span) {
	if span == nil {
		return
	}

	base.sendSpan = span.StartSpan("spdy_send_message")
	base.encodeSpan = span.StartSpan("spdy_encode_message")
	base.decodeSpan = span.StartSpan("spdy_decode_message")
	base.waitSpan = span.StartSpan("spdy_wait_and_transfer")
}

func (base *BaseResponser) FinishAnalyze() {
	tracing.Finish(base.sendSpan, base.encodeSpan)
}

type EventHandler interface {
	CreateRequester(uint64) (Requester, error)
	WarpRequester(uint64, []byte) (Requester, error)
	Handle(Requester, Responser) error
	Session() *MultiplexedSession
}

type BaseEventHandler struct {
	session *MultiplexedSession
	derive  EventHandler
}

func (base *BaseEventHandler) InitDerive(session *MultiplexedSession, derive EventHandler) {
	base.session = session
	base.derive = derive
}

func (base *BaseEventHandler) WarpRequester(sequence uint64, data []byte) (Requester, error) {
	requester, err := base.derive.CreateRequester(sequence)
	if err != nil {
		return nil, err
	}
	req, err := requester.Decode(data)
	if err != nil {
		return nil, err
	}
	requester.SetData(req)
	return requester, nil
}

func (base *BaseEventHandler) Session() *MultiplexedSession {
	return base.session
}

type EventHandlerFactory interface {
	CreateEventHandler(session *MultiplexedSession) EventHandler
	EventHandlerType() uint8
}
