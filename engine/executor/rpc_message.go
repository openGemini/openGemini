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

package executor

import (
	"reflect"

	"github.com/openGemini/openGemini/engine/executor/spdy/rpc"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/query"
	proto2 "github.com/openGemini/openGemini/open_src/influx/query/proto"
	"google.golang.org/protobuf/proto"
)

func NewRPCMessage(typ uint8) transport.Codec {
	var data transport.Codec

	switch typ {
	case ErrorMessage:
		data = &Error{}
	case ChunkResponseMessage:
		data = &ChunkImpl{}
	case AnalyzeResponseMessage:
		data = &AnalyzeResponse{}
	case QueryMessage:
		data = &RemoteQuery{}
	case FinishMessage:
		data = &Finish{}
	default:
		return nil
	}
	return data
}

type Error struct {
	data string
}

func NewErrorMessage(err string) *rpc.Message {
	return rpc.NewMessage(ErrorMessage, &Error{data: err})
}

func (e *Error) Marshal(buf []byte) ([]byte, error) {
	buf = append(buf, e.data...)
	return buf, nil
}

func (e *Error) Unmarshal(buf []byte) error {
	e.data = string(buf)
	return nil
}

func (e *Error) Size() int {
	return len(e.data)
}

func (e *Error) Instance() transport.Codec {
	return &Error{}
}

type Finish struct {
}

func NewFinishMessage() *rpc.Message {
	return rpc.NewMessage(FinishMessage, &Finish{})
}

func (e *Finish) Marshal(buf []byte) ([]byte, error) {
	return buf, nil
}

func (e *Finish) Unmarshal(_ []byte) error {
	return nil
}

func (e *Finish) Size() int {
	return 0
}

func (e *Finish) Instance() transport.Codec {
	return &Finish{}
}

type Abort struct {
	ClientID uint64
	Seq      uint64
}

func NewAbort(seq uint64, clientID uint64) *Abort {
	return &Abort{ClientID: clientID, Seq: seq}
}

func (e *Abort) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendUint64(buf, e.ClientID)
	buf = codec.AppendUint64(buf, e.Seq)
	return buf, nil
}

func (e *Abort) Unmarshal(buf []byte) error {
	dec := codec.NewBinaryDecoder(buf)
	e.ClientID = dec.Uint64()
	e.Seq = dec.Uint64()
	return nil
}

func (e *Abort) Size() int {
	return codec.SizeOfUint64() * 2
}

func (e *Abort) Instance() transport.Codec {
	return &Abort{}
}

type AnalyzeResponse struct {
	trace *tracing.Trace
}

func NewAnalyzeResponse(trace *tracing.Trace) *rpc.Message {
	return rpc.NewMessage(AnalyzeResponseMessage, &AnalyzeResponse{
		trace: trace,
	})
}

func (a *AnalyzeResponse) Marshal(buf []byte) ([]byte, error) {
	b, err := a.trace.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf = append(buf, b...)
	return buf, nil
}

func (a *AnalyzeResponse) Unmarshal(buf []byte) error {
	a.trace, _ = tracing.NewTrace("root")
	return a.trace.UnmarshalBinary(buf)
}

func (a *AnalyzeResponse) Size() int {
	return 0
}

func (a AnalyzeResponse) Instance() transport.Codec {
	return &AnalyzeResponse{}
}

func NewChunkResponse(chunk Chunk) *rpc.Message {
	return rpc.NewMessage(ChunkResponseMessage, chunk)
}

type RemoteQuery struct {
	Database string
	PtID     uint32
	NodeID   uint64
	ShardIDs []uint64
	Opt      query.ProcessorOptions
	Analyze  bool
	Node     []byte
}

func (c *RemoteQuery) Marshal(buf []byte) ([]byte, error) {
	opt, err := c.Opt.MarshalBinary()
	if err != nil {
		return nil, err
	}

	msg, err := proto.Marshal(&proto2.RemoteQuery{
		Database:  c.Database,
		PtID:      c.PtID,
		ShardIDs:  c.ShardIDs,
		NodeID:    c.NodeID,
		Opt:       opt,
		Analyze:   c.Analyze,
		QueryNode: c.Node,
	})

	ret := make([]byte, len(buf)+len(msg))
	copy(ret, buf)
	copy(ret[len(buf):], msg)
	return ret, err
}

func (c *RemoteQuery) Unmarshal(buf []byte) error {
	var pb proto2.RemoteQuery
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}

	c.Database = pb.GetDatabase()
	c.PtID = pb.GetPtID()
	c.ShardIDs = pb.GetShardIDs()
	c.NodeID = pb.GetNodeID()
	c.Analyze = pb.GetAnalyze()
	c.NodeID = pb.GetNodeID()
	c.Node = pb.QueryNode

	if err := c.Opt.UnmarshalBinary(pb.GetOpt()); err != nil {
		return err
	}
	return nil
}

func (c *RemoteQuery) Size() int {
	return 0
}

func (c *RemoteQuery) Instance() transport.Codec {
	return &RemoteQuery{}
}

func NewInvalidTypeError(exp string, data interface{}) error {
	return errno.NewError(errno.InvalidDataType, exp, reflect.TypeOf(data).String())
}
