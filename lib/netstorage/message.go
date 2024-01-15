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

package netstorage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	netdata "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
)

type BaseMessage struct {
	Typ  uint8
	Data codec.BinaryCodec
}

func (bm *BaseMessage) init(typ uint8, data codec.BinaryCodec) {
	bm.Typ = typ
	bm.Data = data
}

func (bm *BaseMessage) Marshal(buf []byte) ([]byte, error) {
	marshal, err := bm.Data.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf = append(buf, bm.Typ)
	buf = append(buf, marshal...)
	return buf, err
}

func (bm *BaseMessage) Size() int {
	return 0
}

type DDLMessage struct {
	BaseMessage
}

func NewDDLMessage(typ uint8, data codec.BinaryCodec) *DDLMessage {
	msg := &DDLMessage{}
	msg.init(typ, data)
	return msg
}

func (m *DDLMessage) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return errno.NewError(errno.ShortBufferSize, 0, 0)
	}
	m.Typ = buf[0]

	msgFn, ok := MessageBinaryCodec[m.Typ]
	if msgFn == nil || !ok {
		return fmt.Errorf("unknown message type: %d", m.Typ)
	}

	m.Data = msgFn()
	return m.Data.UnmarshalBinary(buf[1:])
}

func (m *DDLMessage) Instance() transport.Codec {
	return &DDLMessage{}
}

type SysCtrlRequest struct {
	mod   string
	param map[string]string
}

func (s *SysCtrlRequest) Marshal(buf []byte) ([]byte, error) {
	req := netdata.SysCtrlRequest{
		Mod:   proto.String(s.mod),
		Param: make(map[string]string, len(s.param)),
	}
	for k, v := range s.param {
		req.Param[k] = v
	}

	marshal, err := proto.Marshal(&req)
	if err != nil {
		return nil, err
	}

	buf = append(buf, marshal...)
	return buf, err
}

func (s *SysCtrlRequest) Unmarshal(buf []byte) error {
	var pb netdata.SysCtrlRequest
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}

	s.mod = pb.GetMod()
	s.param = pb.GetParam()
	return nil
}

func (s *SysCtrlRequest) Mod() string {
	return s.mod
}

func (s *SysCtrlRequest) SetMod(m string) {
	s.mod = m
}

func (s *SysCtrlRequest) Param() map[string]string {
	return s.param
}

func (s *SysCtrlRequest) SetParam(m map[string]string) {
	s.param = make(map[string]string, len(m))
	for k, v := range m {
		s.param[k] = v
	}
}

func (s *SysCtrlRequest) Instance() transport.Codec {
	return &SysCtrlRequest{}
}

func (s *SysCtrlRequest) Size() int {
	return 0
}

func (s *SysCtrlRequest) Get(key string) (v string, ok bool) {
	v, ok = s.param[key]
	return
}

type SysCtrlResponse struct {
	err    string
	result map[string]string
}

func (s *SysCtrlResponse) Marshal(buf []byte) ([]byte, error) {
	pb := netdata.SysCtrlResponse{
		Err:    proto.String(s.err),
		Result: s.result,
	}

	marshal, err := proto.Marshal(&pb)
	if err != nil {
		return nil, err
	}

	buf = append(buf, marshal...)
	return buf, err
}

func (s *SysCtrlResponse) Unmarshal(buf []byte) error {
	var pb netdata.SysCtrlResponse
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	s.result = pb.GetResult()
	s.err = pb.GetErr()
	return nil
}

func (s *SysCtrlResponse) Error() error {
	if s.err == "" {
		return nil
	}
	return fmt.Errorf(s.err)
}

func (s *SysCtrlResponse) SetErr(err string) {
	s.err = err
}

func (s *SysCtrlResponse) Result() map[string]string {
	return s.result
}

func (s *SysCtrlResponse) SetResult(ret map[string]string) {
	s.result = ret
}

func (s *SysCtrlResponse) Instance() transport.Codec {
	return &SysCtrlResponse{}
}

func (s *SysCtrlResponse) Size() int {
	return 0
}

type WritePointsRequest struct {
	points []byte
}

func NewWritePointsRequest(points []byte) *WritePointsRequest {
	return &WritePointsRequest{
		points: points,
	}
}

func (r *WritePointsRequest) Points() []byte {
	return r.points
}

func (r *WritePointsRequest) Marshal(buf []byte) ([]byte, error) {
	buf = append(buf, r.points...)
	return buf, nil
}

func (r *WritePointsRequest) Unmarshal(buf []byte) error {
	r.points = bufferpool.GetPoints()
	r.points = bufferpool.Resize(r.points, len(buf))
	copy(r.points, buf)
	return nil
}

func (r *WritePointsRequest) Instance() transport.Codec {
	return &WritePointsRequest{}
}

func (r *WritePointsRequest) Size() int {
	return len(r.points)
}

type WritePointsResponse struct {
	Code    uint8
	ErrCode errno.Errno
	Message string
}

func NewWritePointsResponse(code uint8, errCode errno.Errno, message string) *WritePointsResponse {
	return &WritePointsResponse{
		Code:    code,
		ErrCode: errCode,
		Message: message,
	}
}

func (r *WritePointsResponse) Marshal(buf []byte) ([]byte, error) {
	buf = append(buf, r.Code)
	buf = encoding.MarshalUint16(buf, uint16(r.ErrCode))
	buf = append(buf, r.Message...)
	return buf, nil
}

func (r *WritePointsResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return errno.NewError(errno.ShortBufferSize, 0, 0)
	}
	tBuf := buf
	r.Code = tBuf[0]
	tBuf = tBuf[1:]
	r.ErrCode = errno.Errno(encoding.UnmarshalUint16(tBuf[:2]))
	tBuf = tBuf[2:]
	r.Message = string(tBuf)
	return nil
}

func (r *WritePointsResponse) Instance() transport.Codec {
	return &WritePointsResponse{}
}

func (r *WritePointsResponse) Size() int {
	return 1 + 2 + len(r.Message)
}

type StreamVar struct {
	Only bool
	Id   []uint64
}

func (s *StreamVar) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendBool(buf, s.Only)
	buf = codec.AppendUint64Slice(buf, s.Id)
	return buf, err
}

func (s *StreamVar) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	s.Only = dec.Bool()
	s.Id = dec.Uint64Slice()
	return err
}

func (s *StreamVar) Size() int {
	size := 0
	size += codec.SizeOfBool()
	size += codec.SizeOfUint64Slice(s.Id)
	return size
}

func (s *StreamVar) Instance() transport.Codec {
	return &StreamVar{}
}

type WriteStreamPointsRequest struct {
	points     []byte
	streamVars []*StreamVar
}

func NewWriteStreamPointsRequest(points []byte, streamVar []*StreamVar) *WriteStreamPointsRequest {
	return &WriteStreamPointsRequest{
		points:     points,
		streamVars: streamVar,
	}
}

func (w *WriteStreamPointsRequest) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendBytes(buf, w.points)

	buf = codec.AppendUint32(buf, uint32(len(w.streamVars)))
	for _, item := range w.streamVars {
		if item == nil {
			buf = codec.AppendUint32(buf, 0)
			continue
		}
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}
	return buf, err
}

func (w *WriteStreamPointsRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	w.points = dec.Bytes()

	streamVarsLen := int(dec.Uint32())
	if streamVarsLen > 0 {
		w.streamVars = make([]*StreamVar, streamVarsLen)
		for i := 0; i < streamVarsLen; i++ {
			subBuf := dec.BytesNoCopy()
			if len(subBuf) == 0 {
				continue
			}

			w.streamVars[i] = &StreamVar{}
			if err := w.streamVars[i].Unmarshal(subBuf); err != nil {
				return err
			}
		}
	}
	return err
}

func (w *WriteStreamPointsRequest) Points() []byte {
	return w.points
}

func (w *WriteStreamPointsRequest) StreamVars() []*StreamVar {
	return w.streamVars
}

func (w *WriteStreamPointsRequest) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(w.points)

	size += codec.MaxSliceSize
	for _, item := range w.streamVars {
		size += codec.SizeOfUint32()
		if item == nil {
			continue
		}
		size += item.Size()
	}
	return size
}

func (w *WriteStreamPointsRequest) Instance() transport.Codec {
	return &WriteStreamPointsRequest{}
}

type WriteStreamPointsResponse struct {
	Code    uint8
	ErrCode errno.Errno
	Message string
}

func NewWriteStreamPointsResponse(code uint8, errCode errno.Errno, message string) *WriteStreamPointsResponse {
	return &WriteStreamPointsResponse{
		Code:    code,
		ErrCode: errCode,
		Message: message,
	}
}

func (r *WriteStreamPointsResponse) Marshal(buf []byte) ([]byte, error) {
	buf = append(buf, r.Code)
	buf = encoding.MarshalUint16(buf, uint16(r.ErrCode))
	buf = append(buf, r.Message...)
	return buf, nil
}

func (r *WriteStreamPointsResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return errno.NewError(errno.ShortBufferSize, 0, 0)
	}
	tBuf := buf
	r.Code = tBuf[0]
	tBuf = tBuf[1:]
	r.ErrCode = errno.Errno(encoding.UnmarshalUint16(tBuf[:2]))
	tBuf = tBuf[2:]
	r.Message = string(tBuf)
	return nil
}

func (r *WriteStreamPointsResponse) Instance() transport.Codec {
	return &WriteStreamPointsResponse{}
}

func (r *WriteStreamPointsResponse) Size() int {
	return 1 + 2 + len(r.Message)
}

type PtRequest struct {
	netdata.PtRequest
}

func NewPtRequest() *PtRequest {
	return &PtRequest{}
}

func (r *PtRequest) Marshal(buf []byte) ([]byte, error) {
	b, err := proto.Marshal(&r.PtRequest)
	if err != nil {
		return nil, err
	}

	buf = append(buf, b...)
	return buf, nil
}

func (r *PtRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, &r.PtRequest)
}

func (r *PtRequest) Instance() transport.Codec {
	return &PtRequest{}
}

func (r *PtRequest) Size() int {
	return 0
}

type PtResponse struct {
	netdata.PtResponse
}

func NewPtResponse() *PtResponse {
	return &PtResponse{}
}

func (r *PtResponse) Marshal(buf []byte) ([]byte, error) {
	b, err := proto.Marshal(&r.PtResponse)
	if err != nil {
		return nil, err
	}

	buf = append(buf, b...)
	return buf, nil
}

func (r *PtResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, &r.PtResponse)
}

func (r *PtResponse) Instance() transport.Codec {
	return &PtResponse{}
}

func (r *PtResponse) Size() int {
	return 0
}

func (r *PtResponse) Error() error {
	if r.Err == nil {
		return nil
	}
	return NormalizeError(r.Err)
}

type SegregateNodeRequest struct {
	netdata.SegregateNodeRequest
}

func NewSegregateNodeRequest() *SegregateNodeRequest {
	return &SegregateNodeRequest{}
}

func (r *SegregateNodeRequest) Marshal(buf []byte) ([]byte, error) {
	b, err := proto.Marshal(&r.SegregateNodeRequest)
	if err != nil {
		return nil, err
	}

	buf = append(buf, b...)
	return buf, nil
}

func (r *SegregateNodeRequest) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, &r.SegregateNodeRequest)
}

func (r *SegregateNodeRequest) Instance() transport.Codec {
	return &SegregateNodeRequest{}
}

func (r *SegregateNodeRequest) Size() int {
	return 0
}

type SegregateNodeResponse struct {
	netdata.SegregateNodeResponse
}

func NewSegregateNodeResponse() *SegregateNodeResponse {
	return &SegregateNodeResponse{}
}

func (r *SegregateNodeResponse) Marshal(buf []byte) ([]byte, error) {
	b, err := proto.Marshal(&r.SegregateNodeResponse)
	if err != nil {
		return nil, err
	}

	buf = append(buf, b...)
	return buf, nil
}

func (r *SegregateNodeResponse) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, &r.SegregateNodeResponse)
}

func (r *SegregateNodeResponse) Instance() transport.Codec {
	return &SegregateNodeResponse{}
}

func (r *SegregateNodeResponse) Size() int {
	return 0
}

func (r *SegregateNodeResponse) Error() error {
	if r.Err == nil {
		return nil
	}
	return NormalizeError(r.Err)
}
