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

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	internal2 "github.com/openGemini/openGemini/lib/netstorage/data"
)

//go:generate tmpl -data=@message_types.tmpldata message_types.go.tmpl
//go:generate tmpl -data=@message_types.tmpldata message_types_test.go.tmpl

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

	msg := NewMessage(m.Typ)
	if msg == nil {
		return fmt.Errorf("unknown message type: %d", m.Typ)
	}

	m.Data = msg
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
	req := internal2.SysCtrlRequest{
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
	var pb internal2.SysCtrlRequest
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
	pb := internal2.SysCtrlResponse{
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
	var pb internal2.SysCtrlResponse
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
	Message string
}

func NewWritePointsResponse(code uint8, message string) *WritePointsResponse {
	return &WritePointsResponse{
		Code:    code,
		Message: message,
	}
}

func (r *WritePointsResponse) Marshal(buf []byte) ([]byte, error) {
	buf = append(buf, r.Code)
	buf = append(buf, r.Message...)
	return buf, nil
}

func (r *WritePointsResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return errno.NewError(errno.ShortBufferSize, 0, 0)
	}

	r.Code = buf[0]
	r.Message = string(buf[1:])
	return nil
}

func (r *WritePointsResponse) Instance() transport.Codec {
	return &WritePointsResponse{}
}

func (r *WritePointsResponse) Size() int {
	return 1 + len(r.Message)
}
