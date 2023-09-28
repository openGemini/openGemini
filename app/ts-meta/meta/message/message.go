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

package message

import (
	"fmt"

	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
)

type MetaMessage struct {
	typ  uint8
	data transport.Codec
}

func NewMetaMessage(typ uint8, data transport.Codec) *MetaMessage {
	return &MetaMessage{typ: typ, data: data}
}

func (m *MetaMessage) Type() uint8 {
	return m.typ
}

func (m *MetaMessage) Instance() transport.Codec {
	return &MetaMessage{}
}

func (m *MetaMessage) Size() int {
	return m.data.Size()
}

func (m *MetaMessage) Marshal(buf []byte) ([]byte, error) {
	buf = append(buf, m.typ)
	return m.data.Marshal(buf)
}

func (m *MetaMessage) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return fmt.Errorf("invalid data. the length is 0")
	}
	m.typ = buf[0]
	msgFn, ok := MetaMessageBinaryCodec[m.typ]
	if msgFn == nil || !ok {
		return fmt.Errorf("unknown type: %d", m.typ)
	}
	m.data = msgFn().Instance()
	return m.data.Unmarshal(buf[1:])
}

func (m *MetaMessage) Data() transport.Codec {
	return m.data
}

type PingRequest struct {
	All int
}

type PingResponse struct {
	Leader []byte
	Err    string
}

type PeersRequest struct {
	codec.EmptyCodec
}

func (o *PeersRequest) Instance() transport.Codec {
	return &PeersRequest{}
}

type PeersResponse struct {
	Peers      []string
	Err        string
	StatusCode int
}

type CreateNodeRequest struct {
	WriteHost string
	QueryHost string
}

type CreateNodeResponse struct {
	//Header     map[string][]string
	Data []byte

	Err string
}

type UserSnapshotRequest struct {
	Index uint64
}

type UserSnapshotResponse struct {
	Err string
}

type SnapshotRequest struct {
	Role  int
	Index uint64
}

type SnapshotResponse struct {
	Data []byte
	Err  string
}

type UpdateRequest struct {
	Body []byte
}

type UpdateResponse struct {
	Data       []byte
	Location   string
	Err        string
	StatusCode int
}

type ExecuteRequest struct {
	Body []byte
}

type ExecuteResponse struct {
	Index      uint64
	Err        string // can retry error
	ErrCommand string // do not to retry error
}

type ReportRequest struct {
	Body []byte
}

type ReportResponse struct {
	Index      uint64
	Err        string // can retry error
	ErrCommand string // do not to retry error
}

type GetShardInfoRequest struct {
	Body []byte
}

type GetShardInfoResponse struct {
	Data    []byte
	ErrCode errno.Errno
	Err     string
}

type GetDownSampleInfoRequest struct{}

type GetDownSampleInfoResponse struct {
	Data []byte
	Err  string
}

type GetRpMstInfosRequest struct {
	DbName    string
	RpName    string
	DataTypes []int64
}

type GetRpMstInfosResponse struct {
	Data []byte
	Err  string
}

type GetStreamInfoRequest struct {
	Body []byte
}

type GetStreamInfoResponse struct {
	Data []byte
	Err  string
}

type GetMeasurementInfoRequest struct {
	DbName  string
	RpName  string
	MstName string
}

type GetMeasurementInfoResponse struct {
	Data []byte
	Err  string
}

type GetMeasurementsInfoRequest struct {
	DbName string
	RpName string
}

type GetMeasurementsInfoResponse struct {
	Data []byte
	Err  string
}

type RegisterQueryIDOffsetRequest struct {
	Host string
}

type RegisterQueryIDOffsetResponse struct {
	Offset uint64
	Err    string
}

type Sql2MetaHeartbeatRequest struct {
	Host string
}

type Sql2MetaHeartbeatResponse struct {
	Err string
}

type GetContinuousQueryLeaseRequest struct {
	Host string
}

type GetContinuousQueryLeaseResponse struct {
	CQNames []string
	Err     string
}
