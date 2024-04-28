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
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
)

func (o *CreateNodeRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendString(buf, o.WriteHost)
	buf = codec.AppendString(buf, o.QueryHost)
	buf = codec.AppendString(buf, o.Role)

	return buf, nil
}

func (o *CreateNodeRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	dec := codec.NewBinaryDecoder(buf)
	o.WriteHost = dec.String()
	o.QueryHost = dec.String()
	o.Role = dec.String()

	return nil
}

func (o *CreateNodeRequest) Size() int {
	size := 0
	size += codec.SizeOfString(o.WriteHost)
	size += codec.SizeOfString(o.QueryHost)
	size += codec.SizeOfString(o.Role)

	return size
}

func (o *CreateNodeRequest) Instance() transport.Codec {
	return &CreateNodeRequest{}
}

func (o *CreateSqlNodeRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendString(buf, o.HttpHost)
	buf = codec.AppendString(buf, o.GossipHost)
	return buf, nil
}

func (o *CreateSqlNodeRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.HttpHost = dec.String()
	o.GossipHost = dec.String()

	return nil
}

func (o *CreateSqlNodeRequest) Size() int {
	size := 0
	size += codec.SizeOfString(o.HttpHost)
	size += codec.SizeOfString(o.GossipHost)
	return size
}

func (o *CreateSqlNodeRequest) Instance() transport.Codec {
	return &CreateSqlNodeRequest{}
}

func (o *CreateNodeResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *CreateNodeResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return nil
}

func (o *CreateNodeResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *CreateNodeResponse) Instance() transport.Codec {
	return &CreateNodeResponse{}
}

func (o *CreateSqlNodeResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *CreateSqlNodeResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return nil
}

func (o *CreateSqlNodeResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *CreateSqlNodeResponse) Instance() transport.Codec {
	return &CreateSqlNodeResponse{}
}

func (o *ExecuteRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Body)

	return buf, nil
}

func (o *ExecuteRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	dec := codec.NewBinaryDecoder(buf)
	o.Body = dec.Bytes()

	return nil
}

func (o *ExecuteRequest) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Body)

	return size
}

func (o *ExecuteRequest) Instance() transport.Codec {
	return &ExecuteRequest{}
}

func (o *ExecuteResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendUint64(buf, o.Index)
	buf = codec.AppendString(buf, o.Err)
	buf = codec.AppendString(buf, o.ErrCommand)

	return buf, nil
}

func (o *ExecuteResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Index = dec.Uint64()
	o.Err = dec.String()
	o.ErrCommand = dec.String()

	return nil
}

func (o *ExecuteResponse) Size() int {
	size := 0
	size += codec.SizeOfUint64()
	size += codec.SizeOfString(o.Err)
	size += codec.SizeOfString(o.ErrCommand)

	return size
}

func (o *ExecuteResponse) Instance() transport.Codec {
	return &ExecuteResponse{}
}

func (o *GetDownSampleInfoRequest) Marshal(buf []byte) ([]byte, error) {

	return buf, nil
}

func (o *GetDownSampleInfoRequest) Unmarshal(buf []byte) error {
	return nil
}

func (o *GetDownSampleInfoRequest) Size() int {
	size := 0

	return size
}

func (o *GetDownSampleInfoRequest) Instance() transport.Codec {
	return &GetDownSampleInfoRequest{}
}

func (o *GetDownSampleInfoResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *GetDownSampleInfoResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return nil
}

func (o *GetDownSampleInfoResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *GetDownSampleInfoResponse) Instance() transport.Codec {
	return &GetDownSampleInfoResponse{}
}

func (o *GetRpMstInfosRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendString(buf, o.DbName)
	buf = codec.AppendString(buf, o.RpName)
	buf = codec.AppendInt64Slice(buf, o.DataTypes)

	return buf, nil
}

func (o *GetRpMstInfosRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.DbName = dec.String()
	o.RpName = dec.String()
	o.DataTypes = dec.Int64Slice()

	return nil
}

func (o *GetRpMstInfosRequest) Size() int {
	size := 0
	size += codec.SizeOfString(o.DbName)
	size += codec.SizeOfString(o.RpName)
	size += codec.SizeOfInt64Slice(o.DataTypes)

	return size
}

func (o *GetRpMstInfosRequest) Instance() transport.Codec {
	return &GetRpMstInfosRequest{}
}

func (o *GetRpMstInfosResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *GetRpMstInfosResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return nil
}

func (o *GetRpMstInfosResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *GetRpMstInfosResponse) Instance() transport.Codec {
	return &GetRpMstInfosResponse{}
}

func (o *GetShardInfoRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Body)

	return buf, nil
}

func (o *GetShardInfoRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Body = dec.Bytes()

	return nil
}

func (o *GetShardInfoRequest) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Body)

	return size
}

func (o *GetShardInfoRequest) Instance() transport.Codec {
	return &GetShardInfoRequest{}
}

func (o *GetShardInfoResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendUint16(buf, uint16(o.ErrCode))
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *GetShardInfoResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.ErrCode = errno.Errno(dec.Uint16())
	o.Err = dec.String()

	return nil
}

func (o *GetShardInfoResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += 2
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *GetShardInfoResponse) Instance() transport.Codec {
	return &GetShardInfoResponse{}
}

func (o *PeersResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendStringSlice(buf, o.Peers)
	buf = codec.AppendString(buf, o.Err)
	buf = codec.AppendInt(buf, o.StatusCode)

	return buf, nil
}

func (o *PeersResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Peers = dec.StringSlice()
	o.Err = dec.String()
	o.StatusCode = dec.Int()

	return nil
}

func (o *PeersResponse) Size() int {
	size := 0
	size += codec.SizeOfStringSlice(o.Peers)
	size += codec.SizeOfString(o.Err)
	size += codec.SizeOfInt()

	return size
}

func (o *PeersResponse) Instance() transport.Codec {
	return &PeersResponse{}
}

func (o *PingRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendInt(buf, o.All)

	return buf, nil
}

func (o *PingRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.All = dec.Int()

	return nil
}

func (o *PingRequest) Size() int {
	size := 0
	size += codec.SizeOfInt()

	return size
}

func (o *PingRequest) Instance() transport.Codec {
	return &PingRequest{}
}

func (o *PingResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Leader)
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *PingResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Leader = dec.Bytes()
	o.Err = dec.String()

	return nil
}

func (o *PingResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Leader)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *PingResponse) Instance() transport.Codec {
	return &PingResponse{}
}

func (o *ReportRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Body)

	return buf, nil
}

func (o *ReportRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Body = dec.Bytes()

	return nil
}

func (o *ReportRequest) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Body)

	return size
}

func (o *ReportRequest) Instance() transport.Codec {
	return &ReportRequest{}
}

func (o *ReportResponse) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendUint64(buf, o.Index)
	buf = codec.AppendString(buf, o.Err)
	buf = codec.AppendString(buf, o.ErrCommand)

	return buf, err
}

func (o *ReportResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Index = dec.Uint64()
	o.Err = dec.String()
	o.ErrCommand = dec.String()

	return nil
}

func (o *ReportResponse) Size() int {
	size := 0
	size += codec.SizeOfUint64()
	size += codec.SizeOfString(o.Err)
	size += codec.SizeOfString(o.ErrCommand)

	return size
}

func (o *ReportResponse) Instance() transport.Codec {
	return &ReportResponse{}
}

func (o *SnapshotRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendInt(buf, o.Role)
	buf = codec.AppendUint64(buf, o.Index)

	return buf, nil
}

func (o *SnapshotRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Role = dec.Int()
	o.Index = dec.Uint64()

	return nil
}

func (o *SnapshotRequest) Size() int {
	size := 0
	size += codec.SizeOfInt()
	size += codec.SizeOfUint64()

	return size
}

func (o *SnapshotRequest) Instance() transport.Codec {
	return &SnapshotRequest{}
}

func (o *SnapshotResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *SnapshotResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return nil
}

func (o *SnapshotResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *SnapshotResponse) Instance() transport.Codec {
	return &SnapshotResponse{}
}

func (o *SnapshotV2Request) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendInt(buf, o.Role)
	buf = codec.AppendUint64(buf, o.Index)
	buf = codec.AppendUint64(buf, o.Id)
	return buf, nil
}

func (o *SnapshotV2Request) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Role = dec.Int()
	o.Index = dec.Uint64()
	o.Id = dec.Uint64()
	return nil
}

func (o *SnapshotV2Request) Size() int {
	size := 0
	size += codec.SizeOfInt()
	size += codec.SizeOfUint64()
	size += codec.SizeOfUint64()
	return size
}

func (o *SnapshotV2Request) Instance() transport.Codec {
	return &SnapshotV2Request{}
}

func (o *SnapshotV2Response) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *SnapshotV2Response) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return nil
}

func (o *SnapshotV2Response) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *SnapshotV2Response) Instance() transport.Codec {
	return &SnapshotV2Response{}
}

func (o *UpdateRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Body)

	return buf, nil
}

func (o *UpdateRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Body = dec.Bytes()

	return nil
}

func (o *UpdateRequest) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Body)

	return size
}

func (o *UpdateRequest) Instance() transport.Codec {
	return &UpdateRequest{}
}

func (o *UpdateResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Location)
	buf = codec.AppendString(buf, o.Err)
	buf = codec.AppendInt(buf, o.StatusCode)

	return buf, nil
}

func (o *UpdateResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Location = dec.String()
	o.Err = dec.String()
	o.StatusCode = dec.Int()

	return nil
}

func (o *UpdateResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Location)
	size += codec.SizeOfString(o.Err)
	size += codec.SizeOfInt()

	return size
}

func (o *UpdateResponse) Instance() transport.Codec {
	return &UpdateResponse{}
}

func (o *UserSnapshotRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendUint64(buf, o.Index)

	return buf, nil
}

func (o *UserSnapshotRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Index = dec.Uint64()

	return nil
}

func (o *UserSnapshotRequest) Size() int {
	size := 0
	size += codec.SizeOfUint64()

	return size
}

func (o *UserSnapshotRequest) Instance() transport.Codec {
	return &UserSnapshotRequest{}
}

func (o *UserSnapshotResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *UserSnapshotResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Err = dec.String()

	return nil
}

func (o *UserSnapshotResponse) Size() int {
	size := 0
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *UserSnapshotResponse) Instance() transport.Codec {
	return &UserSnapshotResponse{}
}

func (o *GetMeasurementInfoRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendString(buf, o.DbName)
	buf = codec.AppendString(buf, o.RpName)
	buf = codec.AppendString(buf, o.MstName)

	return buf, nil
}

func (o *GetMeasurementInfoRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.DbName = dec.String()
	o.RpName = dec.String()
	o.MstName = dec.String()

	return nil
}

func (o *GetMeasurementInfoRequest) Size() int {
	size := 0
	size += codec.SizeOfString(o.DbName)
	size += codec.SizeOfString(o.RpName)
	size += codec.SizeOfString(o.MstName)

	return size
}

func (o *GetMeasurementInfoRequest) Instance() transport.Codec {
	return &GetMeasurementInfoRequest{}
}

func (o *GetMeasurementInfoResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *GetMeasurementInfoResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return nil
}

func (o *GetMeasurementInfoResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *GetMeasurementInfoResponse) Instance() transport.Codec {
	return &GetMeasurementInfoResponse{}
}

func (o *GetMeasurementsInfoRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendString(buf, o.DbName)
	buf = codec.AppendString(buf, o.RpName)

	return buf, nil
}

func (o *GetMeasurementsInfoRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	dec := codec.NewBinaryDecoder(buf)
	o.DbName = dec.String()
	o.RpName = dec.String()

	return nil
}

func (o *GetMeasurementsInfoRequest) Size() int {
	size := 0
	size += codec.SizeOfString(o.DbName)
	size += codec.SizeOfString(o.RpName)

	return size
}

func (o *GetMeasurementsInfoRequest) Instance() transport.Codec {
	return &GetMeasurementsInfoRequest{}
}

func (o *GetMeasurementsInfoResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, nil
}

func (o *GetMeasurementsInfoResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return nil
}

func (o *GetMeasurementsInfoResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *GetMeasurementsInfoResponse) Instance() transport.Codec {
	return &GetMeasurementsInfoResponse{}
}

func (o *GetStreamInfoRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendBytes(buf, o.Body)

	return buf, nil
}

func (o *GetStreamInfoRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Body = dec.Bytes()

	return nil
}

func (o *GetStreamInfoRequest) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Body)

	return size
}

func (o *GetStreamInfoRequest) Instance() transport.Codec {
	return &GetStreamInfoRequest{}
}

func (o *GetStreamInfoResponse) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *GetStreamInfoResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return nil
}

func (o *GetStreamInfoResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *GetStreamInfoResponse) Instance() transport.Codec {
	return &GetStreamInfoResponse{}
}

func (o *RegisterQueryIDOffsetRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendString(buf, o.Host)

	return buf, nil
}

func (o *RegisterQueryIDOffsetRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Host = dec.String()

	return nil
}

func (o *RegisterQueryIDOffsetRequest) Size() int {
	size := codec.SizeOfString(o.Host)

	return size
}

func (o *RegisterQueryIDOffsetRequest) Instance() transport.Codec {
	return &RegisterQueryIDOffsetRequest{}
}

func (o *RegisterQueryIDOffsetResponse) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendUint64(buf, o.Offset)
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *RegisterQueryIDOffsetResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	dec := codec.NewBinaryDecoder(buf)
	o.Offset = dec.Uint64()
	o.Err = dec.String()

	return nil
}

func (o *RegisterQueryIDOffsetResponse) Size() int {
	size := codec.SizeOfUint64()
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *RegisterQueryIDOffsetResponse) Instance() transport.Codec {
	return &RegisterQueryIDOffsetResponse{}
}

func (o *Sql2MetaHeartbeatRequest) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendString(buf, o.Host)

	return buf, err
}

func (o *Sql2MetaHeartbeatRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Host = dec.String()

	return err
}

func (o *Sql2MetaHeartbeatRequest) Size() int {
	size := 0
	size += codec.SizeOfString(o.Host)

	return size
}

func (o *Sql2MetaHeartbeatRequest) Instance() transport.Codec {
	return &Sql2MetaHeartbeatRequest{}
}

func (o *Sql2MetaHeartbeatResponse) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *Sql2MetaHeartbeatResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Err = dec.String()

	return err
}

func (o *Sql2MetaHeartbeatResponse) Size() int {
	size := 0
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *Sql2MetaHeartbeatResponse) Instance() transport.Codec {
	return &Sql2MetaHeartbeatResponse{}
}

func (o *GetContinuousQueryLeaseRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendString(buf, o.Host)
	return buf, nil
}

func (o *GetContinuousQueryLeaseRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.Host = dec.String()
	return nil
}

func (o *GetContinuousQueryLeaseRequest) Size() int {
	return codec.SizeOfString(o.Host)
}

func (o *GetContinuousQueryLeaseRequest) Instance() transport.Codec {
	return &GetContinuousQueryLeaseRequest{}
}

func (o *GetContinuousQueryLeaseResponse) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendStringSlice(buf, o.CQNames)
	buf = codec.AppendString(buf, o.Err)
	return buf, nil
}

func (o *GetContinuousQueryLeaseResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	o.CQNames = dec.StringSlice()
	o.Err = dec.String()
	return nil
}

func (o *GetContinuousQueryLeaseResponse) Size() int {
	size := codec.SizeOfStringSlice(o.CQNames)
	size += codec.SizeOfString(o.Err)
	return size
}

func (o *GetContinuousQueryLeaseResponse) Instance() transport.Codec {
	return &GetContinuousQueryLeaseResponse{}
}

func (req *VerifyDataNodeStatusRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendUint64(buf, req.NodeID)
	return buf, nil
}

func (req *VerifyDataNodeStatusRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	req.NodeID = dec.Uint64()
	return nil
}

func (req *VerifyDataNodeStatusRequest) Size() int {
	return codec.SizeOfUint64()
}

func (req *VerifyDataNodeStatusRequest) Instance() transport.Codec {
	return &VerifyDataNodeStatusRequest{}
}

func (resp *VerifyDataNodeStatusResponse) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendString(buf, resp.Err)

	return buf, err
}

func (resp *VerifyDataNodeStatusResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	dec := codec.NewBinaryDecoder(buf)
	resp.Err = dec.String()
	return nil
}

func (resp *VerifyDataNodeStatusResponse) Size() int {
	return codec.SizeOfString(resp.Err)
}

func (resp *VerifyDataNodeStatusResponse) Instance() transport.Codec {
	return &VerifyDataNodeStatusResponse{}
}

func (req *SendSysCtrlToMetaRequest) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendString(buf, req.Mod)
	buf = codec.AppendMapStringString(buf, req.Param)
	return buf, nil
}

func (req *SendSysCtrlToMetaRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	dec := codec.NewBinaryDecoder(buf)
	req.Mod = dec.String()
	req.Param = dec.MapStringString()
	return nil
}

func (req *SendSysCtrlToMetaRequest) Size() int {
	size := codec.SizeOfString(req.Mod)
	size += codec.SizeOfMapStringString(req.Param)
	return size
}

func (req *SendSysCtrlToMetaRequest) Instance() transport.Codec {
	return &SendSysCtrlToMetaRequest{}
}

func (resp *SendSysCtrlToMetaResponse) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendString(buf, resp.Err)

	return buf, err
}

func (resp *SendSysCtrlToMetaResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	dec := codec.NewBinaryDecoder(buf)
	resp.Err = dec.String()
	return nil
}

func (resp *SendSysCtrlToMetaResponse) Size() int {
	return codec.SizeOfString(resp.Err)
}

func (resp *SendSysCtrlToMetaResponse) Instance() transport.Codec {
	return &SendSysCtrlToMetaResponse{}
}
