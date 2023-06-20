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
	var err error
	buf = codec.AppendString(buf, o.WriteHost)
	buf = codec.AppendString(buf, o.QueryHost)

	return buf, err
}

func (o *CreateNodeRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.WriteHost = dec.String()
	o.QueryHost = dec.String()

	return err
}

func (o *CreateNodeRequest) Size() int {
	size := 0
	size += codec.SizeOfString(o.WriteHost)
	size += codec.SizeOfString(o.QueryHost)

	return size
}

func (o *CreateNodeRequest) Instance() transport.Codec {
	return &CreateNodeRequest{}
}

func (o *CreateNodeResponse) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *CreateNodeResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return err
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

func (o *ExecuteRequest) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendBytes(buf, o.Body)

	return buf, err
}

func (o *ExecuteRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Body = dec.Bytes()

	return err
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
	var err error
	buf = codec.AppendUint64(buf, o.Index)
	buf = codec.AppendString(buf, o.Err)
	buf = codec.AppendString(buf, o.ErrCommand)

	return buf, err
}

func (o *ExecuteResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Index = dec.Uint64()
	o.Err = dec.String()
	o.ErrCommand = dec.String()

	return err
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
	var err error

	return buf, err
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
	var err error
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *GetDownSampleInfoResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return err
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
	var err error
	buf = codec.AppendString(buf, o.DbName)
	buf = codec.AppendString(buf, o.RpName)
	buf = codec.AppendInt64Slice(buf, o.DataTypes)

	return buf, err
}

func (o *GetRpMstInfosRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.DbName = dec.String()
	o.RpName = dec.String()
	o.DataTypes = dec.Int64Slice()

	return err
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
	var err error
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *GetRpMstInfosResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return err
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
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Body = dec.Bytes()

	return err
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
	var err error
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendUint16(buf, uint16(o.ErrCode))
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *GetShardInfoResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.ErrCode = errno.Errno(dec.Uint16())
	o.Err = dec.String()

	return err
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
	var err error
	buf = codec.AppendStringSlice(buf, o.Peers)
	buf = codec.AppendString(buf, o.Err)
	buf = codec.AppendInt(buf, o.StatusCode)

	return buf, err
}

func (o *PeersResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Peers = dec.StringSlice()
	o.Err = dec.String()
	o.StatusCode = dec.Int()

	return err
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
	var err error
	buf = codec.AppendInt(buf, o.All)

	return buf, err
}

func (o *PingRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.All = dec.Int()

	return err
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
	var err error
	buf = codec.AppendBytes(buf, o.Leader)
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *PingResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Leader = dec.Bytes()
	o.Err = dec.String()

	return err
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
	var err error
	buf = codec.AppendBytes(buf, o.Body)

	return buf, err
}

func (o *ReportRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Body = dec.Bytes()

	return err
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
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Index = dec.Uint64()
	o.Err = dec.String()
	o.ErrCommand = dec.String()

	return err
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
	var err error
	buf = codec.AppendInt(buf, o.Role)
	buf = codec.AppendUint64(buf, o.Index)

	return buf, err
}

func (o *SnapshotRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Role = dec.Int()
	o.Index = dec.Uint64()

	return err
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
	var err error
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *SnapshotResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return err
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

func (o *UpdateRequest) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendBytes(buf, o.Body)

	return buf, err
}

func (o *UpdateRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Body = dec.Bytes()

	return err
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
	var err error
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Location)
	buf = codec.AppendString(buf, o.Err)
	buf = codec.AppendInt(buf, o.StatusCode)

	return buf, err
}

func (o *UpdateResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Location = dec.String()
	o.Err = dec.String()
	o.StatusCode = dec.Int()

	return err
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
	var err error
	buf = codec.AppendUint64(buf, o.Index)

	return buf, err
}

func (o *UserSnapshotRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Index = dec.Uint64()

	return err
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
	var err error
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *UserSnapshotResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Err = dec.String()

	return err
}

func (o *UserSnapshotResponse) Size() int {
	size := 0
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *UserSnapshotResponse) Instance() transport.Codec {
	return &UserSnapshotResponse{}
}

func (o *GetUserInfoRequest) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendUint64(buf, o.Index)

	return buf, err
}

func (o *GetUserInfoRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Index = dec.Uint64()

	return err
}

func (o *GetUserInfoRequest) Size() int {
	size := 0
	size += codec.SizeOfUint64()

	return size
}

func (o *GetUserInfoRequest) Instance() transport.Codec {
	return &GetUserInfoRequest{}
}

func (o *GetUserInfoResponse) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *GetUserInfoResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return err
}

func (o *GetUserInfoResponse) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(o.Data)
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *GetUserInfoResponse) Instance() transport.Codec {
	return &GetUserInfoResponse{}
}

func (o *GetMeasurementInfoRequest) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendString(buf, o.DbName)
	buf = codec.AppendString(buf, o.RpName)
	buf = codec.AppendString(buf, o.MstName)

	return buf, err
}

func (o *GetMeasurementInfoRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.DbName = dec.String()
	o.RpName = dec.String()
	o.MstName = dec.String()

	return err
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
	var err error
	buf = codec.AppendBytes(buf, o.Data)
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *GetMeasurementInfoResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return err
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

func (o *GetStreamInfoRequest) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendBytes(buf, o.Body)

	return buf, err
}

func (o *GetStreamInfoRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Body = dec.Bytes()

	return err
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
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Data = dec.Bytes()
	o.Err = dec.String()

	return err
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
	var err error
	buf = codec.AppendString(buf, o.Host)

	return buf, err
}

func (o *RegisterQueryIDOffsetRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Host = dec.String()

	return err
}

func (o *RegisterQueryIDOffsetRequest) Size() int {
	size := 0
	size += codec.SizeOfString(o.Host)

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
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Offset = dec.Uint64()
	o.Err = dec.String()

	return err
}

func (o *RegisterQueryIDOffsetResponse) Size() int {
	size := 0
	size += codec.SizeOfUint64()
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *RegisterQueryIDOffsetResponse) Instance() transport.Codec {
	return &RegisterQueryIDOffsetResponse{}
}
