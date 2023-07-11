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
	"errors"
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/lib/errno"
	internal2 "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/openGemini/openGemini/open_src/influx/meta"
)

//go:generate protoc --gogo_out=. data/data.proto
//go:generate protoc --gofast_out=. data/data.proto

type DeleteType int

const (
	DatabaseDelete DeleteType = iota
	RetentionPolicyDelete
	MeasurementDelete
)

type RunStateType int32

const (
	Unknown RunStateType = iota
	Running
	Killed
)

type DeleteRequest struct {
	Database    string
	Rp          string
	Measurement string
	ShardIds    []uint64
	Type        DeleteType
	PtId        uint32
}

func (ddr *DeleteRequest) MarshalBinary() ([]byte, error) {
	dr := &internal2.DeleteRequest{DB: proto.String(ddr.Database)}
	dr.DeleteType = proto.Int(int(ddr.Type))
	switch ddr.Type {
	case MeasurementDelete:
		dr.Mst = proto.String(ddr.Measurement)
		dr.ShardIDs = ddr.ShardIds
		fallthrough
	case RetentionPolicyDelete:
		dr.Rp = proto.String(ddr.Rp)
		fallthrough
	case DatabaseDelete:
		dr.PtId = proto.Uint32(ddr.PtId)
	default:
		return nil, fmt.Errorf("do not surrport delete command %d", ddr.Type)
	}
	return proto.Marshal(dr)
}

func (ddr *DeleteRequest) UnmarshalBinary(data []byte) error {
	var pb internal2.DeleteRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	ddr.Type = DeleteType(pb.GetDeleteType())
	switch ddr.Type {
	case MeasurementDelete:
		ddr.Measurement = pb.GetMst()
		ddr.ShardIds = pb.GetShardIDs()
		fallthrough
	case RetentionPolicyDelete:
		ddr.Rp = pb.GetRp()
		fallthrough
	case DatabaseDelete:
		ddr.Database = pb.GetDB()
		ddr.PtId = pb.GetPtId()
	default:
		return fmt.Errorf("do not surrport delete command %d", ddr.Type)
	}
	return nil
}

type DeleteResponse struct {
	Err error
}

func (dr *DeleteResponse) MarshalBinary() ([]byte, error) {
	var pb internal2.DeleteResponse
	if dr.Err != nil {
		pb.Err = proto.String(dr.Err.Error())
	}
	return proto.Marshal(&pb)
}

func (dr *DeleteResponse) UnmarshalBinary(data []byte) error {
	var pb internal2.DeleteResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Err != nil {
		dr.Err = errors.New(pb.GetErr())
	}
	return nil
}

type SeriesKeysRequest struct {
	internal2.SeriesKeysRequest
}

func (r *SeriesKeysRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.SeriesKeysRequest)
}

func (r *SeriesKeysRequest) UnmarshalBinary(buf []byte) error {
	return proto.Unmarshal(buf, &r.SeriesKeysRequest)
}

type SeriesKeysResponse struct {
	internal2.SeriesKeysResponse
}

func (r *SeriesKeysResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.SeriesKeysResponse)
}

func (r *SeriesKeysResponse) UnmarshalBinary(buf []byte) error {
	return proto.Unmarshal(buf, &r.SeriesKeysResponse)
}

func (r *SeriesKeysResponse) Error() error {
	return NormalizeError(r.Err)
}

type SeriesCardinalityRequest struct {
	SeriesKeysRequest
}

type SeriesCardinalityResponse struct {
	meta.CardinalityResponse
}

type SeriesExactCardinalityRequest struct {
	SeriesKeysRequest
}

type SeriesExactCardinalityResponse struct {
	ExactCardinalityResponse
}

type ShowTagValuesCardinalityRequest struct {
	ShowTagValuesRequest
}

type ShowTagValuesCardinalityResponse struct {
	ExactCardinalityResponse
}

type ExactCardinalityResponse struct {
	internal2.ExactCardinalityResponse
}

func (r *ExactCardinalityResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.ExactCardinalityResponse)
}

func (r *ExactCardinalityResponse) UnmarshalBinary(buf []byte) error {
	return proto.Unmarshal(buf, &r.ExactCardinalityResponse)
}

func (r *ExactCardinalityResponse) Error() error {
	return NormalizeError(r.Err)
}

type ShowTagValuesRequest struct {
	internal2.ShowTagValuesRequest
}

func (r *ShowTagValuesRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.ShowTagValuesRequest)
}

func (r *ShowTagValuesRequest) UnmarshalBinary(buf []byte) error {
	return proto.Unmarshal(buf, &r.ShowTagValuesRequest)
}

func (r *ShowTagValuesRequest) SetTagKeys(tagKeys map[string]map[string]struct{}) {
	if tagKeys == nil {
		return
	}

	r.TagKeys = make([]*internal2.MapTagKeys, 0, len(tagKeys))
	for name, keys := range tagKeys {
		item := &internal2.MapTagKeys{
			Measurement: proto.String(name),
			Keys:        make([]string, 0, len(keys)),
		}
		for k := range keys {
			item.Keys = append(item.Keys, k)
		}
		r.TagKeys = append(r.TagKeys, item)
	}
}

func (r *ShowTagValuesRequest) GetTagKeysBytes() map[string][][]byte {
	ret := make(map[string][][]byte, len(r.TagKeys))
	for _, item := range r.TagKeys {
		ret[*item.Measurement] = make([][]byte, 0, len(item.Keys))
		for i := range item.Keys {
			ret[*item.Measurement] = append(ret[*item.Measurement], []byte(item.Keys[i]))
		}
	}

	return ret
}

type ShowTagValuesResponse struct {
	internal2.ShowTagValuesResponse
}

func (r *ShowTagValuesResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.ShowTagValuesResponse)
}

func (r *ShowTagValuesResponse) UnmarshalBinary(buf []byte) error {
	return proto.Unmarshal(buf, &r.ShowTagValuesResponse)
}

func (r *ShowTagValuesResponse) Error() error {
	return NormalizeError(r.Err)
}

func NormalizeError(errStr *string) error {
	if errStr == nil {
		return nil
	}
	errBytes := bytesutil.ToUnsafeBytes(*errStr)
	errCode := encoding.UnmarshalUint16(errBytes[:2])
	if errCode != 0 {
		return errno.NewError(errno.Errno(errCode), bytesutil.ToUnsafeString(errBytes[2:]))
	}
	return fmt.Errorf("%s", bytesutil.ToUnsafeString(errBytes[2:]))
}

func MarshalError(e error) *string {
	if e == nil {
		return nil
	}
	var dst []byte
	switch stdErr := e.(type) {
	case *errno.Error:
		dst = encoding.MarshalUint16(dst, uint16(stdErr.Errno()))
		return proto.String(bytesutil.ToUnsafeString(dst) + e.Error())
	default:
		dst = encoding.MarshalUint16(dst, 0)
		return proto.String(bytesutil.ToUnsafeString(dst) + e.Error())
	}
}

func (r *ShowTagValuesResponse) GetTagValuesSlice() TablesTagSets {
	ret := make(TablesTagSets, 0, len(r.Values))
	for _, item := range r.Values {
		if len(item.Values) != len(item.Keys) {
			continue
		}

		values := make(TagSets, 0, len(item.Values))
		for i := 0; i < len(item.Values); i++ {
			values = append(values, TagSet{item.Keys[i], item.Values[i]})
		}

		ret = append(ret, TableTagSets{
			Name:   item.GetMeasurement(),
			Values: values,
		})
	}
	return ret
}

func (r *ShowTagValuesResponse) SetTagValuesSlice(s TablesTagSets) {
	if s == nil {
		return
	}
	r.Values = make([]*internal2.TagValuesSlice, 0, len(s))
	for i := range s {
		keys := make([]string, 0, len(s[i].Values))
		values := make([]string, 0, len(s[i].Values))

		for j := range s[i].Values {
			keys = append(keys, s[i].Values[j].Key)
			values = append(values, s[i].Values[j].Value)
		}

		r.Values = append(r.Values, &internal2.TagValuesSlice{
			Measurement: proto.String(s[i].Name),
			Keys:        keys,
			Values:      values,
		})
	}
}

type ExecuteStatementMessage struct {
	StatementType string
	Result        []byte
	Filtered      []byte
}

const (
	ShowMeasurementsStatement           = "ShowMeasurementsStatement"
	ShowTagKeysStatement                = "ShowTagKeysStatement"
	ShowTagValuesStatement              = "ShowTagValuesStatement"
	ShowSeriesCardinalityStatement      = "ShowSeriesCardinalityStatement"
	ShowMeasurementCardinalityStatement = "ShowMeasurementCardinalityStatement"
)

type GetShardSplitPointsRequest struct {
	internal2.GetShardSplitPointsRequest
}

func (r *GetShardSplitPointsRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.GetShardSplitPointsRequest)
}

func (r *GetShardSplitPointsRequest) UnmarshalBinary(buf []byte) error {
	return proto.Unmarshal(buf, &r.GetShardSplitPointsRequest)
}

func (r *GetShardSplitPointsRequest) Error() error {
	return nil
}

type GetShardSplitPointsResponse struct {
	internal2.GetShardSplitPointsResponse
}

func (r *GetShardSplitPointsResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.GetShardSplitPointsResponse)
}

func (r *GetShardSplitPointsResponse) UnmarshalBinary(buf []byte) error {
	return proto.Unmarshal(buf, &r.GetShardSplitPointsResponse)
}

func (r *GetShardSplitPointsResponse) Error() error {
	return NormalizeError(r.Err)
}

type CreateDataBaseRequest struct {
	internal2.CreateDataBaseRequest
}

func (r *CreateDataBaseRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.CreateDataBaseRequest)
}

func (r *CreateDataBaseRequest) UnmarshalBinary(buf []byte) error {
	return proto.Unmarshal(buf, &r.CreateDataBaseRequest)
}

type CreateDataBaseResponse struct {
	internal2.CreateDataBaseResponse
}

func (r *CreateDataBaseResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.CreateDataBaseResponse)
}

func (r *CreateDataBaseResponse) UnmarshalBinary(buf []byte) error {
	return proto.Unmarshal(buf, &r.CreateDataBaseResponse)
}

func (r *CreateDataBaseResponse) Error() error {
	if r.Err == nil {
		return nil
	}
	return fmt.Errorf("%s", *r.Err)
}

type ShowQueriesRequest struct {
}

func (r *ShowQueriesRequest) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (r *ShowQueriesRequest) UnmarshalBinary(buf []byte) error {
	return nil
}

type QueryExeInfo struct {
	QueryID   uint64
	Stmt      string
	Database  string
	BeginTime int64
	RunState  RunStateType
}

type ShowQueriesResponse struct {
	QueryExeInfos []*QueryExeInfo
}

func (r *ShowQueriesResponse) MarshalBinary() ([]byte, error) {
	var pb internal2.ShowQueriesResponse
	pb.QueryExeInfos = make([]*internal2.QueryExeInfo, 0, len(r.QueryExeInfos))
	for _, info := range r.QueryExeInfos {
		pb.QueryExeInfos = append(pb.QueryExeInfos, &internal2.QueryExeInfo{
			QueryID:   proto.Uint64(info.QueryID),
			Stmt:      proto.String(info.Stmt),
			Database:  proto.String(info.Database),
			BeginTime: proto.Int64(info.BeginTime),
			RunState:  proto.Int32(int32(info.RunState)),
		})
	}
	return proto.Marshal(&pb)
}

func (r *ShowQueriesResponse) UnmarshalBinary(buf []byte) error {
	var pb internal2.ShowQueriesResponse
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	r.QueryExeInfos = make([]*QueryExeInfo, 0, len(pb.GetQueryExeInfos()))
	for _, pbInfo := range pb.QueryExeInfos {
		r.QueryExeInfos = append(r.QueryExeInfos, &QueryExeInfo{
			QueryID:   pbInfo.GetQueryID(),
			Stmt:      pbInfo.GetStmt(),
			Database:  pbInfo.GetDatabase(),
			BeginTime: pbInfo.GetBeginTime(),
			RunState:  RunStateType(pbInfo.GetRunState()),
		})
	}
	return nil
}
