// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package netstorage_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	netdata "github.com/openGemini/openGemini/lib/netstorage/data"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestSeriesKeysRequestMessage(t *testing.T) {
	req := &netstorage.SeriesKeysRequest{}
	req.Db = proto.String("db0")
	req.PtIDs = []uint32{1, 2, 3, 4, 5}
	req.Condition = proto.String("a=1 and b=2")
	req.Measurements = []string{"mst0", "mst1"}

	msg := netstorage.NewDDLMessage(netstorage.SeriesKeysRequestMessage, req)
	buf, err := msg.Marshal(nil)
	if !assert.NoError(t, err) {
		return
	}

	msg2 := netstorage.NewDDLMessage(netstorage.SeriesKeysRequestMessage, &netstorage.SeriesKeysRequest{})
	if !assert.NoError(t, msg2.Unmarshal(buf)) {
		return
	}

	other, ok := msg2.Data.(*netstorage.SeriesKeysRequest)
	if !assert.Equal(t, ok, true, "unmarshal failed") {
		return
	}

	if !reflect.DeepEqual(req.GetPtIDs(), other.GetPtIDs()) ||
		req.GetDb() != other.GetDb() ||
		req.GetCondition() != other.GetCondition() ||
		!reflect.DeepEqual(req.GetMeasurements(), other.GetMeasurements()) {

		t.Fatalf("codec failed; \nexp: %+v \ngot: %+v", req, other)
	}
}

func TestSeriesKeysResponseMessage(t *testing.T) {
	resp := &netstorage.SeriesKeysResponse{}
	resp.Series = []string{"cpu,hostname=127.0.0.1,role=store", "memory,hostname=127.0.0.2,role=sql"}
	resp.Err = proto.String("no errors")

	buf, err := resp.MarshalBinary()
	if !assert.NoError(t, err) {
		return
	}

	other := &netstorage.SeriesKeysResponse{}
	if !assert.NoError(t, other.UnmarshalBinary(buf)) {
		return
	}

	if !reflect.DeepEqual(resp.GetSeries(), other.GetSeries()) ||
		resp.Error().Error() != other.Error().Error() {

		t.Fatalf("codec SeriesKeysResponse failed \nexp: %+v \ngot: %+v",
			resp, other)
	}
}

func makeDeleteRequestMessage(t *testing.T, typ netstorage.DeleteType) (*netstorage.DeleteRequest, *netstorage.DeleteRequest) {
	req := &netstorage.DeleteRequest{}
	req.Database = "db0"
	req.Type = typ
	req.Measurement = "mst_0"
	req.Rp = "test_1"
	req.ShardIds = []uint64{12, 3, 4, 5}

	msg := netstorage.NewDDLMessage(netstorage.DeleteRequestMessage, req)
	buf, err := msg.Marshal(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	msg2 := msg.Instance()
	if err := msg2.Unmarshal(buf); err != nil {
		t.Fatalf("%v", err)
	}

	other, ok := msg2.(*netstorage.DDLMessage).Data.(*netstorage.DeleteRequest)
	if !ok {
		t.Fatalf("unmarshal DeleteRequest failed")
	}

	return req, other
}

func TestDeleteRequestMessage(t *testing.T) {
	req, other := makeDeleteRequestMessage(t, netstorage.MeasurementDelete)
	if !assert.Equal(t, req, other) {
		return
	}

	req, other = makeDeleteRequestMessage(t, netstorage.DatabaseDelete)

	assert.Empty(t, other.Measurement, "expected value of Measurement is empty, got: %v", other.Measurement)
	assert.Empty(t, other.ShardIds, "expected value of ShardIds is empty, got: %+v", other.ShardIds)
	assert.Empty(t, other.Rp, "expected value of Rp is empty, got: %+v", other.Rp)
}

func TestShowTagValuesRequest(t *testing.T) {
	req := &netstorage.ShowTagValuesRequest{}
	req.Db = proto.String("db0")

	tagKeys := make(map[string]map[string]struct{})
	tagKeys["cpu"] = map[string]struct{}{
		"hostname": {},
		"role":     {},
	}
	tagKeys["memory"] = map[string]struct{}{
		"hostname": {},
		"max":      {},
	}
	req.SetTagKeys(tagKeys)
	buf, err := req.MarshalBinary()
	if !assert.NoError(t, err) {
		return
	}

	other := &netstorage.ShowTagValuesRequest{}
	if !assert.NoError(t, other.UnmarshalBinary(buf)) {
		return
	}

	got := make(map[string]map[string]struct{})
	for mst, items := range other.GetTagKeysBytes() {
		got[mst] = make(map[string]struct{})
		for _, item := range items {
			got[mst][string(item)] = struct{}{}
		}
	}

	assert.Equal(t, tagKeys, got)
}

func TestShowTagValuesResponse(t *testing.T) {
	resp := &netstorage.ShowTagValuesResponse{}

	set := netstorage.TableTagSets{
		Name: "cpu",
		Values: netstorage.TagSets{
			{
				Key:   "hostname",
				Value: "127.0.0.1",
			},
			{
				Key:   "hostname",
				Value: "127.0.0.2",
			},
			{
				Key:   "role",
				Value: "store",
			},
		},
	}

	sets := netstorage.TablesTagSets{set}
	resp.SetTagValuesSlice(sets)

	buf, err := resp.MarshalBinary()
	if !assert.NoError(t, err) {
		return
	}

	other := &netstorage.ShowTagValuesResponse{}
	if !assert.NoError(t, other.UnmarshalBinary(buf)) {
		return
	}

	assert.Equal(t, sets, other.GetTagValuesSlice())
}

func TestWritePointsRequest(t *testing.T) {
	req := netstorage.NewWritePointsRequest([]byte{1, 2, 3, 4, 5, 6, 7})
	other, ok := assertCodec(t, req, true, false)
	if !ok {
		return
	}

	assert.Equal(t, req.Points(), other.(*netstorage.WritePointsRequest).Points())
}

func TestWritePointsResponse(t *testing.T) {
	req := netstorage.NewWritePointsResponse(1, 0, "ok")

	other, ok := assertCodec(t, req, true, true)
	if !ok {
		return
	}

	assert.Equal(t, req, other.(*netstorage.WritePointsResponse))
}

func TestStreamVar(t *testing.T) {
	sv := &netstorage.StreamVar{Only: false, Id: []uint64{0, 1}}
	if _, ok := sv.Instance().(*netstorage.StreamVar); !ok {
		t.Fatal("StreamVar Instance failed")
	}

	var buf []byte
	sv = &netstorage.StreamVar{}
	assert.Equal(t, sv.Unmarshal(buf), nil)
}

func TestWriteStreamPointsRequest(t *testing.T) {
	req := netstorage.NewWriteStreamPointsRequest([]byte{1, 2, 3, 4, 5, 6, 7}, []*netstorage.StreamVar{
		{Only: false, Id: []uint64{1}}, {Only: false, Id: []uint64{2}},
	})

	other, ok := assertCodec(t, req, true, false)
	if !ok {
		return
	}

	assert.Equal(t, req.StreamVars(), other.(*netstorage.WriteStreamPointsRequest).StreamVars())

	var err error
	var buf []byte
	buf, err = req.Marshal(buf[:0])
	if err != nil {
		t.Fatal("WriteStreamPointsRequest Marshal failed")
	}
	err = req.Unmarshal(buf)
	if err != nil {
		t.Fatal("WriteStreamPointsRequest Unmarshal failed")
	}
}

func TestWriteStreamPointsResponse(t *testing.T) {
	req := netstorage.NewWriteStreamPointsResponse(1, 0, "ok")

	other, ok := assertCodec(t, req, true, true)
	if !ok {
		return
	}
	assert.Equal(t, req, other.(*netstorage.WriteStreamPointsResponse))

	var err error
	var buf []byte
	buf, err = req.Marshal(buf[:0])
	if err != nil {
		t.Fatal("WriteStreamPointsResponse Marshal failed")
	}
	err = req.Unmarshal(buf)
	if err != nil {
		t.Fatal("WriteStreamPointsResponse Unmarshal failed")
	}
	assert.Equal(t, req.Size(), len(buf))

}

func TestInvalidDDLMessage(t *testing.T) {
	msg := &netstorage.DDLMessage{}
	err := msg.Unmarshal(nil)
	if !assert.EqualError(t, err, errno.NewError(errno.ShortBufferSize, 0, 0).Error()) {
		return
	}

	buf := []byte{129}
	assert.EqualError(t, msg.Unmarshal(buf), fmt.Sprintf("unknown message type: %d", buf[0]))
}

func TestSysCtrlRequest(t *testing.T) {
	req := &netstorage.SysCtrlRequest{}
	req.SetMod("test")
	req.SetParam(map[string]string{"sex": "male", "country": "cn"})

	obj, ok := assertCodec(t, req, false, false)
	if !ok {
		return
	}
	other := obj.(*netstorage.SysCtrlRequest)

	assert.Equal(t, req.Mod(), other.Mod())
	assert.Equal(t, req.Param(), other.Param())
}

func TestSysCtrlResponse(t *testing.T) {
	req := &netstorage.SysCtrlResponse{}
	req.SetResult(map[string]string{"sex": "male", "country": "cn"})
	req.SetErr("some error")

	obj, ok := assertCodec(t, req, false, false)
	if !ok {
		return
	}
	other := obj.(*netstorage.SysCtrlResponse)

	assert.Equal(t, req.Result(), other.Result())
	assert.Equal(t, req.Error(), other.Error())
}

func assertCodec(t *testing.T, obj transport.Codec, assertSize bool, assertUnmarshalNil bool) (transport.Codec, bool) {
	buf, err := obj.Marshal(nil)
	if !assert.NoError(t, err) {
		return nil, false
	}

	if assertSize && !assert.Equal(t, len(buf), obj.Size(),
		"invalid size, exp: %d, got: %d", len(buf), obj.Size()) {
		return nil, false
	}

	other := obj.Instance()
	if assertUnmarshalNil && !assert.EqualError(t, other.Unmarshal(nil), errno.NewError(errno.ShortBufferSize, 0, 0).Error()) {
		return nil, false
	}

	if !assert.NoError(t, other.Unmarshal(buf)) {
		return nil, false
	}

	return other, true
}

func Test_PtRequest_Marshal_Unmarshal(t *testing.T) {
	req := &netstorage.PtRequest{
		PtRequest: netdata.PtRequest{
			Pt: &proto2.DbPt{
				Db: proto.String("DB"),
				Pt: &proto2.PtInfo{
					Owner: &proto2.PtOwner{
						NodeID: proto.Uint64(4),
					},
					Status: proto.Uint32(0),
					PtId:   proto.Uint32(1),
				},
			},
			MigrateType: proto.Int32(2),
			OpId:        proto.Uint64(1234),
		},
	}
	buf, err := req.Marshal(nil)
	require.NoError(t, err)
	req2 := &netstorage.PtRequest{}
	err = req2.Unmarshal(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.String(), req2.String())
}

func Test_PtResponse_Marshal_Unmarshal(t *testing.T) {
	resp := &netstorage.PtResponse{
		PtResponse: netdata.PtResponse{
			Err: proto.String("error"),
		},
	}
	buf, err := resp.Marshal(nil)
	require.NoError(t, err)
	resp2 := &netstorage.PtResponse{}
	err = resp2.Unmarshal(buf)
	require.NoError(t, err)
	require.EqualValues(t, resp.String(), resp2.String())
}

func TestShowQueriesRequest_Marshal_Unmarshal(t *testing.T) {
	req := &netstorage.ShowQueriesRequest{}
	buf, err := req.MarshalBinary()
	require.NoError(t, err)

	req2 := &netstorage.ShowQueriesRequest{}
	err = req2.UnmarshalBinary(buf)
	require.NoError(t, err)
}

func TestShowQueriesResponse_Marshal_Unmarshal(t *testing.T) {
	resp := &netstorage.ShowQueriesResponse{
		QueryExeInfos: []*netstorage.QueryExeInfo{{
			QueryID:   1,
			Stmt:      "SELECT * FROM mst1",
			Database:  "db1",
			BeginTime: time.Now().UnixNano(),
			RunState:  netstorage.Running,
		}, {
			QueryID:   2,
			Stmt:      "SELECT * FROM mst2",
			Database:  "db2",
			BeginTime: time.Now().UnixNano(),
			RunState:  netstorage.Running,
		}, {
			QueryID:   3,
			Stmt:      "SELECT * FROM mst3",
			Database:  "db3",
			BeginTime: time.Now().UnixNano(),
			RunState:  netstorage.Killed,
		}},
	}
	buf, err := resp.MarshalBinary()
	require.NoError(t, err)

	resp2 := &netstorage.ShowQueriesResponse{}
	err = resp2.UnmarshalBinary(buf)
	require.NoError(t, err)
	for i := range resp.QueryExeInfos {
		assert.Equal(t, resp.QueryExeInfos[i], resp2.QueryExeInfos[i])
	}
}

func TestKillQueryRequest_Marshal_Unmarshal(t *testing.T) {
	req := &netstorage.KillQueryRequest{}
	req.QueryID = proto.Uint64(1)
	buf, err := req.MarshalBinary()
	require.NoError(t, err)
	req2 := &netstorage.KillQueryRequest{}
	err = req2.UnmarshalBinary(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.String(), req2.String())
}

func TestKillQueryResponse_Marshal_Unmarshal(t *testing.T) {
	resp := &netstorage.KillQueryResponse{}
	resp.ErrCode = proto.Uint32(1234)
	buf, err := resp.MarshalBinary()
	require.NoError(t, err)
	resp2 := &netstorage.KillQueryResponse{}
	err = resp2.UnmarshalBinary(buf)
	require.NoError(t, err)
	require.EqualValues(t, resp.String(), resp2.String())
}

func TestSegregateNodeRequest_Marshal_Unmarshal(t *testing.T) {
	req := netstorage.NewSegregateNodeRequest()
	req.NodeId = proto.Uint64(1)
	var buf []byte
	buf, err := req.Marshal(buf)
	require.NoError(t, err)
	req2 := netstorage.NewSegregateNodeRequest()
	err = req2.Unmarshal(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.String(), req2.String())
	newReq := req.Instance()
	if newReq.Size() != 0 {
		t.Fatal("newReq.size() error")
	}
}

func TestSegregateNodeResponse_Marshal_Unmarshal(t *testing.T) {
	resp := netstorage.NewSegregateNodeResponse()
	var msg string = "error"
	resp.Err = &msg
	var buf []byte
	buf, err := resp.Marshal(buf)
	require.NoError(t, err)
	resp2 := netstorage.NewSegregateNodeResponse()
	err = resp2.Unmarshal(buf)
	require.NoError(t, err)
	require.EqualValues(t, resp.String(), resp2.String())
	newResp := resp.Instance()
	if newResp.Size() != 0 {
		t.Fatal("newResp.size() error")
	}
	if newResp.(*netstorage.SegregateNodeResponse).Error() != nil {
		t.Fatal("newResp.Error() error")
	}
}

func TestRaftMsgRequest_Marshal_Unmarshal(t *testing.T) {
	req := &netstorage.RaftMessagesRequest{
		Database:    "db0",
		PtId:        2,
		RaftMessage: raftpb.Message{Type: 1, To: 2, From: 3, Commit: 100},
	}
	buf, err := req.MarshalBinary()
	require.NoError(t, err)
	req2 := &netstorage.RaftMessagesRequest{}
	err = req2.UnmarshalBinary(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.Database, req2.Database)
	require.EqualValues(t, req.PtId, req2.PtId)
	require.EqualValues(t, req, req2)
}

func TestRaftMsgResponse_Marshal_Unmarshal(t *testing.T) {
	req := &netstorage.RaftMessagesResponse{}
	req.ErrMsg = proto.String("mock error")
	buf, err := req.MarshalBinary()
	require.NoError(t, err)
	req2 := &netstorage.RaftMessagesResponse{}
	err = req2.UnmarshalBinary(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.String(), req2.String())
}

func Test_ShowClusterRequest_Unmarshal(t *testing.T) {
	req := &message.ShowClusterRequest{}
	buf, err := req.Marshal(nil)
	assert.NoError(t, err)
	myReq := &message.ShowClusterRequest{}
	err = myReq.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, req, myReq)
	err = myReq.Unmarshal(nil)
	assert.NoError(t, err)
	assert.Equal(t, req, myReq)
	newRep := req.Instance()
	if newRep.Size() == 0 {
		t.Fatal("newRep.size() error")
	}
}

func Test_ShowClusterResponse_Unmarshal(t *testing.T) {
	resp := &message.ShowClusterResponse{}
	buf, err := resp.Marshal(nil)
	assert.NoError(t, err)
	myResp := &message.ShowClusterResponse{}
	err = myResp.Unmarshal(buf)
	assert.NoError(t, err)
	assert.Equal(t, resp, myResp)
	err = myResp.Unmarshal(nil)
	assert.NoError(t, err)
	assert.Equal(t, resp, myResp)
	newResp := resp.Instance()
	if newResp.Size() == 0 {
		t.Fatal("newResp.size() error")
	}
}
