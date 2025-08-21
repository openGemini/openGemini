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

package msgservice_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/msgservice"
	netdata "github.com/openGemini/openGemini/lib/msgservice/data"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestSeriesKeysRequestMessage(t *testing.T) {
	req := &msgservice.SeriesKeysRequest{}
	req.Db = proto.String("db0")
	req.PtIDs = []uint32{1, 2, 3, 4, 5}
	req.Condition = proto.String("a=1 and b=2")
	req.Measurements = []string{"mst0", "mst1"}

	msg := msgservice.NewDDLMessage(msgservice.SeriesKeysRequestMessage, req)
	buf, err := msg.Marshal(nil)
	if !assert.NoError(t, err) {
		return
	}

	msg2 := msgservice.NewDDLMessage(msgservice.SeriesKeysRequestMessage, &msgservice.SeriesKeysRequest{})
	if !assert.NoError(t, msg2.Unmarshal(buf)) {
		return
	}

	other, ok := msg2.Data.(*msgservice.SeriesKeysRequest)
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
	resp := &msgservice.SeriesKeysResponse{}
	resp.Series = []string{"cpu,hostname=127.0.0.1,role=store", "memory,hostname=127.0.0.2,role=sql"}
	resp.Err = proto.String("no errors")

	buf, err := resp.MarshalBinary()
	if !assert.NoError(t, err) {
		return
	}

	other := &msgservice.SeriesKeysResponse{}
	if !assert.NoError(t, other.UnmarshalBinary(buf)) {
		return
	}

	if !reflect.DeepEqual(resp.GetSeries(), other.GetSeries()) ||
		resp.Error().Error() != other.Error().Error() {

		t.Fatalf("codec SeriesKeysResponse failed \nexp: %+v \ngot: %+v",
			resp, other)
	}
}

func makeDeleteRequestMessage(t *testing.T, typ msgservice.DeleteType) (*msgservice.DeleteRequest, *msgservice.DeleteRequest) {
	req := &msgservice.DeleteRequest{}
	req.Database = "db0"
	req.Type = typ
	req.Measurement = "mst_0"
	req.Rp = "test_1"
	req.ShardIds = []uint64{12, 3, 4, 5}

	msg := msgservice.NewDDLMessage(msgservice.DeleteRequestMessage, req)
	buf, err := msg.Marshal(nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	msg2 := msg.Instance()
	if err := msg2.Unmarshal(buf); err != nil {
		t.Fatalf("%v", err)
	}

	other, ok := msg2.(*msgservice.DDLMessage).Data.(*msgservice.DeleteRequest)
	if !ok {
		t.Fatalf("unmarshal DeleteRequest failed")
	}

	return req, other
}

func TestDeleteRequestMessage(t *testing.T) {
	req, other := makeDeleteRequestMessage(t, msgservice.MeasurementDelete)
	if !assert.Equal(t, req, other) {
		return
	}

	req, other = makeDeleteRequestMessage(t, msgservice.DatabaseDelete)

	assert.Empty(t, other.Measurement, "expected value of Measurement is empty, got: %v", other.Measurement)
	assert.Empty(t, other.ShardIds, "expected value of ShardIds is empty, got: %+v", other.ShardIds)
	assert.Empty(t, other.Rp, "expected value of Rp is empty, got: %+v", other.Rp)
}

func TestShowTagValuesRequest(t *testing.T) {
	req := &msgservice.ShowTagValuesRequest{}
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

	other := &msgservice.ShowTagValuesRequest{}
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
	resp := &msgservice.ShowTagValuesResponse{}

	set := influxql.TableTagSets{
		Name: "cpu",
		Values: influxql.TagSets{
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

	sets := influxql.TablesTagSets{set}
	resp.SetTagValuesSlice(sets)

	buf, err := resp.MarshalBinary()
	if !assert.NoError(t, err) {
		return
	}

	other := &msgservice.ShowTagValuesResponse{}
	if !assert.NoError(t, other.UnmarshalBinary(buf)) {
		return
	}

	assert.Equal(t, sets, other.GetTagValuesSlice())
}

func TestWritePointsRequest(t *testing.T) {
	req := msgservice.NewWritePointsRequest([]byte{1, 2, 3, 4, 5, 6, 7})
	other, ok := assertCodec(t, req, true, false)
	if !ok {
		return
	}

	assert.Equal(t, req.Points(), other.(*msgservice.WritePointsRequest).Points())
}

func TestWritePointsResponse(t *testing.T) {
	req := msgservice.NewWritePointsResponse(1, 0, "ok")

	other, ok := assertCodec(t, req, true, true)
	if !ok {
		return
	}

	assert.Equal(t, req, other.(*msgservice.WritePointsResponse))
}

func TestWriteBlobsRequest(t *testing.T) {
	group, _ := shelf.NewBlobGroup(1)
	req := msgservice.NewWriteBlobsRequest("db", "rp", 0, 0, group)
	other, ok := assertCodec(t, req, true, false)
	if !ok {
		return
	}

	assert.Equal(t, req.Db, other.(*msgservice.WriteBlobsRequest).Db)
	assert.Equal(t, req.Pt, other.(*msgservice.WriteBlobsRequest).Pt)
	assert.Equal(t, req.Rp, other.(*msgservice.WriteBlobsRequest).Rp)
	assert.Equal(t, req.Shard, other.(*msgservice.WriteBlobsRequest).Shard)
	assert.Equal(t, req.Bg.GetBlobs(), other.(*msgservice.WriteBlobsRequest).Bg.GetBlobs())

	group, _ = shelf.NewBlobGroup(1)
	req = msgservice.NewWriteBlobsRequest("db", "rp", 0, 0, group)
	other, ok = assertCodec(t, req, true, true)
	if !ok {
		return
	}

	group, _ = shelf.NewBlobGroup(0)
	req = msgservice.NewWriteBlobsRequest("db", "rp", 0, 0, group)
	other, ok = assertCodec(t, req, true, true)
	if !ok {
		return
	}

}

func TestWriteBlobsResponse(t *testing.T) {
	req := msgservice.NewWriteBlobsResponse(1, 0, "ok")

	other, ok := assertCodec(t, req, true, true)
	if !ok {
		return
	}

	assert.Equal(t, req, other.(*msgservice.WriteBlobsResponse))
}

func TestStreamVar(t *testing.T) {
	sv := &msgservice.StreamVar{Only: false, Id: []uint64{0, 1}}
	if _, ok := sv.Instance().(*msgservice.StreamVar); !ok {
		t.Fatal("StreamVar Instance failed")
	}

	var buf []byte
	sv = &msgservice.StreamVar{}
	assert.Equal(t, sv.Unmarshal(buf), nil)
}

func TestWriteStreamPointsRequest(t *testing.T) {
	req := msgservice.NewWriteStreamPointsRequest([]byte{1, 2, 3, 4, 5, 6, 7}, []*msgservice.StreamVar{
		{Only: false, Id: []uint64{1}}, {Only: false, Id: []uint64{2}},
	})

	other, ok := assertCodec(t, req, true, false)
	if !ok {
		return
	}

	assert.Equal(t, req.StreamVars(), other.(*msgservice.WriteStreamPointsRequest).StreamVars())

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
	req := msgservice.NewWriteStreamPointsResponse(1, 0, "ok")

	other, ok := assertCodec(t, req, true, true)
	if !ok {
		return
	}
	assert.Equal(t, req, other.(*msgservice.WriteStreamPointsResponse))

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
	msg := &msgservice.DDLMessage{}
	err := msg.Unmarshal(nil)
	if !assert.EqualError(t, err, errno.NewError(errno.ShortBufferSize, 0, 0).Error()) {
		return
	}

	buf := []byte{129}
	assert.EqualError(t, msg.Unmarshal(buf), fmt.Sprintf("unknown message type: %d", buf[0]))
}

func TestSysCtrlRequest(t *testing.T) {
	req := &msgservice.SysCtrlRequest{}
	req.SetMod("test")
	req.SetParam(map[string]string{"sex": "male", "country": "cn"})

	obj, ok := assertCodec(t, req, false, false)
	if !ok {
		return
	}
	other := obj.(*msgservice.SysCtrlRequest)

	assert.Equal(t, req.Mod(), other.Mod())
	assert.Equal(t, req.Param(), other.Param())
}

func TestSysCtrlResponse(t *testing.T) {
	req := &msgservice.SysCtrlResponse{}
	req.SetResult(map[string]string{"sex": "male", "country": "cn"})
	req.SetErr("some error")

	obj, ok := assertCodec(t, req, false, false)
	if !ok {
		return
	}
	other := obj.(*msgservice.SysCtrlResponse)

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
	req := &msgservice.PtRequest{
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
	req2 := &msgservice.PtRequest{}
	err = req2.Unmarshal(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.String(), req2.String())
}

func Test_PtResponse_Marshal_Unmarshal(t *testing.T) {
	resp := &msgservice.PtResponse{
		PtResponse: netdata.PtResponse{
			Err: proto.String("error"),
		},
	}
	buf, err := resp.Marshal(nil)
	require.NoError(t, err)
	resp2 := &msgservice.PtResponse{}
	err = resp2.Unmarshal(buf)
	require.NoError(t, err)
	require.EqualValues(t, resp.String(), resp2.String())
}

func TestShowQueriesRequest_Marshal_Unmarshal(t *testing.T) {
	req := &msgservice.ShowQueriesRequest{}
	buf, err := req.MarshalBinary()
	require.NoError(t, err)

	req2 := &msgservice.ShowQueriesRequest{}
	err = req2.UnmarshalBinary(buf)
	require.NoError(t, err)
}

func TestShowQueriesResponse_Marshal_Unmarshal(t *testing.T) {
	resp := &msgservice.ShowQueriesResponse{
		QueryExeInfos: []*msgservice.QueryExeInfo{{
			QueryID:   1,
			Stmt:      "SELECT * FROM mst1",
			Database:  "db1",
			BeginTime: time.Now().UnixNano(),
			RunState:  msgservice.Running,
		}, {
			QueryID:   2,
			Stmt:      "SELECT * FROM mst2",
			Database:  "db2",
			BeginTime: time.Now().UnixNano(),
			RunState:  msgservice.Running,
		}, {
			QueryID:   3,
			Stmt:      "SELECT * FROM mst3",
			Database:  "db3",
			BeginTime: time.Now().UnixNano(),
			RunState:  msgservice.Killed,
		}},
	}
	buf, err := resp.MarshalBinary()
	require.NoError(t, err)

	resp2 := &msgservice.ShowQueriesResponse{}
	err = resp2.UnmarshalBinary(buf)
	require.NoError(t, err)
	for i := range resp.QueryExeInfos {
		assert.Equal(t, resp.QueryExeInfos[i], resp2.QueryExeInfos[i])
	}
}

func TestKillQueryRequest_Marshal_Unmarshal(t *testing.T) {
	req := &msgservice.KillQueryRequest{}
	req.QueryID = proto.Uint64(1)
	buf, err := req.MarshalBinary()
	require.NoError(t, err)
	req2 := &msgservice.KillQueryRequest{}
	err = req2.UnmarshalBinary(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.String(), req2.String())
}

func TestKillQueryResponse_Marshal_Unmarshal(t *testing.T) {
	resp := &msgservice.KillQueryResponse{}
	resp.ErrCode = proto.Uint32(1234)
	buf, err := resp.MarshalBinary()
	require.NoError(t, err)
	resp2 := &msgservice.KillQueryResponse{}
	err = resp2.UnmarshalBinary(buf)
	require.NoError(t, err)
	require.EqualValues(t, resp.String(), resp2.String())
}

func TestSegregateNodeRequest_Marshal_Unmarshal(t *testing.T) {
	req := msgservice.NewSegregateNodeRequest()
	req.NodeId = proto.Uint64(1)
	var buf []byte
	buf, err := req.Marshal(buf)
	require.NoError(t, err)
	req2 := msgservice.NewSegregateNodeRequest()
	err = req2.Unmarshal(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.String(), req2.String())
	newReq := req.Instance()
	if newReq.Size() != 0 {
		t.Fatal("newReq.size() error")
	}
}

func TestSegregateNodeResponse_Marshal_Unmarshal(t *testing.T) {
	resp := msgservice.NewSegregateNodeResponse()
	var msg string = "error"
	resp.Err = &msg
	var buf []byte
	buf, err := resp.Marshal(buf)
	require.NoError(t, err)
	resp2 := msgservice.NewSegregateNodeResponse()
	err = resp2.Unmarshal(buf)
	require.NoError(t, err)
	require.EqualValues(t, resp.String(), resp2.String())
	newResp := resp.Instance()
	if newResp.Size() != 0 {
		t.Fatal("newResp.size() error")
	}
	if newResp.(*msgservice.SegregateNodeResponse).Error() != nil {
		t.Fatal("newResp.Error() error")
	}
}

func TestRaftMsgRequest_Marshal_Unmarshal(t *testing.T) {
	req := &msgservice.RaftMessagesRequest{
		Database:    "db0",
		PtId:        2,
		RaftMessage: raftpb.Message{Type: 1, To: 2, From: 3, Commit: 100},
	}
	buf, err := req.MarshalBinary()
	require.NoError(t, err)
	req2 := &msgservice.RaftMessagesRequest{}
	err = req2.UnmarshalBinary(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.Database, req2.Database)
	require.EqualValues(t, req.PtId, req2.PtId)
	require.EqualValues(t, req, req2)
}

func TestRaftMsgResponse_Marshal_Unmarshal(t *testing.T) {
	req := &msgservice.RaftMessagesResponse{}
	req.ErrMsg = proto.String("mock error")
	buf, err := req.MarshalBinary()
	require.NoError(t, err)
	req2 := &msgservice.RaftMessagesResponse{}
	err = req2.UnmarshalBinary(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.String(), req2.String())
}

func TestDropSeriesRequest_Marshal_Unmarshal(t *testing.T) {
	req := &msgservice.DropSeriesRequest{}
	db := "db0"
	cond := "a=2"
	req.PtIDs = []uint32{2}
	req.Db = &db
	req.Measurements = []string{"mst"}
	req.Condition = &cond
	buf, err := req.MarshalBinary()
	require.NoError(t, err)
	req2 := &msgservice.DropSeriesRequest{}
	err = req2.UnmarshalBinary(buf)
	require.NoError(t, err)
	require.Equal(t, req.Db, req2.Db)
	require.Equal(t, req.Condition, req2.Condition)
	require.Equal(t, req.Measurements, req2.Measurements)
	require.Equal(t, req.PtIDs, req2.PtIDs)
}

func TestDropSeriesResponse_Marshal_Unmarshal(t *testing.T) {
	req := &msgservice.DropSeriesResponse{}
	req.Err = proto.String("mock error")
	buf, err := req.MarshalBinary()
	require.NoError(t, err)
	req2 := &msgservice.DropSeriesResponse{}
	err = req2.UnmarshalBinary(buf)
	require.NoError(t, err)
	require.Equal(t, req.String(), req2.String())
}

func TestTransferLeadershipRequest_Marshal_Unmarshal(t *testing.T) {
	req := msgservice.NewTransferLeadershipRequest()
	req.NodeId = proto.Uint64(1)
	req.Database = proto.String("db0")
	req.PtId = proto.Uint32(0)
	req.NewMasterPtId = proto.Uint32(1)
	var buf []byte
	buf, err := req.Marshal(buf)
	require.NoError(t, err)
	req2 := msgservice.NewTransferLeadershipRequest()
	err = req2.Unmarshal(buf)
	require.NoError(t, err)
	require.EqualValues(t, req.String(), req2.String())
	newReq := req.Instance()
	if newReq.Size() != 0 {
		t.Fatal("newReq.size() error")
	}
}

func TestTransferLeadershipRequest_Marshal_Err(t *testing.T) {
	req := msgservice.NewTransferLeadershipRequest()
	req.NodeId = proto.Uint64(1)
	var buf []byte
	_, err := req.Marshal(buf)
	assert.NotEqual(t, err, nil)
}

func TestTransferLeadershipResponse_Marshal_Unmarshal(t *testing.T) {
	resp := msgservice.NewTransferLeadershipResponse()
	var msg string = "error"
	resp.Err = &msg
	var buf []byte
	buf, err := resp.Marshal(buf)
	require.NoError(t, err)
	resp2 := msgservice.NewTransferLeadershipResponse()
	err = resp2.Unmarshal(buf)
	require.NoError(t, err)
	require.EqualValues(t, resp.String(), resp2.String())
	newResp := resp.Instance()
	if newResp.Size() != 0 {
		t.Fatal("newResp.size() error")
	}
	if newResp.(*msgservice.TransferLeadershipResponse).Error() != nil {
		t.Fatal("newResp.Error() error")
	}
	if resp.Error() == nil {
		t.Fatal("resp.Error() error")
	}
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

func Test_RaftMsgMessage_Unmarshal(t *testing.T) {
	req := &msgservice.RaftMsgMessage{}
	err := req.Unmarshal([]byte{})
	assert.Equal(t, "invalid buffer size, expected greater than 0; actual 0", err.Error())
	err = req.Unmarshal([]byte{'a'})
	assert.Equal(t, "unknown message type: 97", err.Error())
	req.Instance()
}

func BenchmarkWriteBlobsRequest_Marshal(b *testing.B) {
	db := "test_db"
	rp := "test_rp"
	pt := uint32(123)
	shard := uint64(456)
	bg, f := shelf.NewBlobGroup(10)
	defer f()

	req := msgservice.NewWriteBlobsRequest(db, rp, pt, shard, bg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, 1024)
		_, err := req.Marshal(buf)
		if err != nil {
			b.Fatalf("Marshal failed: %v", err)
		}
	}

}

func BenchmarkWriteBlobsRequest_Unmarshal(b *testing.B) {
	db := "test_db"
	rp := "test_rp"
	pt := uint32(123)
	shard := uint64(456)

	bg, f := shelf.NewBlobGroup(10)
	defer f()

	req := msgservice.NewWriteBlobsRequest(db, rp, pt, shard, bg)

	buf, err := req.Marshal(nil)
	if err != nil {
		b.Fatalf("Marshal failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newReq := msgservice.NewWriteBlobsRequest("", "", 0, 0, nil)
		err = newReq.Unmarshal(buf)
		if err != nil {
			b.Fatalf("Unmarshal failed: %v", err)
		}
	}
}

func TestSendClearEventsRequestMarshal(t *testing.T) {
	var req *msgservice.SendClearEventsRequest
	var nodeid uint64 = 2
	var db string = "db0"
	var rp string = "autogen"
	var pt uint32 = 1
	var noClearIndex uint64 = 1
	var ClearIndex uint64 = 2
	var noClearShardId uint64 = 1
	var clearShardId uint64 = 2
	var pair *netdata.Pair = &netdata.Pair{
		IndexInfo: &netdata.IndexPair{
			NoClearIndex: &noClearIndex,
			ToClearIndex: &ClearIndex,
		},
		ShardInfo: []*netdata.ShardPair{
			&netdata.ShardPair{
				NoClearShard: &noClearShardId,
				ToClearShard: &clearShardId,
			},
		},
	}
	req = &msgservice.SendClearEventsRequest{
		ClearEventsRequest: netdata.ClearEventsRequest{
			NodeId:   &nodeid,
			Database: &db,
			Rp:       &rp,
			PtId:     &pt,
			Pair:     pair,
		},
	}
	buf, err := req.Marshal(nil)
	require.NoError(t, err)

	err = req.Unmarshal(buf)
	require.NoError(t, err)

	instance := req.Instance()
	require.NotNilf(t, instance, "")
	size := req.Size()
	require.Equal(t, 0, size)
}

func TestSendClearEventsResponseMarshal(t *testing.T) {
	var res *msgservice.SendClearEventsResponse
	res = &msgservice.SendClearEventsResponse{
		ClearEventsResponse: netdata.ClearEventsResponse{
			Err: nil,
		},
	}
	buf, err := res.Marshal(nil)
	require.NoError(t, err)

	err = res.Unmarshal(buf)
	require.NoError(t, err)

	instance := res.Instance()
	require.NotNilf(t, instance, "")
	size := res.Size()
	require.Equal(t, 0, size)

	err = res.Error()
	require.NoError(t, err)
	str := "mock error"
	res.Err = &str
	err = res.Error()
	require.Error(t, err)
}
