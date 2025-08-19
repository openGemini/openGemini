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

package handler

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	internal "github.com/openGemini/openGemini/lib/msgservice/data"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/raftconn"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dataPath = "/tmp/test_data"

type handlerItem struct {
	typ uint8
	msg codec.BinaryCodec
}

type mockCodec struct {
}

func (r *mockCodec) MarshalBinary() ([]byte, error) {
	return []byte{1}, nil
}

func (r *mockCodec) UnmarshalBinary(buf []byte) error {
	return nil
}

func TestBaseHandler_SetMessage(t *testing.T) {
	var items = []handlerItem{
		{
			typ: msgservice.SeriesKeysRequestMessage,
			msg: &msgservice.SeriesKeysRequest{},
		},
		{
			typ: msgservice.SeriesExactCardinalityRequestMessage,
			msg: &msgservice.SeriesExactCardinalityRequest{},
		},
		{
			typ: msgservice.SeriesCardinalityRequestMessage,
			msg: &msgservice.SeriesCardinalityRequest{},
		},
		{
			typ: msgservice.ShowTagValuesRequestMessage,
			msg: &msgservice.ShowTagValuesRequest{},
		},
		{
			typ: msgservice.ShowTagValuesCardinalityRequestMessage,
			msg: &msgservice.ShowTagValuesCardinalityRequest{},
		},
		{
			typ: msgservice.GetShardSplitPointsRequestMessage,
			msg: &msgservice.GetShardSplitPointsRequest{},
		},
		{
			typ: msgservice.DeleteRequestMessage,
			msg: &msgservice.DeleteRequest{},
		},
		{
			typ: msgservice.CreateDataBaseRequestMessage,
			msg: &msgservice.CreateDataBaseRequest{},
		},
		{
			typ: msgservice.ShowTagKeysRequestMessage,
			msg: &msgservice.ShowTagKeysRequest{},
		},
		{
			typ: msgservice.DropSeriesRequestMessage,
			msg: &msgservice.DropSeriesRequest{},
		},
	}

	for _, item := range items {
		h := NewHandler(item.typ)
		require.NoError(t, h.SetMessage(item.msg))
		require.NotNil(t, h.SetMessage(&mockCodec{}))
	}
}

func TestCreateDataBase_Process(t *testing.T) {
	db := path.Join(dataPath, "db0")
	pt := uint32(1)
	rp := "rp0"

	h := NewHandler(msgservice.CreateDataBaseRequestMessage)
	if err := h.SetMessage(&msgservice.CreateDataBaseRequest{
		CreateDataBaseRequest: internal.CreateDataBaseRequest{
			Db: &db,
			Pt: &pt,
			Rp: &rp,
		},
	}); err != nil {
		t.Fatal(err)
	}

	h.SetStore(&storage.Storage{})

	rsp, _ := h.Process()
	response, ok := rsp.(*msgservice.CreateDataBaseResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())
	response.Err = nil

	fileops.MkdirAll(dataPath, 0750)
	defer fileops.RemoveAll(dataPath)
	rsp, _ = h.Process()
	response, ok = rsp.(*msgservice.CreateDataBaseResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.Nil(t, response.Error())
}

type mockShard struct {
	engine.Shard
}

func (m *mockShard) GetDataPath() string {
	return dataPath + "/shard0"
}

func (m *mockShard) GetID() uint64 {
	return 0
}

func (s *mockShard) GetEngineType() config.EngineType {
	return 0
}

func (s *mockShard) GetRPName() string {
	return "rp0"
}

type MockJSONMarshaller struct {
	MarshalFunc func(v interface{}) ([]byte, error)
}

func (m *MockJSONMarshaller) Marshal(v interface{}) ([]byte, error) {
	return nil, errors.New("error json")
}

func (m *mockShard) GetIndexBuilder() *tsi.IndexBuilder {
	ltime := uint64(time.Now().Unix())
	lockPath := ""
	path := os.TempDir()
	indexIdent := &meta.IndexIdentifier{OwnerDb: "db0", OwnerPt: 1, Policy: "rp0"}
	indexIdent.Index = &meta.IndexDescriptor{IndexID: 2,
		IndexGroupID: 3, TimeRange: meta.TimeRangeInfo{}}

	opts := new(tsi.Options).
		Path(path).
		Ident(indexIdent).
		IndexType(index.MergeSet).
		EngineType(config.TSSTORE).
		StartTime(time.Now()).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		LogicalClock(1).
		SequenceId(&ltime).
		Lock(&lockPath)

	indexBuilder := tsi.NewIndexBuilder(opts)

	primaryIndex := &mockMergeSetIndex{}
	indexRelation, _ := tsi.NewIndexRelation(opts, primaryIndex, indexBuilder)
	indexBuilder.Relations[uint32(index.MergeSet)] = indexRelation
	return indexBuilder
}

type mockMergeSetIndex struct {
	tsi.MergeSetIndex
}

func (m *mockMergeSetIndex) SearchSeriesByTableAndCond(name []byte, expr influxql.Expr, tr tsi.TimeRange) ([]uint64, error) {
	r := make([]uint64, 0)
	if len(name) == 0 {
		return r, nil
	}
	r = append(r, 111)
	return r, nil
}

type MockMEngine struct {
	engine.Engine
}

const (
	DelIndexBuilderId = math.MaxUint64
)

func (m *MockMEngine) GetDatabase(database string) map[uint32]*engine.DBPTInfo {
	shards := map[uint64]engine.Shard{
		11: &mockShard{},
	}
	node := &raftconn.RaftNode{DataCommittedC: make(map[uint64]chan error), Identity: "db_1"}
	dBPT := &engine.DBPTInfo{}
	dBPT.SetDelIndexBuilder(make(map[string]*tsi.IndexBuilder))
	dBPT.SetShards(shards)
	dBPT.SetNode(node)
	dBPT.SetDatabase("db0")
	timeRangeInfo := &meta.ShardTimeRangeInfo{
		ShardDuration: &meta.ShardDurationInfo{
			DurationInfo: meta.DurationDescriptor{Duration: time.Second}},
		OwnerIndex: meta.IndexDescriptor{IndexID: DelIndexBuilderId},
	}
	dBPT.SetPath(dataPath)
	dBPT.SetWalPath(dataPath + "wal")
	lockPath := path.Join(dataPath, "LOCK")
	dBPT.SetLockPath(&lockPath)

	dBPT.SetIndexBuilder(make(map[uint64]*tsi.IndexBuilder))
	rp := "rp0"
	msInfo := &meta.MeasurementInfo{
		EngineType: config.TSSTORE,
	}
	client := &mockMetaClient{}
	dBPT.NewShard(rp, uint64(66), timeRangeInfo, client, msInfo)

	mapDBPTInfo := map[uint32]*engine.DBPTInfo{
		1: dBPT,
	}
	return mapDBPTInfo
}

type mockMetaClient struct {
	metaclient.MetaClient
}

func (m *MockMEngineNoDeletedTsids) GetDatabase(database string) map[uint32]*engine.DBPTInfo {
	shards := map[uint64]engine.Shard{
		11: &mockShard{},
	}
	node := &raftconn.RaftNode{DataCommittedC: make(map[uint64]chan error), Identity: "db_1"}
	dBPT := &engine.DBPTInfo{}
	dBPT.SetDelIndexBuilder(make(map[string]*tsi.IndexBuilder))
	dBPT.SetShards(shards)
	dBPT.SetNode(node)
	dBPT.SetDatabase("db0")
	timeRangeInfo := &meta.ShardTimeRangeInfo{
		ShardDuration: &meta.ShardDurationInfo{
			DurationInfo: meta.DurationDescriptor{Duration: time.Second}},
		OwnerIndex: meta.IndexDescriptor{IndexID: uint64(666)},
	}
	dBPT.SetPath(dataPath)
	dBPT.SetWalPath(path.Join(dataPath, "wal"))
	lockPath := path.Join(dataPath, "LOCK")
	dBPT.SetLockPath(&lockPath)

	dBPT.SetIndexBuilder(make(map[uint64]*tsi.IndexBuilder))
	rp := "rp0"
	msInfo := &meta.MeasurementInfo{
		EngineType: config.TSSTORE,
	}
	client := &mockMetaClient{}
	dBPT.NewShard(rp, uint64(66), timeRangeInfo, client, msInfo)

	mapDBPTInfo := map[uint32]*engine.DBPTInfo{
		1: dBPT,
	}
	return mapDBPTInfo
}

func (m *MockMEngineNoDeletedTsids) OpenShardLazy(sh engine.Shard) error {
	return nil
}

type MockMEngineNoDeletedTsids struct {
	engine.Engine
}

func (m *mockMetaClient) DatabaseOption(name string) (*obs.ObsOptions, error) {
	return nil, nil
}

func (m *MockMEngine) OpenShardLazy(sh engine.Shard) error {
	return nil
}

type MockMEngineError struct {
	engine.Engine
}

func (m *MockMEngineError) OpenShardLazy(sh engine.Shard) error {
	return errors.New("error")
}

func (m *MockMEngineError) GetDatabase(database string) map[uint32]*engine.DBPTInfo {
	shards := map[uint64]engine.Shard{
		11: &mockShard{},
	}
	node := &raftconn.RaftNode{DataCommittedC: make(map[uint64]chan error), Identity: "db_1"}
	dBPT := &engine.DBPTInfo{}
	dBPT.SetShards(shards)
	dBPT.SetNode(node)
	dBPT.SetDatabase("db0")

	mapDBPTInfo := map[uint32]*engine.DBPTInfo{
		1: dBPT,
	}
	return mapDBPTInfo
}

func TestDropSeries_Process(t *testing.T) {
	db := "db0"
	pt := []uint32{1}
	condition := "tag1 = 'a1'"
	measurements := []string{"mst_000"}

	h := NewHandler(msgservice.DropSeriesRequestMessage)
	if err := h.SetMessage(&msgservice.DropSeriesRequest{
		DropSeriesRequest: internal.DropSeriesRequest{
			Db:           &db,
			PtIDs:        pt,
			Measurements: measurements,
			Condition:    &condition,
		},
	}); err != nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}

	s.SetEngine(&MockMEngineError{})
	h.SetStore(s)
	_, e := h.Process()
	require.ErrorContains(t, e, "error")

	s.SetEngine(&MockMEngine{})
	h.SetStore(s)

	rsp, _ := h.Process()
	response, ok := rsp.(*msgservice.DropSeriesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())
	response.Err = nil

	client := metaclient.NewClient("", false, 0)
	s.SetMetaClient(client)
	s.SetEngine(&MockMEngineNoDeletedTsids{})
	h.SetStore(s)

	rsp, e = h.Process()
	if e != nil {
		t.Fatal("execute h.Process failed")
	}

	response, ok = rsp.(*msgservice.DropSeriesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())

	fileops.MkdirAll(dataPath, 0750)
	defer fileops.RemoveAll(dataPath)
	rsp, _ = h.Process()
	response, ok = rsp.(*msgservice.DropSeriesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.Nil(t, response.Error())

	db = "db0"
	pt = []uint32{1}
	condition = "tag1 = 'a1'"
	measurements = []string{""}
	h = NewHandler(msgservice.DropSeriesRequestMessage)
	if err := h.SetMessage(&msgservice.DropSeriesRequest{
		DropSeriesRequest: internal.DropSeriesRequest{
			Db:           &db,
			PtIDs:        pt,
			Measurements: measurements,
			Condition:    &condition,
		},
	}); err != nil {
		t.Fatal(err)
	}
	s.SetEngine(&MockMEngine{})
	h.SetStore(s)

	rsp, _ = h.Process()
	_, ok = rsp.(*msgservice.DropSeriesResponse)
	require.True(t, ok)
}

func TestDropSeriesConditionError_Process(t *testing.T) {
	db := "db0"
	pt := []uint32{1}
	condition := "tag1 ="
	measurements := []string{"mst_000"}

	h := NewHandler(msgservice.DropSeriesRequestMessage)
	if err := h.SetMessage(&msgservice.DropSeriesRequest{
		DropSeriesRequest: internal.DropSeriesRequest{
			Db:           &db,
			PtIDs:        pt,
			Measurements: measurements,
			Condition:    &condition,
		},
	}); err != nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}
	s.SetEngine(&MockMEngine{})
	h.SetStore(s)

	_, e := h.Process()
	_, ok := e.(*influxql.ParseError)
	require.True(t, ok)
}

var clientIDs = []uint64{1000, 2000, 3000}
var mockQueriesNum = 10

type mockQuery struct {
	id   int
	info *msgservice.QueryExeInfo
}

func (m *mockQuery) Abort(source string) {}

func (m *mockQuery) Crash() {}

func (m *mockQuery) GetQueryExeInfo() *msgservice.QueryExeInfo {
	return m.info
}

func generateMockQueryExeInfos(clientID uint64, n int) []mockQuery {
	res := make([]mockQuery, mockQueriesNum)
	for i := 0; i < n; i++ {
		q := mockQuery{id: i, info: &msgservice.QueryExeInfo{
			QueryID:   clientID + uint64(i),
			Stmt:      fmt.Sprintf("select * from mst%d\n", i),
			Database:  fmt.Sprintf("db%d", i),
			BeginTime: int64(i * 10000000),
			RunState:  msgservice.Running,
		}}
		res[i] = q
	}
	return res
}

func TestShowQueries_Process(t *testing.T) {
	resp := &EmptyResponser{}
	resp.session = spdy.NewMultiplexedSession(spdy.DefaultConfiguration(), nil, 0)

	except := make([]*msgservice.QueryExeInfo, 0)

	for _, cid := range clientIDs {
		// generate mock infos for all clients
		queries := generateMockQueryExeInfos(cid, mockQueriesNum)
		for i, mQuery := range queries {
			qm := query.NewManager(cid)

			qid := mQuery.GetQueryExeInfo().QueryID

			qm.Add(qid, &queries[i])
			except = append(except, mQuery.GetQueryExeInfo())
		}
	}

	// Simulate show queries, get the queryInfos
	h := NewHandler(msgservice.ShowQueriesRequestMessage)
	if err := h.SetMessage(&msgservice.ShowQueriesRequest{}); err != nil {
		t.Fatal(err)
	}
	h.SetStore(&storage.Storage{})

	rsp, err := h.Process()
	if err != nil {
		t.Fatal(err)
	}

	response, ok := rsp.(*msgservice.ShowQueriesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}

	res := response.QueryExeInfos
	// sort res and except to assert
	sort.Slice(res, func(i, j int) bool {
		return res[i].QueryID > res[j].QueryID
	})
	sort.Slice(except, func(i, j int) bool {
		return except[i].QueryID > except[j].QueryID
	})

	for i := range except {
		assert.Equal(t, except[i].QueryID, res[i].QueryID)
		assert.Equal(t, except[i].Stmt, res[i].Stmt)
		assert.Equal(t, except[i].Database, res[i].Database)
	}

	assert.Equal(t, len(clientIDs)*mockQueriesNum, len(res))
}

func TestKillQuery_Process(t *testing.T) {
	abortedQID := clientIDs[0] + 1

	resp := &EmptyResponser{}
	resp.session = spdy.NewMultiplexedSession(spdy.DefaultConfiguration(), nil, 0)

	h := NewHandler(msgservice.KillQueryRequestMessage)
	req := msgservice.KillQueryRequest{}
	req.QueryID = proto.Uint64(abortedQID)
	if err := h.SetMessage(&req); err != nil {
		t.Fatal(err)
	}
	h.SetStore(&storage.Storage{})

	for _, cid := range clientIDs {
		// generate mock infos for all clients
		queries := generateMockQueryExeInfos(cid, mockQueriesNum)
		for i, mQuery := range queries {
			qm := query.NewManager(cid)

			qid := mQuery.GetQueryExeInfo().QueryID

			qm.Add(qid, &queries[i])
		}
	}

	rsp, err := h.Process()
	if err != nil {
		t.Fatal(err)
	}
	response, ok := rsp.(*msgservice.KillQueryResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NoError(t, response.Error())
	assert.Equal(t, true, query.NewManager(clientIDs[0]).Aborted(abortedQID))
}

func TestShowTagKeys_Process(t *testing.T) {
	db := path.Join(dataPath, "db0")
	condition := "tag1=tagkey"

	h := NewHandler(msgservice.ShowTagKeysRequestMessage)
	req := msgservice.ShowTagKeysRequest{}
	req.Db = &db
	req.Condition = &condition
	req.PtIDs = []uint32{1}
	req.Measurements = []string{"mst"}
	if err := h.SetMessage(&req); err != nil {
		t.Fatal(err)
	}
	st := storage.Storage{}
	st.SetEngine(&MockEngine{})
	h.SetStore(&st)
	rsp, err := h.Process()
	if err != nil {
		t.Fatal(err)
	}
	response, ok := rsp.(*msgservice.ShowTagKeysResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NoError(t, response.Error())
}
