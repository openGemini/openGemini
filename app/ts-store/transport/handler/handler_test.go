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
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
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
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/smart_query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

func (e *MockMEngine) DbPTRef(db string, ptId uint32, read bool) error {
	return nil
}

func (e *MockMEngine) MarkDropSeries(db string, ptID uint32, mstName []byte, expr influxql.Expr, t tsi.TimeRange) error {
	return errors.New("error")
}

func (e *MockMEngine) DbPTUnref(db string, ptId uint32, read bool) {}

func (e *MockMEngine) GetTask(typ scheduler.TaskType, id uint64) (scheduler.Task, error) {
	switch typ {
	case scheduler.CompactTask:
		return &immutable.CompactTask{}, nil
	default:
		return nil, errors.New("wrong task type")
	}
}

func (e *MockMEngine) GetCompactTaskById(typ scheduler.TaskType, id uint64) (scheduler.Task, error) {
	return &immutable.CompactTask{}, nil
}

func (e *MockMEngine) CompactFiles(typ scheduler.TaskType, id uint64, info *immutable.CompactedFileInfo) error {
	return nil
}

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

func (m *MockMEngine) GetRPPTWriteStat(db, rp string) (meta.PTMstWriteStatus, error) {
	if db == "db0" && rp == "rp0" {
		return map[uint32]meta.MstWriteStatus{
			0: {
				"mst0": 5,
			},
		}, nil
	}
	return nil, errors.New("error")
}

type mockMetaClient struct {
	metaclient.MetaClient
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

func (e *MockMEngineError) DbPTRef(db string, ptId uint32, read bool) error {
	if db == "db1" {
		return errors.New("error DbPTRef db1")
	}
	return nil
}

func (e *MockMEngineError) MarkDropSeries(db string, ptID uint32, mstName []byte, expr influxql.Expr, t tsi.TimeRange) error {
	if db == "db2" {
		return errors.New("error MarkDropSeries db2")
	}
	return nil
}

func (e *MockMEngineError) DbPTUnref(db string, ptId uint32, read bool) {}

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

	s.SetEngine(&MockMEngine{})
	h.SetStore(s)
	rsp0, e := h.Process()
	if e != nil {
		t.Fatalf("execute h.Process failed: %v", e)
	}
	r, ok := rsp0.(*msgservice.DropSeriesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, r.GetErr())

	db1 := "db1"
	h1 := NewHandler(msgservice.DropSeriesRequestMessage)
	if err := h1.SetMessage(&msgservice.DropSeriesRequest{
		DropSeriesRequest: internal.DropSeriesRequest{
			Db:           &db1,
			PtIDs:        pt,
			Measurements: measurements,
			Condition:    &condition,
		},
	}); err != nil {
		t.Fatal(err)
	}
	s.SetEngine(&MockMEngineError{})
	h1.SetStore(s)
	rsp0, e = h1.Process()
	if e != nil {
		t.Fatalf("execute h.Process failed: %v", e)
	}
	r, ok = rsp0.(*msgservice.DropSeriesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	require.ErrorContains(t, r.Error(), "error DbPTRef db1")

	db2 := "db2"
	h2 := NewHandler(msgservice.DropSeriesRequestMessage)
	if err := h2.SetMessage(&msgservice.DropSeriesRequest{
		DropSeriesRequest: internal.DropSeriesRequest{
			Db:           &db2,
			PtIDs:        pt,
			Measurements: measurements,
			Condition:    &condition,
		},
	}); err != nil {
		t.Fatal(err)
	}
	s.SetEngine(&MockMEngineError{})
	h2.SetStore(s)
	rsp0, e = h2.Process()
	if e != nil {
		t.Fatalf("execute h.Process failed: %v", e)
	}
	r, ok = rsp0.(*msgservice.DropSeriesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	require.ErrorContains(t, r.Error(), "error MarkDropSeries db2")
}

func TestGetTask_Process(t *testing.T) {
	h := NewHandler(msgservice.GetTaskRequestMessage)
	if err := h.SetMessage(&msgservice.GetTaskRequest{}); err != nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}

	s.SetEngine(&MockMEngine{})
	h.SetStore(s)
	rsp0, e := h.Process()
	if e != nil {
		t.Fatalf("execute h.Process failed: %v", e)
	}
	r, ok := rsp0.(*msgservice.GetTaskResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.Nil(t, r.Error())

	if err := h.SetMessage(&msgservice.GetTaskRequest{Type: -1}); err != nil {
		t.Fatal(err)
	}
	_, e = h.Process()
	assert.NoError(t, e)
}

func TestSendTaskResult_Process(t *testing.T) {
	h := NewHandler(msgservice.SendTaskResultRequestMessage)
	if err := h.SetMessage(&msgservice.SendTaskResultRequest{}); err != nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}
	s.SetEngine(&MockMEngine{})
	h.SetStore(s)
	req := &msgservice.SendTaskResultRequest{}
	h.SetMessage(req)
	_, err := h.Process()
	assert.Error(t, err)

	req.Info = &immutable.CompactTask{}
	_, err = h.Process()
	assert.Error(t, err)

	req.Info = &immutable.CompactedFileInfo{}
	_, err = h.Process()
	assert.NoError(t, err)

	if err := h.SetMessage(&msgservice.SendTaskResultRequest{Type: -1}); err != nil {
		t.Fatal(err)
	}
	_, err = h.Process()
	assert.Error(t, err)
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

	r, e := h.Process()
	if e != nil {
		t.Fatalf("execute h.Process failed: %v", e)
	}
	r1, ok := r.(*msgservice.DropSeriesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	require.ErrorContains(t, r1.Error(), "found EOF, expected identifier, string, number, bool at line 1, char 7")
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

func buildSmartQueryRequest(fields []smart_query.Field, cond smart_query.OrCond) *msgservice.SmarterQueryRequest {
	db := path.Join(dataPath, "db0")
	rp := "autogen"
	mst := "mst0"
	startTime := int64(0)
	endTime := int64(1e10)
	interval := int64(1e9)
	limit := int64(0)
	req := &msgservice.SmarterQueryRequest{}
	req.Db = &db
	req.RP = &rp
	req.PtIDs = []uint32{1}
	req.Mst = proto.String(mst)
	if len(fields) > 0 {
		req.Fields = make([]*internal.Field, 0, len(fields))
		for i := range fields {
			req.Fields = append(req.Fields,
				&internal.Field{
					Call: proto.String(fields[i].Call),
					Val:  proto.String(fields[i].Val),
					Typ:  proto.Uint32(uint32(fields[i].Typ)),
				})
		}
	}
	if len(cond.Cond) > 0 {
		req.Condition = make([]*internal.EqCond, 0, len(cond.Cond))
		for i := range cond.Cond {
			req.Condition = append(req.Condition,
				&internal.EqCond{
					Key: proto.String(cond.Cond[i].Key),
					Val: proto.String(cond.Cond[i].Val),
					Op:  proto.Int64(int64(cond.Cond[i].Op)),
					Typ: proto.Uint32(uint32(cond.Cond[i].Typ)),
				},
			)
		}
	}
	req.StartTime = proto.Int64(startTime)
	req.EndTime = proto.Int64(endTime)
	req.Interval = proto.Int64(interval)
	req.Limit = proto.Int32(int32(limit))
	return req
}

func TestSmarterQuery_Process(t *testing.T) {
	t.Run("smart query no call", func(t *testing.T) {
		h := NewHandler(msgservice.SmarterQueryRequestMessage)
		fields := []smart_query.Field{
			{Val: "cpu", Typ: influxql.Float},
		}
		cond := smart_query.OrCond{
			Cond: []smart_query.EqCond{{Key: "hostname", Val: "hostname_1", Typ: influxql.Tag, Op: influxql.EQ}},
		}
		req := buildSmartQueryRequest(fields, cond)
		if err := h.SetMessage(req); err != nil {
			t.Fatal(err)
		}
		st := storage.Storage{}
		eg := &MockEngine{}
		schema := record.Schemas{{Type: influx.Field_Type_Float, Name: "cpu"}, {Type: influx.Field_Type_Int, Name: record.TimeField}}
		eg.SetIterator(1, schema)
		st.SetEngine(eg)
		h.SetStore(&st)
		rsp, err := h.Process()
		if err != nil {
			t.Fatal(err)
		}
		response, ok := rsp.(*msgservice.SmarterQueryResponse)
		if !ok {
			t.Fatal("response type is invalid")
		}
		assert.NoError(t, response.Error())
		require.NoError(t, response.UnmarshalBinary(response.Data))
		res := response.GetResult()
		require.Equal(t, len(res.GetData()), 1)
		require.True(t, res.IsSingleSeries())
	})

	t.Run("smart query merge", func(t *testing.T) {
		h := NewHandler(msgservice.SmarterQueryRequestMessage)
		fields := []smart_query.Field{
			{Call: "max", Val: "cpu", Typ: influxql.Float},
		}
		cond := smart_query.OrCond{
			Cond: []smart_query.EqCond{{Key: "hostname", Val: "hostname_1", Typ: influxql.Tag, Op: influxql.EQ}},
		}
		req := buildSmartQueryRequest(fields, cond)
		if err := h.SetMessage(req); err != nil {
			t.Fatal(err)
		}
		st := storage.Storage{}
		eg := &MockEngine{}
		schema := record.Schemas{{Type: influx.Field_Type_Float, Name: "cpu"}, {Type: influx.Field_Type_Int, Name: record.TimeField}}
		eg.SetIterator(1, schema)
		st.SetEngine(eg)
		h.SetStore(&st)
		rsp, err := h.Process()
		if err != nil {
			t.Fatal(err)
		}
		response, ok := rsp.(*msgservice.SmarterQueryResponse)
		if !ok {
			t.Fatal("response type is invalid")
		}
		assert.NoError(t, response.Error())
		require.NoError(t, response.UnmarshalBinary(response.Data))
		res := response.GetResult()
		require.Equal(t, len(res.GetData()), 1)
		require.True(t, res.IsSingleSeries())
	})

	t.Run("smart query agg merge", func(t *testing.T) {
		h := NewHandler(msgservice.SmarterQueryRequestMessage)
		fields := []smart_query.Field{
			{Call: "max", Val: "cpu", Typ: influxql.Float},
		}
		cond := smart_query.OrCond{
			Cond: []smart_query.EqCond{{Key: "hostname", Val: "hostname_1", Typ: influxql.Tag, Op: influxql.EQ}},
		}
		req := buildSmartQueryRequest(fields, cond)
		if err := h.SetMessage(req); err != nil {
			t.Fatal(err)
		}
		st := storage.Storage{}
		eg := &MockEngine{}
		schema := record.Schemas{{Type: influx.Field_Type_Float, Name: "cpu"}, {Type: influx.Field_Type_Int, Name: record.TimeField}}
		eg.SetIterator(2, schema)
		st.SetEngine(eg)
		h.SetStore(&st)
		rsp, err := h.Process()
		if err != nil {
			t.Fatal(err)
		}
		response, ok := rsp.(*msgservice.SmarterQueryResponse)
		if !ok {
			t.Fatal("response type is invalid")
		}
		assert.NoError(t, response.Error())
		require.NoError(t, response.UnmarshalBinary(response.Data))
		res := response.GetResult()
		require.Equal(t, len(res.GetData()), 1)
		require.False(t, res.IsSingleSeries())
	})

	t.Run("smart query colstore count(time)", func(t *testing.T) {
		h := NewHandler(msgservice.SmarterQueryRequestMessage)
		fields := []smart_query.Field{
			{Call: "count", Val: "time", Typ: influxql.Integer},
		}
		interval := int64(0)
		cond := smart_query.OrCond{}
		req := buildSmartQueryRequest(fields, cond)
		req.Interval = &interval
		if err := h.SetMessage(req); err != nil {
			t.Fatal(err)
		}
		st := storage.Storage{}
		eg := &MockEngine{isColStore: true}
		schema := record.Schemas{{Type: influx.Field_Type_Int, Name: record.TimeField}, {Type: influx.Field_Type_Int, Name: record.TimeField}}
		eg.SetIterator(2, schema)
		st.SetEngine(eg)
		h.SetStore(&st)
		rsp, err := h.Process()
		if err != nil {
			t.Fatal(err)
		}
		response, ok := rsp.(*msgservice.SmarterQueryResponse)
		if !ok {
			t.Fatal("response type is invalid")
		}
		assert.NoError(t, response.Error())
		require.NoError(t, response.UnmarshalBinary(response.Data))
		res := response.GetResult()
		require.Equal(t, len(res.GetData()), 1)
		require.True(t, res.IsSingleSeries())
	})

	t.Run("smart query colstore limit", func(t *testing.T) {
		h := NewHandler(msgservice.SmarterQueryRequestMessage)
		fields := []smart_query.Field{
			{Val: "cpu", Typ: influxql.Float},
		}
		interval := int64(0)
		cond := smart_query.OrCond{}
		req := buildSmartQueryRequest(fields, cond)
		req.Interval = &interval
		tags := influx.PointTags{{Key: "hostname", Value: "hostname_1"}}
		r := influx.Row{Name: "cpu_0000", Tags: tags}
		r.UnmarshalIndexKeys(nil)
		req.HintType = proto.Int64(int64(hybridqp.FullSeriesQuery))
		req.HintSeriesKeys = r.IndexKey
		req.Limit = proto.Int32(1)
		if err := h.SetMessage(req); err != nil {
			t.Fatal(err)
		}
		st := storage.Storage{}
		eg := &MockEngine{isColStore: true}
		schema := record.Schemas{{Type: influx.Field_Type_Int, Name: record.TimeField}, {Type: influx.Field_Type_Int, Name: record.TimeField}}
		eg.SetIterator(1, schema)
		st.SetEngine(eg)
		h.SetStore(&st)
		rsp, err := h.Process()
		if err != nil {
			t.Fatal(err)
		}
		response, ok := rsp.(*msgservice.SmarterQueryResponse)
		if !ok {
			t.Fatal("response type is invalid")
		}
		assert.NoError(t, response.Error())
		require.NoError(t, response.UnmarshalBinary(response.Data))
		res := response.GetResult()
		require.Equal(t, len(res.GetData()), 1)
		require.True(t, res.IsSingleSeries())
	})
}

func TestSmarterQuery_SetMessage(t *testing.T) {
	q := &SmarterQuery{}
	assert.Error(t, q.SetMessage(&msgservice.SmarterQueryResponse{}), "msgservice.SmarterQueryRequest")
}

func TestPullRPPTWriteStatus_Process(t *testing.T) {
	db := "db0"
	rp := "rp0"
	t.Run("success case", func(t *testing.T) {
		h := NewHandler(msgservice.PullRPPTWriteStatusRequestMessage)
		err := h.SetMessage(&msgservice.ShowTagKeysRequest{})
		assert.Error(t, err)
		if err := h.SetMessage(&msgservice.PullRPPTWriteStatusRequest{
			PullRPPTWriteStatusRequest: internal.PullRPPTWriteStatusRequest{
				Database:        proto.String(db),
				RetentionPolicy: proto.String(rp),
			},
		}); err != nil {
			t.Fatal(err)
		}

		s := &storage.Storage{}
		s.SetEngine(&MockMEngine{})
		h.SetStore(s)

		rsp, err := h.Process()
		if err != nil {
			t.Fatalf("execute h.Process failed: %v", err)
		}

		response, ok := rsp.(*msgservice.PullRPPTWriteStatusResponse)
		if !ok {
			t.Fatal("response type is invalid")
		}

		assert.NotNil(t, response.StatusInfo)
		assert.Equal(t, 1, len(response.StatusInfo))
		assert.Equal(t, int64(5), response.StatusInfo[0]["mst0"])
	})
	t.Run("fail case", func(t *testing.T) {
		h := NewHandler(msgservice.PullRPPTWriteStatusRequestMessage)
		err := h.SetMessage(&msgservice.ShowTagKeysRequest{})
		assert.Error(t, err)
		if err := h.SetMessage(&msgservice.PullRPPTWriteStatusRequest{
			PullRPPTWriteStatusRequest: internal.PullRPPTWriteStatusRequest{
				Database:        proto.String(db),
				RetentionPolicy: proto.String("rp1"),
			},
		}); err != nil {
			t.Fatal(err)
		}
		s := &storage.Storage{}
		s.SetEngine(&MockMEngine{})
		h.SetStore(s)
		rsp, err := h.Process()
		if err != nil {
			t.Fatalf("execute h.Process failed: %v", err)
		}
		response, ok := rsp.(*msgservice.PullRPPTWriteStatusResponse)
		if !ok {
			t.Fatal("response type is invalid")
		}
		assert.ErrorContains(t, response.Error, "error")
	})
}
