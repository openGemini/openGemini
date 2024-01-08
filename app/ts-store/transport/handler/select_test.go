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

package handler

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/rpc"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	qry "github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/require"
)

type MockStoreEngine struct {
	readerCount int
}

func NewMockStoreEngine(readerCount int) *MockStoreEngine {
	return &MockStoreEngine{
		readerCount: readerCount,
	}
}

func (s *MockStoreEngine) RefEngineDbPt(string, uint32) error {
	return nil
}

func (s *MockStoreEngine) GetShardDownSampleLevel(db string, ptId uint32, shardID uint64) int {
	return 0
}

func (s *MockStoreEngine) UnrefEngineDbPt(string, uint32) {

}

func (s *MockStoreEngine) ExecuteDelete(*netstorage.DeleteRequest) error {
	return nil
}

func (s *MockStoreEngine) GetShardSplitPoints(db string, pt uint32, shardID uint64, idxes []int64) ([]string, error) {
	return nil, nil
}

func (s *MockStoreEngine) SeriesCardinality(db string, ptIDs []uint32, measurements []string, condition influxql.Expr, tr influxql.TimeRange) ([]meta.MeasurementCardinalityInfo, error) {
	return nil, nil
}

func (s *MockStoreEngine) SeriesExactCardinality(db string, ptIDs []uint32, measurements []string, condition influxql.Expr, tr influxql.TimeRange) (map[string]uint64, error) {
	return nil, nil
}

func (s *MockStoreEngine) SeriesKeys(db string, ptIDs []uint32, measurements []string, condition influxql.Expr, tr influxql.TimeRange) ([]string, error) {
	return nil, nil
}

func (s *MockStoreEngine) TagKeys(db string, ptIDs []uint32, measurements []string, condition influxql.Expr, tr influxql.TimeRange) ([]string, error) {
	return nil, nil
}

func (s *MockStoreEngine) TagValues(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr, tr influxql.TimeRange) (netstorage.TablesTagSets, error) {
	return nil, nil
}

func (s *MockStoreEngine) TagValuesCardinality(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr, tr influxql.TimeRange) (map[string]uint64, error) {
	return nil, nil
}

func (s *MockStoreEngine) SendSysCtrlOnNode(req *netstorage.SysCtrlRequest) (map[string]string, error) {
	return nil, nil
}

func (s *MockStoreEngine) PreOffload(*meta.DbPtInfo) error {
	return nil
}

func (s *MockStoreEngine) RollbackPreOffload(*meta.DbPtInfo) error {
	return nil
}

func (s *MockStoreEngine) PreAssign(uint64, *meta.DbPtInfo) error {
	return nil
}

func (s *MockStoreEngine) Offload(*meta.DbPtInfo) error {
	return nil
}

func (s *MockStoreEngine) Assign(uint64, *meta.DbPtInfo) error {
	return nil
}

func (s *MockStoreEngine) GetConnId() uint64 {
	return 0
}

func (s *MockStoreEngine) RowCount(_ string, _ uint32, _ []uint64, _ hybridqp.Catalog) (int64, error) {
	return 0, nil
}

func (s *MockStoreEngine) CheckPtsRemovedDone() error {
	return nil
}

type DummySeriesTransform struct {
	executor.BaseProcessor
}

func NewDummySeriesTransform() *DummySeriesTransform {
	series := &DummySeriesTransform{}

	return series
}

type DummySeriesTransformCreator struct {
}

func (creator *DummySeriesTransformCreator) Create(plan executor.LogicalPlan, opt *qry.ProcessorOptions) (executor.Processor, error) {
	return NewDummySeriesTransform(), nil
}

func (dummy *DummySeriesTransform) Work(ctx context.Context) error {
	return nil
}

func (dummy *DummySeriesTransform) Close() {

}

func (dummy *DummySeriesTransform) Release() error {
	return nil
}

func (dummy *DummySeriesTransform) Name() string {
	return "DummySeriesTransform"
}

func (dummy *DummySeriesTransform) GetOutputs() executor.Ports {
	return nil
}

func (dummy *DummySeriesTransform) GetInputs() executor.Ports {
	return nil
}

func (dummy *DummySeriesTransform) GetOutputNumber(port executor.Port) int {
	return 0
}

func (dummy *DummySeriesTransform) GetInputNumber(port executor.Port) int {
	return executor.INVALID_NUMBER
}

func (dummy *DummySeriesTransform) IsSink() bool {
	return false
}

func (dummy *DummySeriesTransform) Explain() []executor.ValuePair {
	return nil
}

func hookLogicPlan() {
	executor.RegistryTransformCreator(&executor.LogicalSeries{}, &DummySeriesTransformCreator{})
}

func TestCreateSerfInstance(t *testing.T) {
	const shardCount = 10
	hookLogicPlan()

	schema := executor.NewQuerySchema(nil, nil, &qry.ProcessorOptions{}, nil)
	plan := executor.NewLogicalSeries(schema)
	node, err := executor.MarshalQueryNode(plan)
	require.NoError(t, err)

	shardIDs := make([]uint64, shardCount)
	for i := 0; i < shardCount; i++ {
		shardIDs[i] = uint64(i)
	}

	req := &executor.RemoteQuery{
		Database: "db0",
		PtID:     2,
		NodeID:   1,
		ShardIDs: shardIDs,
		Node:     node,
	}

	store := mockStorage(t.TempDir())
	resp := &EmptyResponser{}
	resp.session = spdy.NewMultiplexedSession(spdy.DefaultConfiguration(), nil, 0)
	h := NewSelect(store, resp, req)

	require.EqualError(t, h.Process(), "pt not found")

	req.PtID = 1
	req.Analyze = true
	resp.err = errors.New("some error")
	h = NewSelect(store, resp, req)
	require.NoError(t, h.Process())

	resp.err = nil
	h = NewSelect(store, resp, req)
	h.SetAbortHook(func() {})
	h.Abort()
	require.NoError(t, h.Process())

	h = NewSelect(store, resp, req)
	require.NotEmpty(t, h.execute(context.Background(), nil))
	h.Abort()
	require.NoError(t, h.execute(context.Background(), &executor.PipelineExecutor{}))

	req.Node = []byte{1}
	h = NewSelect(store, resp, req)
	require.EqualError(t, h.Process(), errno.NewError(errno.ShortBufferSize, util.Uint64SizeBytes, 1).Error())
}

type EmptyResponser struct {
	transport.Responser
	session *spdy.MultiplexedSession

	err error
}

func (r *EmptyResponser) Session() *spdy.MultiplexedSession {
	return r.session
}

func (r *EmptyResponser) Response(response interface{}, full bool) error {

	return r.err
}

func (r *EmptyResponser) Callback(data interface{}) error {

	return nil
}

func (r *EmptyResponser) Encode(dst []byte, data interface{}) ([]byte, error) {
	return nil, nil
}

func (r *EmptyResponser) Decode(data []byte) (interface{}, error) {
	return nil, nil
}

func (r *EmptyResponser) Sequence() uint64 {
	return 200
}

func TestSelectProcessor(t *testing.T) {
	resp := &EmptyResponser{}
	resp.session = spdy.NewMultiplexedSession(spdy.DefaultConfiguration(), nil, 0)

	msg1 := rpc.NewMessage(executor.QueryMessage, &executor.RemoteQuery{ShardIDs: []uint64{1, 2, 3}})
	msg1.SetClientID(100)

	msg2 := rpc.NewMessage(executor.QueryMessage, &executor.RemoteQuery{ShardIDs: []uint64{1, 2}})
	msg2.SetClientID(100)

	msg3 := rpc.NewMessage(executor.QueryMessage, &executor.RemoteQuery{ShardIDs: []uint64{1}, Analyze: true})
	msg3.SetClientID(100)

	msg4 := rpc.NewMessage(executor.QueryMessage, &executor.RemoteQuery{Opt: qry.ProcessorOptions{IncQuery: true, LogQueryCurrId: "1", IterID: 0}})
	msg4.SetClientID(100)

	msg5 := rpc.NewMessage(executor.FinishMessage, &executor.IncQueryFinish{})
	msg5.SetClientID(100)

	e := resourceallocator.InitResAllocator(2, 0, 2, 0, resourceallocator.ShardsParallelismRes, time.Second, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0)
	}()
	p := NewSelectProcessor(nil)
	require.NoError(t, p.Handle(resp, msg1))
	require.NoError(t, p.Handle(resp, msg2))
	require.NoError(t, p.Handle(resp, msg3))

	query.NewManager(100).Abort(resp.Sequence())
	require.NoError(t, p.Handle(resp, msg3))

	cache.PutNodeIterNum("2", 0)
	require.NoError(t, p.Handle(resp, msg4))

	cache.PutNodeIterNum("1", 0)
	require.NoError(t, p.Handle(resp, msg4))
	err := p.Handle(resp, msg5)
	require.Equal(t, strings.Contains(err.Error(), "executor.RemoteQuery"), true)
}

func mockStorage(dir string) *storage.Storage {
	node := metaclient.NewNode(dir + "/meta")
	storeConfig := config.NewStore()
	monitorConfig := config.Monitor{
		Pushers:      "http",
		StoreEnabled: true,
	}
	config.SetHaPolicy(config.SSPolicy)
	config := &config.TSStore{
		Data:    storeConfig,
		Monitor: monitorConfig,
		Common:  config.NewCommon(),
		Meta:    config.NewMeta(),
	}

	storage, err := storage.OpenStorage(dir+"/data", node, nil, config)
	if err != nil {
		return nil
	}
	storage.GetEngine().CreateDBPT("db0", 1, false)
	return storage
}

func TestNewShardTraits(t *testing.T) {
	resp := &EmptyResponser{}
	resp.session = spdy.NewMultiplexedSession(spdy.DefaultConfiguration(), nil, 0)
	msg := rpc.NewMessage(executor.QueryMessage, &executor.RemoteQuery{Database: "db0", PtID: 0, Node: []byte{10}, ShardIDs: []uint64{1, 2, 3}})
	req, _ := msg.Data().(*executor.RemoteQuery)
	store := mockStorage(t.TempDir())
	p := NewSelectProcessor(store)
	s := NewSelect(p.store, resp, req)
	traits := s.NewShardTraits(s.req, s.w)
	require.NotEmpty(t, traits)
}

func TestSelect_GetQueryExeInfo(t *testing.T) {
	rq := executor.RemoteQuery{
		Opt: qry.ProcessorOptions{
			QueryId: 1,
			Query:   "SELECT * FROM mst1",
		},
		Database: "db1",
	}
	s := NewSelect(nil, nil, &rq)
	info := s.GetQueryExeInfo()
	require.Equal(t, rq.Opt.QueryId, info.QueryID)
	require.Equal(t, rq.Opt.Query, info.Stmt)
	require.Equal(t, rq.Database, info.Database)
}

func TestSelectForCsstore(t *testing.T) {
	resp := &EmptyResponser{}
	resp.session = spdy.NewMultiplexedSession(spdy.DefaultConfiguration(), nil, 0)
	ptQuerys := make([]executor.PtQuery, 0)
	ptQuerys = append(ptQuerys, executor.PtQuery{PtID: 1, ShardInfos: []executor.ShardInfo{{ID: 1, Path: "/obs/db0/log1/seg0", Version: 4}}})
	ptQuerys = append(ptQuerys, executor.PtQuery{PtID: 2, ShardInfos: []executor.ShardInfo{{ID: 2, Path: "/obs/db0/log1/seg1", Version: 4}}})
	q := &executor.RemoteQuery{
		Database: "db0",
		PtID:     0,
		Opt: qry.ProcessorOptions{
			QueryId: 1,
			Query:   "SELECT * FROM mst1",
			Sources: influxql.Sources{
				&influxql.Measurement{
					ObsOptions: &obs.ObsOptions{},
				},
			},
		},
		Node:     []byte{1, 2, 3, 4, 5, 6, 7, 8},
		PtQuerys: ptQuerys}

	msg := rpc.NewMessage(executor.QueryMessage, q)
	req, _ := msg.Data().(*executor.RemoteQuery)
	store := mockStorage(t.TempDir())
	p := NewSelectProcessor(store)
	s := NewSelect(p.store, resp, req)

	s.aborted = false
	err := s.process(s.w, nil, s.req)
	if err == nil {
		t.Fatal()
	}
}
