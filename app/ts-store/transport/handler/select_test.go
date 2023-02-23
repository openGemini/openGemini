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
	"testing"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/rpc"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
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

func (s *MockStoreEngine) CreateLogicPlan(context.Context, string, uint32, uint64, influxql.Sources, *executor.QuerySchema) (hybridqp.QueryNode, error) {
	readers := make([][]interface{}, s.readerCount)
	shard := executor.NewLogicalDummyShard(readers)
	return shard, nil
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

func (s *MockStoreEngine) SeriesCardinality(db string, ptIDs []uint32, measurements []string, condition influxql.Expr) ([]meta.MeasurementCardinalityInfo, error) {
	return nil, nil
}

func (s *MockStoreEngine) SeriesExactCardinality(db string, ptIDs []uint32, measurements []string, condition influxql.Expr) (map[string]uint64, error) {
	return nil, nil
}

func (s *MockStoreEngine) SeriesKeys(db string, ptIDs []uint32, measurements []string, condition influxql.Expr) ([]string, error) {
	return nil, nil
}

func (s *MockStoreEngine) TagValues(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr) (netstorage.TablesTagSets, error) {
	return nil, nil
}

func (s *MockStoreEngine) TagValuesCardinality(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr) (map[string]uint64, error) {
	return nil, nil
}

func (s *MockStoreEngine) SendSysCtrlOnNode(req *netstorage.SysCtrlRequest) error {
	return nil
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

type DummySeriesTransform struct {
	executor.BaseProcessor
}

func NewDummySeriesTransform() *DummySeriesTransform {
	series := &DummySeriesTransform{}

	return series
}

type DummySeriesTransformCreator struct {
}

func (creator *DummySeriesTransformCreator) Create(plan executor.LogicalPlan, opt qry.ProcessorOptions) (executor.Processor, error) {
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

/*func TestCreateSerfInstance(t *testing.T) {
	const shardCount = 10
	const readCount = 10
	hookLogicPlan()

	schema := executor.NewQuerySchema(nil, nil, &qry.ProcessorOptions{})
	plan := executor.NewLogicalSeries(schema)
	node, err := executor.MarshalQueryNode(plan)
	if err != nil {
		t.Error(err)
	}

	shardIDs := make([]uint64, shardCount)
	for i := 0; i < shardCount; i++ {
		shardIDs[i] = uint64(i)
	}

	req := &executor.RemoteQuery{
		ShardIDs: shardIDs,
		Node:     node,
	}

	h := NewSelect(NewMockStoreEngine(readCount), nil, req)

	if err := h.Process(); err != nil {
		t.Error(err)
	}
}*/

type EmptyResponser struct {
	transport.Responser
	session *spdy.MultiplexedSession
}

func (r *EmptyResponser) Session() *spdy.MultiplexedSession {
	return r.session
}

func (r *EmptyResponser) Response(response interface{}, full bool) error {

	return nil
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

	msg := rpc.NewMessage(executor.QueryMessage, &executor.RemoteQuery{Node: []byte{10}})
	msg.SetClientID(100)

	p := NewSelectProcessor(nil)
	require.NoError(t, p.Handle(resp, msg))
}

var storageDataPath = "/tmp/data/"
var metaPath = "/tmp/meta"

func mockStorage() *storage.Storage {
	node := metaclient.NewNode(metaPath)
	storeConfig := config.NewStore()
	monitorConfig := config.Monitor{
		Pushers: "http",
	}
	config.SetHaEnable(true)
	config := &config.TSStore{
		Data:    storeConfig,
		Monitor: monitorConfig,
		Common:  config.NewCommon(),
	}

	storage, err := storage.OpenStorage(storageDataPath, node, nil, config)
	if err != nil {
		return nil
	}
	return storage
}

func TestNewShardTraits(t *testing.T) {
	resp := &EmptyResponser{}
	resp.session = spdy.NewMultiplexedSession(spdy.DefaultConfiguration(), nil, 0)
	msg := rpc.NewMessage(executor.QueryMessage, &executor.RemoteQuery{Database: "db0", PtID: 0, Node: []byte{10}, ShardIDs: []uint64{1, 2, 3}})
	req, _ := msg.Data().(*executor.RemoteQuery)
	store := mockStorage()
	p := NewSelectProcessor(store)
	s := NewSelect(p.store, resp, req)
	config.SetHaEnable(false)
	_, _, err := s.NewShardTraits(s.req, s.w)
	if err != nil {
		t.Fatal(err)
	}
	config.SetHaEnable(true)
	_, _, err = s.NewShardTraits(s.req, s.w)
	if err == nil {
		t.Fatal(err)
	}
	s.store.GetEngine().CreateDBPT("db0", 0)
	_, _, err = s.NewShardTraits(s.req, s.w)
	if err != nil {
		t.Fatal(err)
	}
}
