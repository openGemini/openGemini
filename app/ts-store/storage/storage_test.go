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

package storage

import (
	"sync"
	"testing"
	"time"

	retention2 "github.com/influxdata/influxdb/services/retention"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendRetentionMstPolicyService(t *testing.T) {
	s := Storage{}
	c := retention2.NewConfig()
	s.appendRetentionMstPolicyService(c)
	if len(s.Services) != 2 {
		t.Errorf("services append fail")
	}
}

func TestAppendDropSeriesService(t *testing.T) {
	s := Storage{}
	c := retention2.NewConfig()
	s.appendDropSeriesService(c)
	if len(s.Services) != 1 {
		t.Errorf("services append fail")
	}

	c2 := retention2.NewConfig()
	c2.Enabled = false
	s.appendDropSeriesService(c2)
	if len(s.Services) != 1 {
		t.Errorf("backend service disabled fail")
	}
}

func TestAppendDownSamplePolicyService(t *testing.T) {
	s := Storage{}
	c := retention2.NewConfig()
	c.Enabled = false
	s.appendDownSamplePolicyService(c)
	c.Enabled = true
	s.appendDownSamplePolicyService(c)
	if len(s.Services) <= 0 {
		t.Errorf("services append fail")
	}
}

func TestReportLoad(t *testing.T) {
	st := &Storage{
		log:  logger.NewLogger(errno.ModuleStorageEngine),
		stop: make(chan struct{}),
		loadCtx: &metaclient.LoadCtx{
			LoadCh: make(chan *metaclient.DBPTCtx),
		},
		metaClient: metaclient.NewClient(t.TempDir(), false, 100),
	}
	require.NoError(t, st.metaClient.Close())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		st.ReportLoad()
		wg.Done()
	}()

	var ctx = make([]*metaclient.DBPTCtx, 5)
	for i := 0; i < 5; i++ {
		ctx[i] = &metaclient.DBPTCtx{
			DBPTStat: &proto2.DBPtStatus{
				DB:   proto.String("db0"),
				PtID: proto.Uint32(100),
				RpStats: []*proto2.RpShardStatus{{
					RpName: proto.String("default"),
					ShardStats: &proto2.ShardStatus{
						ShardID:     proto.Uint64(101),
						ShardSize:   proto.Uint64(102),
						SeriesCount: proto.Int32(103),
						MaxTime:     proto.Int64(104),
					},
				}},
			},
		}
	}

	for i := 0; i < 5; i++ {
		st.loadCtx.LoadCh <- ctx[i]
	}

	time.Sleep(time.Second / 10)
	close(st.stop)
	wg.Wait()
}

type MockEngine struct {
	engine.Engine
}

func (e *MockEngine) WriteRec(_, _ string, _ uint32, _ uint64, _ *record.Record, _ []byte) error {
	return nil
}

func (e *MockEngine) ClearRepCold(req *msgservice.SendClearEventsRequest) error {
	return nil
}

func TestWriteRec(t *testing.T) {
	s := &Storage{engine: &MockEngine{}}
	err := s.WriteRec("db0", "rp0", "mst0", 0, 0, nil, nil)
	assert.Equal(t, err, nil)
}

type MockMetaClient struct {
	GetShardRangeInfoFn       func(db string, rp string, shardID uint64) (*meta.ShardTimeRangeInfo, error)
	GetMeasurementInfoStoreFn func(dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error)

	replicaInfo *message.ReplicaInfo
}

func (mc *MockMetaClient) GetShardRangeInfo(db string, rp string, shardID uint64) (*meta.ShardTimeRangeInfo, error) {
	return mc.GetShardRangeInfoFn(db, rp, shardID)
}

func (mc *MockMetaClient) RetryMeasurement(dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
	return mc.GetMeasurementInfoStoreFn(dbName, rpName, mstName)
}

func (mc *MockMetaClient) GetReplicaInfo(_ string, _ uint32) *message.ReplicaInfo {
	return mc.replicaInfo
}

func (mc *MockMetaClient) ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo) {
	return "db", "rp", &meta.ShardGroupInfo{}
}

func TestStorage_Write(t *testing.T) {
	dir := t.TempDir()
	st := &Storage{
		log:  logger.NewLogger(errno.ModuleStorageEngine),
		stop: make(chan struct{}),
	}
	defer st.MustClose()

	st.MetaClient = &MockMetaClient{
		GetShardRangeInfoFn: func(db string, rp string, shardID uint64) (*meta.ShardTimeRangeInfo, error) {
			return &meta.ShardTimeRangeInfo{
				ShardDuration: &meta.ShardDurationInfo{
					Ident:        meta.ShardIdentifier{ShardID: 1, Policy: "autogen", OwnerDb: "db0", OwnerPt: 1},
					DurationInfo: meta.DurationDescriptor{Duration: time.Second}},
				OwnerIndex: meta.IndexDescriptor{IndexID: 1, TimeRange: meta.TimeRangeInfo{StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Hour)}},
				TimeRange:  meta.TimeRangeInfo{StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Hour)},
			}, nil
		},
		GetMeasurementInfoStoreFn: func(dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
			return &meta.MeasurementInfo{Name: "mst"}, nil
		},
	}

	newEngineFn := engine.GetNewEngineFunction(config.DefaultEngine)
	loadCtx := metaclient.LoadCtx{}
	loadCtx.LoadCh = make(chan *metaclient.DBPTCtx, 100)
	eng, err := newEngineFn(dir, dir, engineOption, &loadCtx)
	if err != nil {
		t.Fatal(err)
	}
	data := &meta.Data{}
	client := metaclient.NewClient("", false, 0)
	client.SetCacheData(data)
	eng.SetMetaClient(client)
	st.engine = eng
	db := "db0"
	rp := "autogen"
	mst := "mst"
	var ptId uint32 = 1
	var shardID uint64 = 1
	st.engine.CreateDBPT(db, ptId, false)
	err = st.Write(db, rp, mst, ptId, shardID, func() error {
		return st.engine.WriteRows(db, rp, ptId, shardID, nil, nil, nil)
	})
	assert.Equal(t, nil, err)

	// test get measurement info failed
	st.MetaClient = &MockMetaClient{
		GetShardRangeInfoFn: func(db string, rp string, shardID uint64) (*meta.ShardTimeRangeInfo, error) {
			return &meta.ShardTimeRangeInfo{
				ShardDuration: &meta.ShardDurationInfo{
					Ident:        meta.ShardIdentifier{ShardID: 2, Policy: "autogen", OwnerDb: "db0", OwnerPt: 1},
					DurationInfo: meta.DurationDescriptor{Duration: time.Second}},
				OwnerIndex: meta.IndexDescriptor{IndexID: 1, TimeRange: meta.TimeRangeInfo{StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Hour)}},
				TimeRange:  meta.TimeRangeInfo{StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Hour)},
			}, nil
		},
		GetMeasurementInfoStoreFn: func(dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
			return &meta.MeasurementInfo{Name: "mst"}, errno.NewError(errno.NoNodeAvailable)
		},
	}

	err = st.Write(db, rp, mst, ptId, 2, func() error {
		return st.engine.WriteRows(db, rp, ptId, 2, nil, nil, nil)
	})
	assert.Equal(t, true, errno.Equal(err, errno.NoNodeAvailable))
}

type MockSlaveStorage struct {
	err error
}

func (s *MockSlaveStorage) WriteRows(_ *netstorage.WriteContext, _ uint64, _ uint32, _, _ string, _ time.Duration) error {
	return s.err
}

func mockRows(num int) influx.Rows {
	rows := make(influx.Rows, num)
	t := time.Now().UnixNano()
	tags := influx.PointTags{
		{Key: "tag-key", Value: "tag-value"},
	}
	fields := influx.Fields{
		{Key: "field-key", NumValue: 1, Type: influx.Field_Type_Int},
	}
	for i := 0; i < num; i++ {
		rows[i].Name = "mock"
		rows[i].Timestamp = t
		rows[i].Tags = tags
		rows[i].Fields = fields
		t++
	}
	return rows
}

var engineOption engine.EngineOptions

func init() {
	engineOption = engine.NewEngineOptions()
	engineOption.ShardMutableSizeLimit = 30 * 1024 * 1024
	engineOption.NodeMutableSizeLimit = 1e9
	engineOption.MaxWriteHangTime = time.Second
}

func TestStorage_WriteToSlave(t *testing.T) {
	dir := t.TempDir()
	st := &Storage{
		log:          logger.NewLogger(errno.ModuleStorageEngine),
		stop:         make(chan struct{}),
		slaveStorage: &MockSlaveStorage{},
	}
	defer st.MustClose()

	// no replication
	mockClient := &MockMetaClient{
		GetShardRangeInfoFn: func(db string, rp string, shardID uint64) (*meta.ShardTimeRangeInfo, error) {
			return &meta.ShardTimeRangeInfo{
				ShardDuration: &meta.ShardDurationInfo{
					Ident:        meta.ShardIdentifier{ShardID: 1, Policy: "autogen", OwnerDb: "db0", OwnerPt: 1},
					DurationInfo: meta.DurationDescriptor{Duration: time.Second}},
				OwnerIndex: meta.IndexDescriptor{IndexID: 1, TimeRange: meta.TimeRangeInfo{StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Hour)}},
				TimeRange:  meta.TimeRangeInfo{StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Hour)},
			}, nil
		},
		GetMeasurementInfoStoreFn: func(dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
			return &meta.MeasurementInfo{Name: "mst"}, nil
		},
	}
	mockData := meta.Data{
		Databases: map[string]*meta.DatabaseInfo{
			"db0": {
				ReplicaN: 2,
			},
		},
	}
	st.MetaClient = mockClient
	st.metaClient = &metaclient.Client{}
	st.metaClient.SetCacheData(&mockData)
	newEngineFn := engine.GetNewEngineFunction(config.DefaultEngine)

	loadCtx := metaclient.LoadCtx{}
	loadCtx.LoadCh = make(chan *metaclient.DBPTCtx, 100)
	eng, err := newEngineFn(dir, dir, engineOption, &loadCtx)
	if err != nil {
		t.Fatal(err)
	}
	eng.SetMetaClient(st.metaClient)
	st.engine = eng
	db := "db0"
	rp := "autogen"
	var ptId uint32 = 1
	var shardID uint64 = 1
	st.engine.CreateDBPT(db, ptId, false)
	_ = config.SetHaPolicy(config.RepPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)

	rows := mockRows(1)
	binaryRows, _ := influx.FastMarshalMultiRows(nil, rows)
	err = st.WriteRows(db, rp, ptId, shardID, rows, binaryRows)
	if !errno.Equal(err, errno.RepConfigWriteNoRepDB) {
		t.Fatal("TestStorage_WriteToSlave err")
	}

	// replication write
	mockClient.replicaInfo = &message.ReplicaInfo{
		ReplicaRole: meta.Master,
		Master:      nil,
		Peers: []*message.PeerInfo{
			{
				PtId:   1,
				NodeId: 1,
				ShardMapper: map[uint64]uint64{
					1: 2,
				},
			},
		},
		ReplicaStatus: meta.Health,
		Term:          0,
	}
	err = st.WriteRows(db, rp, ptId, shardID, rows, binaryRows)
	assert.Equal(t, nil, err)
}

func Test_StorageCheckPtsRemovedDone(t *testing.T) {
	st1 := &Storage{
		engine: &engine.EngineImpl{},
	}
	if st1.CheckPtsRemovedDone() != nil {
		t.Fatal("Test_StorageCheckPtsRemovedDone nil DBPartitions error")
	}
	pts := make(map[uint32]*engine.DBPTInfo, 0)
	pts[0] = &engine.DBPTInfo{}
	dbpts := make(map[string]map[uint32]*engine.DBPTInfo, 0)
	dbpts["db0"] = pts
	st2 := &Storage{
		engine: &engine.EngineImpl{
			DBPartitions: dbpts,
		},
	}
	if st2.CheckPtsRemovedDone() == nil {
		t.Fatal("Test_StorageCheckPtsRemovedDone one DBPartitions error")
	}
}

func TestStorage_RepConfigWrite(t *testing.T) {
	dir := t.TempDir()
	st := &Storage{
		log:          logger.NewLogger(errno.ModuleStorageEngine),
		stop:         make(chan struct{}),
		slaveStorage: &MockSlaveStorage{},
	}
	defer st.MustClose()

	// no replication
	mockClient := &MockMetaClient{
		GetShardRangeInfoFn: func(db string, rp string, shardID uint64) (*meta.ShardTimeRangeInfo, error) {
			return &meta.ShardTimeRangeInfo{
				ShardDuration: &meta.ShardDurationInfo{
					Ident:        meta.ShardIdentifier{ShardID: 1, Policy: "autogen", OwnerDb: "db0", OwnerPt: 1},
					DurationInfo: meta.DurationDescriptor{Duration: time.Second}},
				OwnerIndex: meta.IndexDescriptor{IndexID: 1, TimeRange: meta.TimeRangeInfo{StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Hour)}},
				TimeRange:  meta.TimeRangeInfo{StartTime: time.Now().UTC(), EndTime: time.Now().UTC().Add(time.Hour)},
			}, nil
		},
		GetMeasurementInfoStoreFn: func(dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
			return &meta.MeasurementInfo{Name: "mst"}, nil
		},
	}
	mockData := meta.Data{
		Databases: map[string]*meta.DatabaseInfo{
			"db0": {
				ReplicaN: 1,
			},
			"db1": {
				ReplicaN: 3,
			},
		},
	}
	st.MetaClient = mockClient
	st.metaClient = &metaclient.Client{}
	st.metaClient.SetCacheData(&mockData)
	newEngineFn := engine.GetNewEngineFunction(config.DefaultEngine)

	loadCtx := metaclient.LoadCtx{}
	loadCtx.LoadCh = make(chan *metaclient.DBPTCtx, 100)
	eng, err := newEngineFn(dir, dir, engineOption, &loadCtx)
	if err != nil {
		t.Fatal(err)
	}
	eng.SetMetaClient(st.metaClient)
	st.engine = eng
	db := "db0"
	rp := "autogen"
	var ptId uint32 = 1
	var shardID uint64 = 1
	st.engine.CreateDBPT(db, ptId, false)
	_ = config.SetHaPolicy(config.RepPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)

	rows := mockRows(1)
	binaryRows, _ := influx.FastMarshalMultiRows(nil, rows)
	err = st.WriteRows(db, rp, ptId, shardID, rows, binaryRows)
	if !errno.Equal(err, errno.RepConfigWriteNoRepDB) {
		t.Fatal("TestStorage_RepConfigWrite err0")
	}

	// replication write
	mockClient.replicaInfo = &message.ReplicaInfo{
		ReplicaRole: meta.Master,
		Master:      nil,
		Peers: []*message.PeerInfo{
			{
				PtId:   1,
				NodeId: 1,
				ShardMapper: map[uint64]uint64{
					1: 2,
				},
			},
		},
		ReplicaStatus: meta.Health,
		Term:          0,
	}
	err = st.WriteRows("db1", rp, ptId, shardID, rows, binaryRows)
	if !errno.Equal(err, errno.PtNotFound) {
		t.Fatal("TestStorage_RepConfigWrite err1")
	}
	err = st.WriteRows("db2", rp, ptId, shardID, rows, binaryRows)
	if !errno.Equal(err, errno.DatabaseNotFound) {
		t.Fatal("TestStorage_RepConfigWrite err2")
	}
}

func TestWriteBlobs(t *testing.T) {
	st := &Storage{
		log:          logger.NewLogger(errno.ModuleStorageEngine),
		stop:         make(chan struct{}),
		slaveStorage: &MockSlaveStorage{},
	}

	dir := t.TempDir()
	loadCtx := metaclient.LoadCtx{}
	loadCtx.LoadCh = make(chan *metaclient.DBPTCtx, 100)
	newEngineFn := engine.GetNewEngineFunction(config.DefaultEngine)
	eng, err := newEngineFn(dir, dir, engineOption, &loadCtx)
	require.NoError(t, err)
	eng.SetMetaClient(st.metaClient)
	st.engine = eng
	defer st.MustClose()

	bg, _ := shelf.NewBlobGroup(1)
	err = st.WriteBlobs("db", "rp", 1, 1, bg, 0, 0)
	require.Error(t, err)
}

func Test_StorageAppendShardMergeService(t *testing.T) {
	st1 := &Storage{
		engine: &engine.EngineImpl{},
	}
	st1.appendShardMergeService(config.ShardMergeConfig{Enabled: true})
}

func TestClearRepCold(t *testing.T) {
	s := Storage{
		engine: &MockEngine{},
	}
	err := s.ClearRepCold(nil)
	require.NoError(t, err)
}
