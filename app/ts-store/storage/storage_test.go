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

package storage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	retention2 "github.com/influxdata/influxdb/services/retention"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	netstorage.Engine
}

func (e *MockEngine) WriteRec(_, _ string, _ uint32, _ uint64, _ *record.Record, _ []byte) error {
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

func (mc *MockMetaClient) GetMeasurementInfoStore(dbName string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
	return mc.GetMeasurementInfoStoreFn(dbName, rpName, mstName)
}

func (mc *MockMetaClient) GetReplicaInfo(db string, pt uint32) *message.ReplicaInfo {
	return mc.replicaInfo
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

	newEngineFn := netstorage.GetNewEngineFunction(config.DefaultEngine)
	opt := netstorage.NewEngineOptions()
	loadCtx := metaclient.LoadCtx{}
	loadCtx.LoadCh = make(chan *metaclient.DBPTCtx, 100)
	eng, err := newEngineFn(dir, dir, opt, &loadCtx)
	if err != nil {
		t.Fatal(err)
	}
	st.engine = eng
	db := "db0"
	rp := "autogen"
	mst := "mst"
	var ptId uint32 = 1
	var shardID uint64 = 1
	st.engine.CreateDBPT(db, ptId, false)
	err = st.Write(db, rp, mst, ptId, shardID, func() error {
		return st.engine.WriteRows(db, rp, ptId, shardID, nil, nil)
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
		return st.engine.WriteRows(db, rp, ptId, 2, nil, nil)
	})
	assert.Equal(t, true, errno.Equal(err, errno.NoNodeAvailable))
}

func TestWriteRowsToSlave(t *testing.T) {
	slaveStorage := &MockSlaveStorage{}
	mc := &MockMetaClient{}
	st := &Storage{
		log:          logger.NewLogger(errno.ModuleStorageEngine),
		stop:         make(chan struct{}),
		slaveStorage: slaveStorage,
		MetaClient:   mc,
	}

	rows := make([]influx.Row, 10)
	require.NoError(t, st.WriteRowsToSlave(rows, "db", "rp", 1, 1))

	mc.replicaInfo = &message.ReplicaInfo{ReplicaRole: meta.Master, Peers: []*message.PeerInfo{{}}}
	slaveStorage.err = errors.New("some mock error")
	require.EqualError(t, st.WriteRowsToSlave(rows, "db", "rp", 1, 1), slaveStorage.err.Error())

	slaveStorage.err = nil
	require.NoError(t, st.WriteRowsToSlave(rows, "db", "rp", 1, 1))
}

type MockSlaveStorage struct {
	err error
}

func (s *MockSlaveStorage) WriteRows(ctx *netstorage.WriteContext, nodeID uint64, pt uint32, database, rpName string, timeout time.Duration) error {
	return s.err
}
