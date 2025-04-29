// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package writer_test

import (
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/writer"
	"github.com/stretchr/testify/require"
)

func TestRecordWriter(t *testing.T) {
	mc := &MockMetaClient{}
	recordWriter := writer.NewRecordWriter(mc, time.Second*10)
	go recordWriter.ApplyTimeRangeLimit(nil)
	go recordWriter.ApplyTimeRangeLimit([]toml.Duration{0, 0})
	go recordWriter.ApplyTimeRangeLimit([]toml.Duration{toml.Duration(time.Hour), toml.Duration(time.Hour)})
	recordWriter.WithLogger(logger.NewLogger(errno.ModuleWrite))

	recs := make([]*writer.MstRecord, 1)
	recs[0] = &writer.MstRecord{}
	recs[0].Mst = "mst_1"

	// time is expired
	rec := buildRecord(5, []uint64{100})
	timeCol := &record.ColVal{}
	timeCol.AppendIntegers(100)
	rec.ColVals[rec.Len()-1] = *timeCol
	recs[0].Rec = *rec

	err := recordWriter.RetryWriteRecords("db0", "default", recs)
	require.ErrorContains(t, err, "point time is expired")

	conf := &config.GetStoreConfig().ShelfMode
	conf.Corrector(8)
	conf.Enabled = true
	defer func() {
		conf.Enabled = false
	}()
	shelf.Open()
	timeCol.Init()
	timeCol.AppendInteger(time.Now().UnixNano())
	err = recordWriter.RetryWriteRecords("db0", "default", recs)
	require.NoError(t, err)

	recordWriter.Close()
}

func TestWriteError(t *testing.T) {
	wr := &writer.WriteError{}
	require.NoError(t, wr.Error())

	err := fmt.Errorf("some error")
	require.EqualError(t, wr.Assert(err, 1), err.Error())

	require.NoError(t, wr.Assert(errno.NewError(errno.InvalidMeasurement), 1))
	require.NoError(t, wr.Assert(errno.NewError(errno.ErrorTagArrayFormat), 1))
}

func TestMetaManager(t *testing.T) {
	mc := &MockMetaClient{}
	mm := writer.NewMetaManager()
	mm.Init(mc, "db0", "")

	mc.DatabaseErr = fmt.Errorf("some error")
	require.Error(t, mm.CheckDBRP())

	mc.DatabaseErr = nil
	mm.Init(mc, "db0", "rp_not_exists")
	require.Error(t, mm.CheckDBRP())

	mm.Init(mc, "db0", "")
	require.NoError(t, mm.CheckDBRP())

	msInfo, _ := mc.SimpleCreateMeasurement("db", "default", "mst", config.TSSTORE)
	rec := &record.Record{}
	require.NoError(t, mm.UpdateSchemaIfNeeded(rec, msInfo, "mst"))

	schema := record.Schemas{
		{Type: 2, Name: "hostname"},
		{Type: 1, Name: "time"},
	}
	rec.ResetWithSchema(schema)
	require.NoError(t, mm.UpdateSchemaIfNeeded(rec, msInfo, "mst"))
}

func buildRecord(fields int, series []uint64) *record.Record {
	var types = []int{influx.Field_Type_String, influx.Field_Type_Int, influx.Field_Type_Float}
	rec := &record.Record{}
	rec.ReserveSchemaAndColVal(fields + 2)

	var appendRow = func(sid uint64) {
		for i := range fields {
			schema := &rec.Schema[i]
			schema.Name = fmt.Sprintf("foo_%d", i)
			schema.Type = types[i%len(types)]

			col := &rec.ColVals[i]
			switch schema.Type {
			case influx.Field_Type_String:
				col.AppendString("foo_00000001")
			case influx.Field_Type_Int:
				col.AppendInteger(int64(fields) * 11)
			case influx.Field_Type_Float:
				col.AppendFloat(float64(fields) * 77.23)
			}
		}

		rec.Schema[fields].Name = "tag"
		rec.Schema[fields].Type = influx.Field_Type_Tag
		rec.Column(fields).AppendString(fmt.Sprintf("s_%d", sid))

		rec.Schema[fields+1].Name = "time"
		rec.Schema[fields+1].Type = influx.Field_Type_Int
		rec.TimeColumn().AppendInteger(time.Now().UnixNano())
	}

	for i := range series {
		appendRow(series[i])
	}

	sort.Sort(rec)
	return rec
}

type MockMetaClient struct {
	writer.MetaClient

	DatabaseErr error
}

func (mc *MockMetaClient) Database(name string) (*meta.DatabaseInfo, error) {
	db := &meta.DatabaseInfo{
		Name:                   name,
		DefaultRetentionPolicy: "default",
		RetentionPolicies:      make(map[string]*meta.RetentionPolicyInfo),
	}
	db.RetentionPolicies["default"] = NewRetentionPolicy("default", time.Hour, config.TSSTORE)
	return db, mc.DatabaseErr
}

func (mc *MockMetaClient) Measurement(db, rp, mst string) (*meta.MeasurementInfo, error) {

	return nil, meta.ErrMeasurementNotFound
}

func (mc *MockMetaClient) SimpleCreateMeasurement(db, rp, mst string, engineType config.EngineType) (*meta.MeasurementInfo, error) {
	msInfo := &meta.MeasurementInfo{
		Name: mst,
		Schema: &meta.CleanSchema{
			"hostname": meta.SchemaVal{
				Typ:     1,
				EndTime: 0,
			},
		},
	}

	return msInfo, nil
}

func (mc *MockMetaClient) UpdateSchema(db, rp, mst string, schema []*proto.FieldSchema) error {

	return nil
}

func (mc *MockMetaClient) GetAliveShards(db string, sgi *meta.ShardGroupInfo, isRead bool) []int {
	return []int{1}
}

func (mc *MockMetaClient) CreateShardGroup(database, policy string, timestamp time.Time,
	version uint32, engineType config.EngineType) (*meta.ShardGroupInfo, error) {
	sg := &meta.ShardGroupInfo{
		ID:        1,
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now().Add(time.Hour),
		Shards: []meta.ShardInfo{
			{ID: 0},
			{ID: 1},
		},
		EngineType: config.TSSTORE,
	}
	return sg, nil
}

func (mc *MockMetaClient) DBPtView(database string) (meta.DBPtInfos, error) {
	pts := meta.DBPtInfos{
		meta.PtInfo{
			Owner:  meta.PtOwner{NodeID: 10},
			Status: 0,
			PtId:   1,
			Ver:    0,
			RGID:   1,
		},
	}
	return pts, nil
}

var shardID uint64

func nextShardID() uint64 {
	return atomic.AddUint64(&shardID, 1)
}

func NewRetentionPolicy(name string, duration time.Duration, engineType config.EngineType) *meta.RetentionPolicyInfo {
	var shards []meta.ShardInfo
	owners := make([]uint32, 1)
	owners[0] = 0

	shards = append(shards, meta.ShardInfo{ID: nextShardID(), Owners: owners})
	start := time.Now()
	rp := &meta.RetentionPolicyInfo{
		Name:               name,
		Duration:           duration,
		ShardGroupDuration: duration,
		ShardGroups: []meta.ShardGroupInfo{
			{ID: nextShardID(),
				StartTime:  start,
				EndTime:    start.Add(duration).Add(-1),
				Shards:     shards,
				EngineType: engineType,
			},
		},
	}
	return rp
}
