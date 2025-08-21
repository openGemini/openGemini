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
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	meta "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
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

	conf := config.GetShelfMode()
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

	err = recordWriter.RetryWriteRecords("dbpt_not_exists", "default", recs)
	require.ErrorContains(t, err, "database not found: dbpt_not_exists")

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
	if name == "db_not_exists" {
		return nil, errno.NewError(errno.DatabaseNotFound)
	}
	db := &meta.DatabaseInfo{
		Name:                   name,
		DefaultRetentionPolicy: "default",
		RetentionPolicies:      make(map[string]*meta.RetentionPolicyInfo),
	}
	db.RetentionPolicies["default"] = NewRetentionPolicy("default", time.Hour, config.TSSTORE)
	if name == "db_with_shardKey" {
		db.ShardKey = meta.ShardKeyInfo{
			ShardKey:   []string{"dbShardKey"},
			Type:       "hash",
			ShardGroup: 1,
		}
	}
	return db, mc.DatabaseErr
}

func (mc *MockMetaClient) Measurement(db, rp, mst string) (*meta.MeasurementInfo, error) {
	if mst == "mst_not_exists" {
		return nil, errno.NewError(errno.ErrMeasurementNotFound)
	}
	if mst == "mst_with_shardKey" {
		mi := &meta.MeasurementInfo{
			Name: mst,
		}
		mi.ShardKeys = []meta.ShardKeyInfo{
			{
				ShardKey:   []string{"mstShardKey"},
				Type:       "hash",
				ShardGroup: 1,
			},
		}
		return mi, nil
	}
	mi, err := mc.SimpleCreateMeasurement(db, rp, mst, config.TSSTORE)
	return mi, err
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
	return []int{0, 1}
}

func (mc *MockMetaClient) CreateShardGroup(database, policy string, timestamp time.Time,
	version uint32, engineType config.EngineType) (*meta.ShardGroupInfo, error) {
	sg := &meta.ShardGroupInfo{
		ID:        1,
		StartTime: timestamp,
		EndTime:   timestamp.Add(10 * time.Nanosecond),
		Shards: []meta.ShardInfo{
			{ID: 0},
			{ID: 1},
		},
		EngineType: config.TSSTORE,
	}
	return sg, nil
}

func (mc *MockMetaClient) DBPtView(database string) (meta.DBPtInfos, error) {
	if database == "dbpt_not_exists" {
		return nil, errno.NewError(errno.DatabaseNotFound, database)
	}
	ret := meta.DBPtInfos{}
	ret = append(ret, meta.PtInfo{
		Owner:  meta.PtOwner{NodeID: 10},
		Status: 0,
		PtId:   1,
		Ver:    0,
		RGID:   1,
	})

	return ret, nil
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

func ResetMetaManager(t *testing.T, m *writer.MetaManager, times []int64) {
	err := m.CreateShardGroupIfNeeded(times)
	require.NoError(t, err)
	err = m.ResetDatabaseInfo()
	require.NoError(t, err)
}

func TestUnmarshalShardKeysMatch(t *testing.T) {
	r := influx.Row{
		Name: "mst",
		Tags: influx.PointTags{influx.Tag{Key: "tag1", Value: "v1"}, influx.Tag{Key: "tag2"}, influx.Tag{Key: "tag3", Value: "v3"}},
	}
	err := r.UnmarshalShardKeyByTag([]string{"tag1", "tag2", "tag3"})
	require.NoError(t, err)
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Tag, Name: "tag1"},
		record.Field{Type: influx.Field_Type_Tag, Name: "tag2"},
		record.Field{Type: influx.Field_Type_Tag, Name: "tag3"},
		record.Field{Type: influx.Field_Type_Int, Name: "fd1"},
	}
	rec := record.NewRecord(schema, false)
	rec.ColVals[0].AppendString("v1")
	rec.ColVals[2].AppendString("v3")
	rec.ColVals[3].AppendInteger(2)
	tags, _ := record.SplitTagField(rec)
	var dst []byte
	dst, err = record.UnmarshalShardKeys(tags, []string{"tag1", "tag2", "tag3"}, 0, dst)
	require.NoError(t, err)
	require.Equal(t, r.ShardKey[len(r.Name)+1:], dst)

	r = influx.Row{
		Name: "mst",
		Tags: influx.PointTags{influx.Tag{Key: "tag1", Value: "v1"}, influx.Tag{Key: "tag2", Value: "v2"}, influx.Tag{Key: "tag3", Value: "v3"}},
	}
	err0 := r.UnmarshalShardKeyByTag([]string{"tag0", "tag2", "tag3"})
	require.EqualError(t, err0, errno.NewError(errno.WritePointShouldHaveAllShardKey).Error())
	schema = record.Schemas{
		record.Field{Type: influx.Field_Type_Tag, Name: "tag1"},
		record.Field{Type: influx.Field_Type_Tag, Name: "tag2"},
		record.Field{Type: influx.Field_Type_Tag, Name: "tag3"},
		record.Field{Type: influx.Field_Type_Int, Name: "fd1"},
	}
	rec = record.NewRecord(schema, false)
	rec.ColVals[0].AppendString("v1")
	rec.ColVals[1].AppendString("v2")
	rec.ColVals[2].AppendString("v3")
	rec.ColVals[3].AppendInteger(2)
	tags, _ = record.SplitTagField(rec)
	_, err1 := record.UnmarshalShardKeys(tags, []string{"tag0", "tag2", "tag3"}, 0, dst)
	require.EqualError(t, err1, errno.NewError(errno.WritePointShouldHaveAllShardKey).Error())

	r = influx.Row{
		Name: "mst",
		Tags: influx.PointTags{influx.Tag{Key: "tag1", Value: "v1"}, influx.Tag{Key: "tag2", Value: "v2"}, influx.Tag{Key: "tag3", Value: "v3"}},
	}
	err2 := r.UnmarshalShardKeyByTag([]string{"tag1", "tag2", "tag4"})
	require.EqualError(t, err2, errno.NewError(errno.WritePointShouldHaveAllShardKey).Error())
	schema = record.Schemas{
		record.Field{Type: influx.Field_Type_Tag, Name: "tag1"},
		record.Field{Type: influx.Field_Type_Tag, Name: "tag2"},
		record.Field{Type: influx.Field_Type_Tag, Name: "tag3"},
		record.Field{Type: influx.Field_Type_Int, Name: "fd1"},
	}
	rec = record.NewRecord(schema, false)
	rec.ColVals[0].AppendString("v1")
	rec.ColVals[1].AppendString("v2")
	rec.ColVals[2].AppendString("v3")
	rec.ColVals[3].AppendInteger(2)
	tags, _ = record.SplitTagField(rec)
	_, err3 := record.UnmarshalShardKeys(tags, []string{"tag1", "tag2", "tag4"}, 0, dst)
	require.EqualError(t, err3, errno.NewError(errno.WritePointShouldHaveAllShardKey).Error())
}

func TestRecordWriter_MapRecord(t *testing.T) {
	mc := &MockMetaClient{}
	recordWriter := writer.NewRecordWriter(mc, time.Second*10)
	recordWriter.WithLogger(logger.NewLogger(errno.ModuleWrite))

	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Tag, Name: "tag1"},
		record.Field{Type: influx.Field_Type_Tag, Name: "tag2"},
		record.Field{Type: influx.Field_Type_Int, Name: "field1"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := record.NewRecord(schema, false)
	rec.ColVals[0].AppendStrings("v1", "v2", "v3")
	rec.ColVals[1].AppendStrings("v1", "v2", "v4")
	rec.ColVals[2].AppendIntegers(1, 2, 3)
	rec.AppendTime([]int64{1, 3, 5}...)

	// Test the case that database information retrieval fails
	ctx, release := writer.NewWriteContext(mc, "db_not_exists", "rp")
	defer func() {
		release()
	}()
	err := ctx.Meta().ResetDatabaseInfo()
	require.EqualError(t, err, errno.NewError(errno.DatabaseNotFound).Error())

	// Test the case that table information retrieval fails
	ctx, release = writer.NewWriteContext(mc, "db", "rp")
	recordWriter.MapRecord(ctx, "mst_not_exists_0000", rec)
	err = ctx.Err().PartialErr()
	require.EqualError(t, err, errno.NewError(errno.ErrMeasurementNotFound).Error())

	// Test the case that shard has not been initialized
	ctx, _ = writer.NewWriteContext(mc, "db", "rp")
	err = ctx.Meta().ResetDatabaseInfo()
	require.NoError(t, err)
	recordWriter.MapRecord(ctx, "mst", rec)
	err = ctx.Err().PartialErr()
	require.EqualError(t, err, errno.NewError(errno.WritePointMap2Shard).Error())

	// Test the case that neither the database nor the table has set a shardKey
	config.GetShelfMode().Concurrent = 4
	ctx, _ = writer.NewWriteContext(mc, "db", "rp")
	ResetMetaManager(t, ctx.Meta(), rec.Times())
	recordWriter.MapRecord(ctx, "mst", rec)
	shardNums := len(reflect.ValueOf(ctx.Meta().Shards()).MapKeys())
	require.Equal(t, len(mc.GetAliveShards("", &meta.ShardGroupInfo{}, false)), shardNums)

	// Test the case that there shardKey is a mismatch
	ctx, _ = writer.NewWriteContext(mc, "db", "rp")
	ResetMetaManager(t, ctx.Meta(), rec.Times())
	recordWriter.MapRecord(ctx, "mst_with_shardKey_0000", rec)
	err = ctx.Err().PartialErr()
	require.EqualError(t, err, errno.NewError(errno.WritePointShouldHaveAllShardKey).Error())

	// Test the case that the shardKey exists in the database
	schema = record.Schemas{
		record.Field{Type: influx.Field_Type_Tag, Name: "dbShardKey"},
		record.Field{Type: influx.Field_Type_Tag, Name: "tag2"},
		record.Field{Type: influx.Field_Type_Int, Name: "field1"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec = record.NewRecord(schema, false)
	rec.ColVals[0].AppendStrings("v1", "v1", "v1")
	rec.ColVals[1].AppendStrings("v1", "v2", "v3")
	rec.ColVals[2].AppendIntegers(1, 2)
	rec.AppendTime([]int64{1, 3, 5}...)
	ctx, _ = writer.NewWriteContext(mc, "db_with_shardKey", "rp")
	ResetMetaManager(t, ctx.Meta(), rec.Times())
	recordWriter.MapRecord(ctx, "mst", rec)
	shardNums = len(reflect.ValueOf(ctx.Meta().Shards()).MapKeys())
	require.Equal(t, 1, shardNums)

	// Test the case that both the database and the table have shardKey set
	ctx, _ = writer.NewWriteContext(mc, "db_with_shardKey", "rp")
	ResetMetaManager(t, ctx.Meta(), rec.Times())
	recordWriter.MapRecord(ctx, "mst_with_shardKey_0000", rec)
	shardNums = len(reflect.ValueOf(ctx.Meta().Shards()).MapKeys())
	require.Equal(t, 1, shardNums)

	// Test the case that the table has shardKey set
	schema = record.Schemas{
		record.Field{Type: influx.Field_Type_Tag, Name: "mstShardKey"},
		record.Field{Type: influx.Field_Type_Tag, Name: "tag2"},
		record.Field{Type: influx.Field_Type_Int, Name: "field1"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec = record.NewRecord(schema, false)
	rec.ColVals[0].AppendStrings("v1", "v1", "v1")
	rec.ColVals[1].AppendStrings("v1", "v2", "v3")
	rec.ColVals[2].AppendIntegers(1, 2)
	rec.AppendTime([]int64{1, 3, 5}...)
	ctx, _ = writer.NewWriteContext(mc, "db", "rp")
	ResetMetaManager(t, ctx.Meta(), rec.Times())
	recordWriter.MapRecord(ctx, "mst_with_shardKey_0000", rec)
	shardNums = len(reflect.ValueOf(ctx.Meta().Shards()).MapKeys())
	require.Equal(t, 1, shardNums)
}

var exitNodeID uint64 = 1
var notExitNodeID uint64 = 2
var aliveNodeID uint64 = 1024

type MockMetaClient2 struct {
	metaclient.Client
}

func (c *MockMetaClient2) ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo) {
	return "db0", "default", &meta.ShardGroupInfo{}
}

func (c *MockMetaClient2) DataNode(id uint64) (*meta.DataNode, error) {
	if id == exitNodeID {
		return &meta.DataNode{
			NodeInfo: meta.NodeInfo{
				ID:   1,
				Host: "192.168.0.1:8400",
			},
		}, nil
	} else if id == aliveNodeID {
		return &meta.DataNode{
			NodeInfo: meta.NodeInfo{
				ID:     1024,
				Host:   "192.168.0.1:8400",
				Status: serf.MemberStatus(meta.StatusAlive),
			},
		}, nil
	} else {
		return nil, fmt.Errorf("no data node")
	}
}

func TestWriteBlobs(t *testing.T) {
	store := writer.NewRecordStore(&MockMetaClient2{})
	bg, _ := shelf.NewBlobGroup(1)
	err := store.WriteBlobs("db0", "default", 1, 1, bg, notExitNodeID, time.Second)
	require.EqualError(t, err, "no data node")

	bg, _ = shelf.NewBlobGroup(1)
	err = store.WriteBlobs("db0", "default", 1, 1, bg, aliveNodeID, time.Second)
	require.NotEmpty(t, err)
}
