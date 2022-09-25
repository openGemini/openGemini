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

package coordinator

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

type MockMetaClient struct {
	DatabaseFn          func(database string) (*meta2.DatabaseInfo, error)
	RetentionPolicyFn   func(database, rp string) (*meta2.RetentionPolicyInfo, error)
	CreateShardGroupFn  func(database, policy string, timestamp time.Time) (*meta2.ShardGroupInfo, error)
	DBPtViewFn          func(database string) (meta2.DBPtInfos, error)
	MeasurementFn       func(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error)
	UpdateSchemaFn      func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error
	CreateMeasurementFn func(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, indexR *meta2.IndexRelation) (*meta2.MeasurementInfo, error)
	GetAliveShardsFn    func(database string, sgi *meta2.ShardGroupInfo) []int
}

func (mmc *MockMetaClient) Database(name string) (di *meta2.DatabaseInfo, err error) {
	return mmc.DatabaseFn(name)
}

func (mmc *MockMetaClient) RetentionPolicy(database, policy string) (*meta2.RetentionPolicyInfo, error) {
	return mmc.RetentionPolicyFn(database, policy)
}

func (mmc *MockMetaClient) CreateShardGroup(database, policy string, timestamp time.Time) (*meta2.ShardGroupInfo, error) {
	return mmc.CreateShardGroupFn(database, policy, timestamp)
}

func (mmc *MockMetaClient) DBPtView(database string) (meta2.DBPtInfos, error) {
	return mmc.DBPtViewFn(database)
}

func (mmc *MockMetaClient) Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
	return mmc.MeasurementFn(database, rpName, mstName)
}

func (mmc *MockMetaClient) UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
	return mmc.UpdateSchemaFn(database, retentionPolicy, mst, fieldToCreate)
}

func (mmc *MockMetaClient) CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, indexR *meta2.IndexRelation) (*meta2.MeasurementInfo, error) {
	return mmc.CreateMeasurementFn(database, retentionPolicy, mst, shardKey, indexR)
}

func (mmc *MockMetaClient) GetAliveShards(database string, sgi *meta2.ShardGroupInfo) []int {
	return mmc.GetAliveShardsFn(database, sgi)
}

func NewMockMetaClient() *MockMetaClient {
	mc := &MockMetaClient{}
	rpInfo := NewRetentionPolicy("rp0", time.Hour)
	dbInfo := &meta2.DatabaseInfo{Name: "db0", RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{"rp0": rpInfo}}
	mc.DatabaseFn = func(database string) (*meta2.DatabaseInfo, error) {
		return dbInfo, nil
	}
	mc.RetentionPolicyFn = func(database, rp string) (*meta2.RetentionPolicyInfo, error) {
		return rpInfo, nil
	}

	mc.CreateShardGroupFn = func(database, policy string, timestamp time.Time) (*meta2.ShardGroupInfo, error) {
		for i := range rpInfo.ShardGroups {
			if timestamp.Equal(rpInfo.ShardGroups[i].StartTime) || timestamp.After(rpInfo.ShardGroups[i].StartTime) && timestamp.Before(rpInfo.ShardGroups[i].EndTime) {
				return &rpInfo.ShardGroups[i], nil
			}
		}
		panic("could not find sg")
	}

	msti := NewMeasurement("mst0")
	mc.CreateMeasurementFn = func(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, indexR *meta2.IndexRelation) (*meta2.MeasurementInfo, error) {
		return msti, nil
	}
	mc.MeasurementFn = func(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
		return msti, nil
	}
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		return nil
	}
	mc.DBPtViewFn = func(database string) (meta2.DBPtInfos, error) {
		return []meta2.PtInfo{{PtId: 0, Owner: meta2.PtOwner{NodeID: 1}, Status: meta2.Online}}, nil
	}
	mc.GetAliveShardsFn = func(database string, sgi *meta2.ShardGroupInfo) []int {
		ptView, _ := mc.DBPtViewFn(database)
		idxes := make([]int, 0, len(ptView))
		for i := range sgi.Shards {
			if ptView[sgi.Shards[i].Owners[0]].Status == meta2.Online {
				idxes = append(idxes, i)
			}
		}
		return idxes
	}
	return mc
}

var shardID uint64

func nextShardID() uint64 {
	return atomic.AddUint64(&shardID, 1)
}

func NewRetentionPolicy(name string, duration time.Duration) *meta2.RetentionPolicyInfo {
	shards := []meta2.ShardInfo{}
	owners := make([]uint32, 1)
	owners[0] = 0

	shards = append(shards, meta2.ShardInfo{ID: nextShardID(), Owners: owners})
	start := time.Now()
	rp := &meta2.RetentionPolicyInfo{
		Name:               name,
		Duration:           duration,
		ShardGroupDuration: duration,
		ShardGroups: []meta2.ShardGroupInfo{
			{ID: nextShardID(),
				StartTime: start,
				EndTime:   start.Add(duration).Add(-1),
				Shards:    shards,
			},
		},
	}
	return rp
}

func NewMeasurement(mst string) *meta2.MeasurementInfo {
	msti := &meta2.MeasurementInfo{Name: mst, ShardKeys: []meta2.ShardKeyInfo{meta2.ShardKeyInfo{Type: "hash"}}}
	msti.Schema = map[string]int32{
		"tk1": influx.Field_Type_Tag,
		"tk2": influx.Field_Type_Tag,
	}
	return msti
}

type MockNetStore struct {
	WriteRowsFn func(nodeID uint64, database, rp string, pt uint32, shard uint64, rows *[]influx.Row, timeout time.Duration) error
}

func (mns *MockNetStore) WriteRows(nodeID uint64, database, rp string, pt uint32, shard uint64, rows *[]influx.Row, timeout time.Duration) error {
	return mns.WriteRowsFn(nodeID, database, rp, pt, shard, rows, timeout)
}

func NewMockNetStore() *MockNetStore {
	store := &MockNetStore{}
	store.WriteRowsFn = func(nodeID uint64, database, rp string, pt uint32, shard uint64, rows *[]influx.Row, timeout time.Duration) error {
		return nil
	}
	return store
}

func TestPointsWriter_WritePointRows(t *testing.T) {
	pw := NewPointsWriter(time.Second)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	err := pw.WritePointRows("db0", "rp0", generateRows())
	if err != nil {
		t.Fatal(err)
	}
}

func TestPointsWriter_updateSchemaIfNeeded(t *testing.T) {
	mi := &meta2.MeasurementInfo{
		Name:      "mst",
		ShardKeys: nil,
		Schema: map[string]int32{
			"value1": influx.Field_Type_Float,
			"value2": influx.Field_Type_String,
		},
		IndexRelations: nil,
		MarkDeleted:    false,
	}

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		if len(fieldToCreate) > 0 && fieldToCreate[0].GetFieldName() == "value3" {
			return fmt.Errorf("field type conflict")
		}

		for _, item := range fieldToCreate {
			mi.Schema[item.GetFieldName()] = item.GetFieldType()
		}
		return nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()

	var errors = []string{
		"",
		`field type conflict: input field "value2" on measurement "mst" is type float, already exists as type string`,
		"field type conflict",
		"",
	}

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}

		for i, r := range rows {
			_, _, err := pw.updateSchemaIfNeeded("db0", "rp0", &r, mi, fs)

			if errors[i] == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, errors[i])
			}
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,tk1=value1,tk2=value2,tk3=value3 value1=1.1`)
	buf.WriteByte('\n')
	buf.WriteString(`mst,tk1=value11,tk2=value22,tk3=value33 value2=22`)
	buf.WriteByte('\n')
	buf.WriteString(`mst,tk1=value11,tk2=value22,tk3=value33 value3=99`)
	buf.WriteByte('\n')
	buf.WriteString(`mst,tk1=value11,tk2=value22,tk3=value33 value4=99`)
	buf.WriteByte('\n')
	unmarshal(buf.Bytes(), callback)
}

func unmarshal(buf []byte, callback func(db string, rows []influx.Row, err error)) {
	w := influx.GetUnmarshalWork()
	w.Callback = callback
	w.Db = ""
	w.ReqBuf = buf
	w.Unmarshal()
}

func generateRows() []influx.Row {
	keys := []string{
		"mst0,tk1=value1,tk2=value2,tk3=value3",
		"mst0,tk1=value11,tk2=value22,tk3=value33",
	}
	pts := make([]influx.Row, 0, len(keys))
	for _, key := range keys {
		pt := influx.Row{}
		strs := strings.Split(key, ",")
		pt.Name = strs[0]
		pt.Tags = make(influx.PointTags, len(strs)-1)
		for i, str := range strs[1:] {
			kv := strings.Split(str, "=")
			pt.Tags[i].Key = kv[0]
			pt.Tags[i].Value = kv[1]
		}
		sort.Sort(&pt.Tags)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pts = append(pts, pt)
	}
	return pts
}

func TestCheckFields(t *testing.T) {
	fields := influx.Fields{
		{
			Key:  "foo",
			Type: influx.Field_Type_Float,
		},
		{
			Key:  "foo",
			Type: influx.Field_Type_Int,
		},
	}

	err := checkFields(fields)
	assert.EqualError(t, err, errno.NewError(errno.DuplicateField, "foo").Error())

	fields[0].Key = "foo2"
	err = checkFields(fields)
	assert.NoError(t, err)
}
