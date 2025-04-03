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

package coordinator

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	noStream = iota
	sameShard
	sameNode
	sameMst
	diffDis
)

var streamDistribution = noStream

var enableFieldIndex = true

var engineType = config.TSSTORE

type MockMetaClient struct {
	DatabaseFn           func(database string) (*meta2.DatabaseInfo, error)
	RetentionPolicyFn    func(database, rp string) (*meta2.RetentionPolicyInfo, error)
	CreateShardGroupFn   func(database, policy string, timestamp time.Time, version uint32, engineType config.EngineType) (*meta2.ShardGroupInfo, error)
	DBPtViewFn           func(database string) (meta2.DBPtInfos, error)
	MeasurementFn        func(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error)
	UpdateSchemaFn       func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error
	CreateMeasurementFn  func(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, numOfShard int32, indexR *influxql.IndexRelation, engineType config.EngineType, colStoreInfo *meta2.ColStoreInfo) (*meta2.MeasurementInfo, error)
	GetAliveShardsFn     func(database string, sgi *meta2.ShardGroupInfo, isRead bool) []int
	GetShardInfoByTimeFn func(database, retentionPolicy string, t time.Time, ptIdx int, nodeId uint64, engineType config.EngineType) (*meta2.ShardInfo, error)
	DBRepGroupsFn        func(database string) []meta2.ReplicaGroup
	GetReplicaNFn        func(database string) (int, error)
	GetSgEndTimeFn       func(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error)
}

func (mmc *MockMetaClient) Database(name string) (di *meta2.DatabaseInfo, err error) {
	return mmc.DatabaseFn(name)
}

func (mmc *MockMetaClient) RetentionPolicy(database, policy string) (*meta2.RetentionPolicyInfo, error) {
	return mmc.RetentionPolicyFn(database, policy)
}

func (mmc *MockMetaClient) CreateShardGroup(database, policy string, timestamp time.Time, version uint32, engineType config.EngineType) (*meta2.ShardGroupInfo, error) {
	return mmc.CreateShardGroupFn(database, policy, timestamp, 0, engineType)
}

func (mmc *MockMetaClient) DBPtView(database string) (meta2.DBPtInfos, error) {
	return mmc.DBPtViewFn(database)
}

func (mmc *MockMetaClient) DBRepGroups(database string) []meta2.ReplicaGroup {
	return mmc.DBRepGroupsFn(database)
}

func (mmc *MockMetaClient) GetReplicaN(database string) (int, error) {
	return mmc.GetReplicaNFn(database)
}

func (mmc *MockMetaClient) Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
	return mmc.MeasurementFn(database, rpName, mstName)
}

func (mmc *MockMetaClient) UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
	return mmc.UpdateSchemaFn(database, retentionPolicy, mst, fieldToCreate)
}

func (mmc *MockMetaClient) CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, numOfShards int32, indexR *influxql.IndexRelation,
	engineType config.EngineType, colStoreInfo *meta2.ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *meta2.Options) (*meta2.MeasurementInfo, error) {
	return mmc.CreateMeasurementFn(database, retentionPolicy, mst, shardKey, numOfShards, indexR, engineType, nil)
}

func (mmc *MockMetaClient) GetAliveShards(database string, sgi *meta2.ShardGroupInfo, isRead bool) []int {
	return mmc.GetAliveShardsFn(database, sgi, isRead)
}

func (mmc *MockMetaClient) GetShardInfoByTime(database, retentionPolicy string, t time.Time, ptIdx int, nodeId uint64, engineType config.EngineType) (*meta2.ShardInfo, error) {
	rp, err := mmc.RetentionPolicy(database, retentionPolicy)
	if err != nil {
		return nil, err
	}
	if rp.MarkDeleted {
		return nil, errno.NewError(errno.RpNotFound)
	}
	shardGroup := rp.ShardGroupByTimestampAndEngineType(t, engineType)
	if shardGroup == nil {
		return nil, errno.NewError(errno.ShardNotFound, shardGroup)
	}
	if shardGroup.Deleted() {
		return nil, errno.NewError(errno.ShardNotFound, shardGroup)
	}

	info, _ := mmc.DBPtView(database)
	if info == nil {
		return nil, errors.New(fmt.Sprintf("db %v in PtView not exist", database))
	}
	cnt := 0
	ptId := uint32(math.MaxUint32)
	for i := range info {
		if info[i].Owner.NodeID == nodeId {
			if ptIdx == cnt {
				ptId = info[i].PtId
				cnt++
				break
			} else {
				cnt++
			}
		}
	}
	if cnt == 0 || ptId == math.MaxUint32 {
		return nil, errors.New("nodeId cannot find pt")
	}

	shard := shardGroup.Shards[ptId]
	return &shard, nil
}

func (mmc *MockMetaClient) GetDstStreamInfos(db, rp string, dstSis *[]*meta2.StreamInfo) bool {
	sis := mmc.GetStreamInfos()
	if len(sis) == 0 {
		return false
	}
	i := 0
	*dstSis = (*dstSis)[:cap(*dstSis)]
	for _, si := range sis {
		if si.SrcMst.Database == db && si.SrcMst.RetentionPolicy == rp {
			if len(*dstSis) < i+1 {
				*dstSis = append(*dstSis, si)
			} else {
				(*dstSis)[i] = si
			}
			i++
		}
	}
	*dstSis = (*dstSis)[:i]
	return len(*dstSis) > 0
}

func (mmc *MockMetaClient) GetSgEndTime(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
	return mmc.GetSgEndTimeFn(database, rp, timestamp, engineType)
}

func (mmc *MockMetaClient) GetStreamInfos() map[string]*meta2.StreamInfo {
	infos := map[string]*meta2.StreamInfo{}
	info := &meta2.StreamInfo{}
	info.ID = 1
	src := meta2.StreamMeasurementInfo{
		Name:            "mst0",
		Database:        "db0",
		RetentionPolicy: "rp0",
	}

	var des meta2.StreamMeasurementInfo
	switch streamDistribution {
	case sameNode:
		des = meta2.StreamMeasurementInfo{
			Name:            "mst2",
			Database:        "db0",
			RetentionPolicy: "rp1",
		}
	case noStream:
		return map[string]*meta2.StreamInfo{}
	default:
		des = meta2.StreamMeasurementInfo{
			Name:            "mst2",
			Database:        "db0",
			RetentionPolicy: "rp0",
		}
	}

	info.DesMst = &des
	info.SrcMst = &src
	var groupKeys []string
	groupKeys = append(groupKeys, "tk1")
	info.Dims = groupKeys
	info.Name = "t"
	info.Interval = time.Duration(5)
	info.Calls = []*meta2.StreamCall{
		{
			Call:  "sum",
			Field: "fk1",
			Alias: "sum_fk1",
		},
		{
			Call:  "sum",
			Field: "fk2",
			Alias: "sum_fk2",
		},
		{
			Call:  "count",
			Field: "fk2",
			Alias: "count_fk2",
		},
		{
			Call:  "min",
			Field: "fk2",
			Alias: "min_fk2",
		},
		{
			Call:  "max",
			Field: "fk2",
			Alias: "max_fk2",
		},
	}
	infos[info.Name] = info
	return infos
}

func NewMockMetaClient() *MockMetaClient {
	mc := &MockMetaClient{}
	rpInfo := NewRetentionPolicy("rp0", time.Hour, engineType)
	rp1Info := NewRetentionPolicy("rp1", time.Hour, engineType)
	var dbInfo *meta2.DatabaseInfo

	switch streamDistribution {
	case sameShard:
		dbInfo = &meta2.DatabaseInfo{Name: "db0", RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{"rp0": rpInfo},
			ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tk1", "tk2"}}}
	case sameNode:
		dbInfo = &meta2.DatabaseInfo{Name: "db0", RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{"rp0": rpInfo, "rp1": rp1Info},
			ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tk1", "tk2"}}}
	default:
		dbInfo = &meta2.DatabaseInfo{Name: "db0", RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{"rp0": rpInfo}}
	}

	mc.DatabaseFn = func(database string) (*meta2.DatabaseInfo, error) {
		return dbInfo, nil
	}
	mc.RetentionPolicyFn = func(database, rp string) (*meta2.RetentionPolicyInfo, error) {
		return rpInfo, nil
	}

	mc.CreateShardGroupFn = func(database, policy string, timestamp time.Time, version uint32, engineType config.EngineType) (*meta2.ShardGroupInfo, error) {
		for i := range rpInfo.ShardGroups {
			if timestamp.Equal(rpInfo.ShardGroups[i].StartTime) || timestamp.After(rpInfo.ShardGroups[i].StartTime) && timestamp.Before(rpInfo.ShardGroups[i].EndTime) {
				return &rpInfo.ShardGroups[i], nil
			}
		}
		panic("could not find sg")
	}

	mc.CreateMeasurementFn = func(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, numOfShards int32, indexR *influxql.IndexRelation, engineType config.EngineType, colStoreInfo *meta2.ColStoreInfo) (*meta2.MeasurementInfo, error) {
		return NewMeasurement(mst, engineType), nil
	}
	mc.MeasurementFn = func(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
		return NewMeasurement(mstName, engineType), nil
	}
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		return nil
	}
	mc.DBPtViewFn = func(database string) (meta2.DBPtInfos, error) {
		return []meta2.PtInfo{{PtId: 0, Owner: meta2.PtOwner{NodeID: 0}, Status: meta2.Online}}, nil
	}
	mc.DBRepGroupsFn = func(database string) []meta2.ReplicaGroup {
		return nil
	}
	mc.GetReplicaNFn = func(database string) (int, error) {
		return 1, nil
	}
	mc.GetAliveShardsFn = func(database string, sgi *meta2.ShardGroupInfo, isRead bool) []int {
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

func NewMockMetaClientWithShardLists() *MockMetaClient {
	mc := NewMockMetaClient()
	mc.CreateMeasurementFn = func(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, numOfShards int32, indexR *influxql.IndexRelation, engineType config.EngineType, colStoreInfo *meta2.ColStoreInfo) (*meta2.MeasurementInfo, error) {
		rp, _ := mc.RetentionPolicy(database, retentionPolicy)
		return NewMeasurementWithShardLists(rp.ShardGroups, mst, engineType), nil
	}
	mc.MeasurementFn = func(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
		rp, _ := mc.RetentionPolicy(database, rpName)
		return NewMeasurementWithShardLists(rp.ShardGroups, mstName, engineType), nil
	}
	return mc
}

var shardID uint64

func nextShardID() uint64 {
	return atomic.AddUint64(&shardID, 1)
}

func NewRetentionPolicy(name string, duration time.Duration, engineType config.EngineType) *meta2.RetentionPolicyInfo {
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
				StartTime:  start,
				EndTime:    start.Add(duration).Add(-1),
				Shards:     shards,
				EngineType: engineType,
			},
		},
	}
	return rp
}

func NewMeasurement(mst string, engineType config.EngineType) *meta2.MeasurementInfo {
	var msti = meta2.NewMeasurementInfo(mst, influx.GetOriginMstName(mst), engineType, 0)
	switch streamDistribution {
	case sameMst:
		msti.ShardKeys = []meta2.ShardKeyInfo{{ShardKey: []string{"tk1"}, Type: "hash"}}
	default:
		msti.ShardKeys = []meta2.ShardKeyInfo{{Type: "hash"}}
	}

	if mst == "rtt" {
		msti.Schema = &meta2.CleanSchema{
			"int":     meta2.SchemaVal{Typ: influx.Field_Type_Int},
			"float":   meta2.SchemaVal{Typ: influx.Field_Type_Float},
			"boolean": meta2.SchemaVal{Typ: influx.Field_Type_Boolean},
			"string":  meta2.SchemaVal{Typ: influx.Field_Type_String},
		}
		msti.ColStoreInfo = &meta2.ColStoreInfo{PrimaryKey: []string{"time"}}
	} else {
		msti.Schema = &meta2.CleanSchema{
			"fk1": meta2.SchemaVal{Typ: influx.Field_Type_Float},
			"fk2": meta2.SchemaVal{Typ: influx.Field_Type_Int},
			"tk1": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
			"tk2": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
		}
	}

	if enableFieldIndex {
		ilist := []string{"tk3", "tk1"}
		msti.IndexRelation = influxql.IndexRelation{
			IndexList: []*influxql.IndexList{{IList: ilist}},
			Oids:      []uint32{0},
		}
	}
	return msti
}

func NewMeasurementWithShardLists(sgs []meta2.ShardGroupInfo, mst string, engineType config.EngineType) *meta2.MeasurementInfo {
	msti := NewMeasurement(mst, engineType)
	msti.InitNumOfShards = 1
	msti.ShardIdexes = make(map[uint64][]int)
	for _, sg := range sgs {
		msti.ShardIdexes[sg.ID] = []int{0}
	}

	return msti
}

type MockNetStore struct {
	WriteRowsFn func(ctx *netstorage.WriteContext, nodeID uint64, pt uint32, database, rp string, timeout time.Duration) error
}

func (mns *MockNetStore) WriteRows(ctx *netstorage.WriteContext, nodeID uint64, pt uint32, database, rp string, timeout time.Duration) error {
	return mns.WriteRowsFn(ctx, nodeID, pt, database, rp, timeout)
}

func NewMockNetStore() *MockNetStore {
	store := &MockNetStore{}
	store.WriteRowsFn = func(ctx *netstorage.WriteContext, nodeID uint64, pt uint32, database, rp string, timeout time.Duration) error {
		return nil
	}
	return store
}

func TestPointsWriter_WritePointRows(t *testing.T) {
	pw := NewPointsWriter(time.Second)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)
	err := pw.RetryWritePointRows("db0", "rp0", generateRows(10, rows))
	if err != nil {
		t.Fatal(err)
	}
}

func TestPointsWriter_WritePointRowsWithShardLists1(t *testing.T) {
	pw := NewPointsWriter(time.Second)
	pw.MetaClient = NewMockMetaClientWithShardLists()
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)
	err := pw.RetryWritePointRows("db0", "rp0", generateRows(10, rows))
	if err != nil {
		t.Fatal(err)
	}
}

func TestPointsWriter_updateSchemaIfNeeded(t *testing.T) {
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.TSSTORE, 0)
	mi.Schema = &meta2.CleanSchema{
		"value1": meta2.SchemaVal{Typ: influx.Field_Type_Float},
		"value2": meta2.SchemaVal{Typ: influx.Field_Type_String},
	}

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		if len(fieldToCreate) > 0 && fieldToCreate[0].GetFieldName() == "value3" {
			return fmt.Errorf("field type conflict")
		}

		for _, item := range fieldToCreate {
			mi.Schema.SetTyp(item.GetFieldName(), item.GetFieldType())
		}
		return nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var errors = []string{
		"",
		`field type conflict: input field "value2" on measurement "mst" is type float, already exists as type string`,
		"field type conflict",
		"",
		"tag key can't be time, measurement is 'mst'",
	}

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}

		for i, r := range rows {
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)

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
	buf.WriteString(`mst,boo=1,time=2 f1=1,f2=2`)
	unmarshal(buf.Bytes(), callback)
}

func TestPointsWriter_updateSchemaIfNeededError(t *testing.T) {
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.COLUMNSTORE, 0)
	mi.Schema = &meta2.CleanSchema{
		"value1": meta2.SchemaVal{Typ: influx.Field_Type_Float},
		"value2": meta2.SchemaVal{Typ: influx.Field_Type_Int},
	}
	mi.ColStoreInfo = &meta2.ColStoreInfo{}

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		if len(fieldToCreate) > 0 && fieldToCreate[0].GetFieldName() == "value3" {
			return fmt.Errorf("field type conflict")
		}

		for _, item := range fieldToCreate {
			mi.Schema.SetTyp(item.GetFieldName(), item.GetFieldType())
		}
		return nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var errors = []string{
		"field type conflict: input field \"value2\" on measurement \"mst\" is type float, already exists as type integer",
		"field type conflict: input field \"value2\" on measurement \"mst\" is type float, already exists as type integer",
		"field type conflict",
	}

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}

		for i, r := range rows {
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)

			if errors[i] == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, errors[i])
			}
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,tk1=value1 value1=1.1,value2=2`)
	buf.WriteByte('\n')
	buf.WriteString(`mst,tk1=value11 value2=22`)
	buf.WriteByte('\n')
	buf.WriteString(`mst value2=22,value3=99`)
	buf.WriteByte('\n')
	unmarshal(buf.Bytes(), callback)
}

func TestPointsWriter_updateSchemaIfNeededErrorV2(t *testing.T) {
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.COLUMNSTORE, 0)
	mi.Schema = &meta2.CleanSchema{
		"tk1":    meta2.SchemaVal{Typ: influx.Field_Type_Tag},
		"value1": meta2.SchemaVal{Typ: influx.Field_Type_Float},
		"value2": meta2.SchemaVal{Typ: influx.Field_Type_Int},
	}

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		if len(fieldToCreate) > 0 && fieldToCreate[0].GetFieldName() == "value3" {
			return fmt.Errorf("field type conflict")
		}

		for _, item := range fieldToCreate {
			mi.Schema.SetTyp(item.GetFieldName(), item.GetFieldType())
		}
		return nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var errors = []string{
		`column store write point has Invalid tag :value1`,
		"column store write point has Invalid field :tk1",
	}

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}

		for i, r := range rows {
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)

			if errors[i] == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, errors[i])
			}
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,tk1="value1",value1=1.1 value2=2`)
	buf.WriteByte('\n')
	buf.WriteString(`mst tk1=1,value2=22,value3=99`)
	buf.WriteByte('\n')
	unmarshal(buf.Bytes(), callback)
}

// new Tag and Field
func TestPointsWriter_updateCleanSchemaIfNeeded1(t *testing.T) {
	meta2.InitSchemaCleanEn(true)
	defer meta2.InitSchemaCleanEn(false)
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.TSSTORE, 0)
	mi.Schema = &meta2.CleanSchema{}

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		for _, item := range fieldToCreate {
			mi.Schema.SetTyp(item.GetFieldName(), item.GetFieldType())
		}
		return nil
	}
	mc.GetSgEndTimeFn = func(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
		return 0, nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}

		for _, r := range rows {
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)
			assert.NoError(t, err)
			_, ok := mi.Schema.GetTyp("tk1")
			assert.Equal(t, ok, true)
			_, ok = mi.Schema.GetTyp("f1")
			assert.Equal(t, ok, true)
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,tk1="tv1" f1=1.1 1`)
	buf.WriteByte('\n')
	unmarshal(buf.Bytes(), callback)
}

// new EndTime Tag and Field
func TestPointsWriter_updateCleanSchemaIfNeeded2(t *testing.T) {
	meta2.InitSchemaCleanEn(true)
	defer meta2.InitSchemaCleanEn(false)
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.TSSTORE, 0)
	mi.Schema = &meta2.CleanSchema{
		"tk1": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
		"f1":  meta2.SchemaVal{Typ: influx.Field_Type_Float},
	}
	cs := mi.Schema

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		for _, item := range fieldToCreate {
			(*cs)[item.GetFieldName()] = meta2.SchemaVal{Typ: int8(item.GetFieldType()), EndTime: item.GetEndTime()}
		}
		return nil
	}
	mc.GetSgEndTimeFn = func(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
		return 1 << 32, nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}

		for _, r := range rows {
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)
			assert.NoError(t, err)
			v, _ := (*cs)["tk1"]
			assert.Equal(t, v.EndTime, int32(1))
			v, _ = (*cs)["f1"]
			assert.Equal(t, v.EndTime, int32(1))
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,tk1="tv1" f1=1.1 1`)
	buf.WriteByte('\n')
	unmarshal(buf.Bytes(), callback)
}

// tagkey errors: nameTime/duplicate tagkey
func TestPointsWriter_updateCleanSchemaIfNeeded3(t *testing.T) {
	meta2.InitSchemaCleanEn(true)
	defer meta2.InitSchemaCleanEn(false)
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.TSSTORE, 0)
	mi.Schema = &meta2.CleanSchema{}
	cs := mi.Schema

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		for _, item := range fieldToCreate {
			(*cs)[item.GetFieldName()] = meta2.SchemaVal{Typ: int8(item.GetFieldType()), EndTime: item.GetEndTime()}
		}
		return nil
	}
	mc.GetSgEndTimeFn = func(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
		return 1 << 32, nil
	}
	var errors = []string{
		"tag key can't be time, measurement is 'mst'",
		"duplicate tag tk1",
	}
	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}

		for i, r := range rows {
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)
			assert.Equal(t, err.Error(), errors[i])
			v, _ := (*cs)["f1"]
			assert.Equal(t, v.EndTime, int32(1))
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,time="tv1" f1=1.1 1`)
	buf.WriteByte('\n')
	buf.WriteString(`mst,tk1="tv1",tk1="tv1" f1=1.1 1`)
	buf.WriteByte('\n')
	unmarshal(buf.Bytes(), callback)
}

// field Type conflict
func TestPointsWriter_updateCleanSchemaIfNeeded4(t *testing.T) {
	meta2.InitSchemaCleanEn(true)
	defer meta2.InitSchemaCleanEn(false)
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.TSSTORE, 0)
	mi.Schema = &meta2.CleanSchema{
		"tk1": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
		"f1":  meta2.SchemaVal{Typ: influx.Field_Type_Int},
	}
	cs := mi.Schema

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		for _, item := range fieldToCreate {
			(*cs)[item.GetFieldName()] = meta2.SchemaVal{Typ: int8(item.GetFieldType()), EndTime: item.GetEndTime()}
		}
		return nil
	}
	mc.GetSgEndTimeFn = func(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
		return 1, nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}

		for _, r := range rows {
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)
			assert.Equal(t, err.Error(), "field type conflict: input field \"f1\" on measurement \"mst\" is type float, already exists as type integer")
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,tk1="tv1" f1=1.1 1`)
	buf.WriteByte('\n')
	unmarshal(buf.Bytes(), callback)
}

// colEngine + tag typ err
func TestPointsWriter_updateCleanSchemaIfNeeded5(t *testing.T) {
	meta2.InitSchemaCleanEn(true)
	defer meta2.InitSchemaCleanEn(false)
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.COLUMNSTORE, 0)
	mi.Schema = &meta2.CleanSchema{
		"tk1": meta2.SchemaVal{Typ: influx.Field_Type_Int},
	}
	cs := mi.Schema

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		for _, item := range fieldToCreate {
			(*cs)[item.GetFieldName()] = meta2.SchemaVal{Typ: int8(item.GetFieldType()), EndTime: item.GetEndTime()}
		}
		return nil
	}
	mc.GetSgEndTimeFn = func(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
		return 1, nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}

		for _, r := range rows {
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)
			assert.Equal(t, err.Error(), "column store write point has Invalid tag :tk1")
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,tk1="tv1" f1=1.1 1`)
	buf.WriteByte('\n')
	unmarshal(buf.Bytes(), callback)
}

// colEngine + tag primKey++ -> colCount != primKeyLen
func TestPointsWriter_updateCleanSchemaIfNeeded6(t *testing.T) {
	meta2.InitSchemaCleanEn(true)
	defer meta2.InitSchemaCleanEn(false)
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.COLUMNSTORE, 0)
	mi.Schema = &meta2.CleanSchema{
		"tk1": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
	}
	mi.ColStoreInfo = &meta2.ColStoreInfo{}
	cs := mi.Schema

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		for _, item := range fieldToCreate {
			(*cs)[item.GetFieldName()] = meta2.SchemaVal{Typ: int8(item.GetFieldType()), EndTime: item.GetEndTime()}
		}
		return nil
	}
	mc.GetSgEndTimeFn = func(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
		return 1, nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}
		for _, r := range rows {
			wh.mstPrimaryKeyRowMap = make(map[string]map[string]struct{})
			wh.mstPrimaryKeyRowMap[r.Name] = make(map[string]struct{})
			wh.mstPrimaryKeyRowMap[r.Name]["tk1"] = struct{}{}
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)
			assert.Equal(t, err.Error(), "checkSchema: write point is not match the number of primary key. mst: mst,  expect:0 but:1")
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,tk1="tv1" f1=1.1 1`)
	buf.WriteByte('\n')
	unmarshal(buf.Bytes(), callback)
}

// colEngine + field Typ err
func TestPointsWriter_updateCleanSchemaIfNeeded7(t *testing.T) {
	meta2.InitSchemaCleanEn(true)
	defer meta2.InitSchemaCleanEn(false)
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.COLUMNSTORE, 0)
	mi.Schema = &meta2.CleanSchema{
		"tk1": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
		"f1":  meta2.SchemaVal{Typ: influx.Field_Type_Tag},
	}
	mi.ColStoreInfo = &meta2.ColStoreInfo{}
	cs := mi.Schema

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		for _, item := range fieldToCreate {
			(*cs)[item.GetFieldName()] = meta2.SchemaVal{Typ: int8(item.GetFieldType()), EndTime: item.GetEndTime()}
		}
		return nil
	}
	mc.GetSgEndTimeFn = func(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
		return 1, nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}
		for _, r := range rows {
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)
			assert.Equal(t, err.Error(), "column store write point has Invalid field :f1")
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,tk1="tv1" f1=1.1 1`)
	buf.WriteByte('\n')
	unmarshal(buf.Bytes(), callback)
}

// colEngine + field primKey++ -> colCount != primKeyLen
func TestPointsWriter_updateCleanSchemaIfNeeded8(t *testing.T) {
	meta2.InitSchemaCleanEn(true)
	defer meta2.InitSchemaCleanEn(false)
	mstName := "mst_0000"
	mi := meta2.NewMeasurementInfo(mstName, influx.GetOriginMstName(mstName), config.COLUMNSTORE, 0)
	mi.Schema = &meta2.CleanSchema{
		"f1": meta2.SchemaVal{Typ: influx.Field_Type_Float},
	}
	mi.ColStoreInfo = &meta2.ColStoreInfo{}
	cs := mi.Schema

	fs := make([]*proto2.FieldSchema, 0, 8)

	mc := NewMockMetaClient()
	mc.UpdateSchemaFn = func(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error {
		for _, item := range fieldToCreate {
			(*cs)[item.GetFieldName()] = meta2.SchemaVal{Typ: int8(item.GetFieldType()), EndTime: item.GetEndTime()}
		}
		return nil
	}
	mc.GetSgEndTimeFn = func(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
		return 1, nil
	}

	pw := NewPointsWriter(time.Second)
	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	wh := newWriteHelper(pw)

	var callback = func(db string, rows []influx.Row, err error) {
		if !assert.NoError(t, err) {
			return
		}
		for _, r := range rows {
			wh.mstPrimaryKeyRowMap = make(map[string]map[string]struct{})
			wh.mstPrimaryKeyRowMap[r.Name] = make(map[string]struct{})
			wh.mstPrimaryKeyRowMap[r.Name]["f1"] = struct{}{}
			_, _, err := wh.updateSchemaIfNeeded("db0", "rp0", &r, mi, mi.OriginName(), fs)
			assert.Equal(t, err.Error(), "checkSchema: write point is not match the number of primary key. mst: mst,  expect:0 but:1")
		}
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(`mst,tk1="tv1" f1=1.1 1`)
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

func buildTagKeys() []string {
	//mock dii tagNum
	tagNum := 50
	tagKeys := make([]string, tagNum)
	for i := 0; i < tagNum; i++ {
		tagKeys[i] = fmt.Sprintf("tagNum%v", i)
	}
	return tagKeys
}

func buildRow() influx.Row {
	pt := influx.Row{}
	//mock dii tagNum
	tagNum := 50
	pt.Tags = make(influx.PointTags, tagNum)
	for i := 0; i < tagNum; i++ {
		kv := fmt.Sprintf("tagNum%v", i)
		pt.Tags[i].Key = kv
		pt.Tags[i].Value = kv
	}
	sort.Sort(&pt.Tags)
	return pt
}

func generateRows(num int, rows []influx.Row) []influx.Row {
	tmpKeys := []string{
		"mst0,tk1=value1,tk2=value2,tk3=value3",
		"mst0,tk1=value11,tk2=value22,tk3=value33",
		"mst0,tk1=value12,tk2=value23",
		"mst0,tk1=value12,tk2=value23",
		"mst0,tk1=value12,tk2=value23",
	}
	keys := make([]string, num)
	for i := 0; i < num; i++ {
		keys[i] = tmpKeys[i%len(tmpKeys)]
	}
	rows = rows[:cap(rows)]
	for j, key := range keys {
		if cap(rows) <= j {
			rows = append(rows, influx.Row{})
		}
		pt := &rows[j]
		strs := strings.Split(key, ",")
		pt.Name = strs[0]
		pt.Tags = pt.Tags[:cap(pt.Tags)]
		for i, str := range strs[1:] {
			if cap(pt.Tags) <= i {
				pt.Tags = append(pt.Tags, influx.Tag{})
			}
			kv := strings.Split(str, "=")
			pt.Tags[i].Key = kv[0]
			pt.Tags[i].Value = kv[1]
		}
		pt.Tags = pt.Tags[:len(strs[1:])]
		sort.Sort(&pt.Tags)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pt.Fields = pt.Fields[:cap(pt.Fields)]
		if cap(pt.Fields) < 1 {
			pt.Fields = append(pt.Fields, influx.Field{}, influx.Field{})
		}
		pt.Fields[0].NumValue = 1
		pt.Fields[0].StrValue = ""
		pt.Fields[0].Type = influx.Field_Type_Float
		pt.Fields[0].Key = "fk1"
		pt.Fields[1].NumValue = 1
		pt.Fields[1].StrValue = ""
		pt.Fields[1].Type = influx.Field_Type_Int
		pt.Fields[1].Key = "fk2"
	}
	return rows[:num]
}

func TestCheckFields_Conflict(t *testing.T) {
	fields := influx.Fields{
		{
			Key:  "foo",
			Type: influx.Field_Type_Float,
		},
		{
			Key:  "foo",
			Type: influx.Field_Type_Int,
		},
		{
			Key:  "time",
			Type: influx.Field_Type_Int,
		},
		{
			Key:  "time",
			Type: influx.Field_Type_Int,
		},
		{
			Key:  "zing",
			Type: influx.Field_Type_String,
		},
	}
	fields, err := fixFields(fields)
	assert.EqualError(t, err, "conflict field type: foo")
	assert.Empty(t, nil, fields)

}

func TestCheckFields_Ignore(t *testing.T) {
	fields := influx.Fields{
		{
			Key:      "foo",
			Type:     influx.Field_Type_Float,
			NumValue: 1,
		},
		{
			Key:      "foo",
			Type:     influx.Field_Type_Float,
			NumValue: 2,
		},
		{
			Key:  "zing",
			Type: influx.Field_Type_String,
		},
	}
	fields, err := fixFields(fields)

	assert.NoError(t, err)
	assert.Equal(t, influx.Fields{
		{
			Key:      "foo",
			Type:     influx.Field_Type_Float,
			NumValue: 2,
		},
		{
			Key:  "zing",
			Type: influx.Field_Type_String,
		},
	}, fields)

}

func TestStreamSymbolMarshalUnmarshal(t *testing.T) {
	var binary []byte
	var err error
	wRows := make([]influx.Row, 10)
	sRows := generateRows(10, wRows)
	sRows[0].StreamOnly = true
	var streamIDs []uint64
	streamIDs = append(streamIDs, 3)
	sRows[0].StreamId = streamIDs
	b, err := influx.FastMarshalMultiRows(binary, sRows)
	if err != nil {
		t.Fatal(err)
	}

	var rows []influx.Row
	var tagPools []influx.Tag
	var fieldPools []influx.Field
	var indexKeyPools []byte
	var indexOptionPools []influx.IndexOption

	rows, _, _, _, _, err = influx.FastUnmarshalMultiRows(b, rows, tagPools, fieldPools, indexOptionPools, indexKeyPools)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].StreamOnly != false {
		t.Error(fmt.Sprintf("expect %v ,got %v", sRows[0].StreamOnly, rows[0].StreamOnly))
	}
	if len(rows[0].StreamId) != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", rows[0].StreamId[0], streamIDs[0]))
	}
}

func TestPointsWriter_xx(t *testing.T) {
	xx := &[]influx.Row{}
	*xx = append(*xx, influx.Row{})
	*xx = append(*xx, influx.Row{})
	*xx = append(*xx, influx.Row{})
	*xx = (*xx)[:cap(*xx)]
	t.Log(*xx)
}

func TestSelectIndexList(t *testing.T) {
	size := 1
	columnToIndex := map[string]int{}
	indexList := make([]string, size)
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("name%v", i)
		columnToIndex[key] = i
		indexList[i] = key
	}
	index, ok := selectIndexList(columnToIndex, indexList)
	if !assert.Equal(t, index, []uint16{0}) {
		t.Fatal("SelectIndexList failed")
	}
	if !assert.Equal(t, ok, true) {
		t.Fatal("SelectIndexList failed")
	}
}

func TestPointsWriter_WritePointRows_SameShardForStream(t *testing.T) {
	streamDistribution = sameShard
	pw := NewPointsWriter(time.Second * 10)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)
	err := pw.writePointRows("db0", "rp0", generateRows(10, rows))
	if err != nil {
		t.Fatal(err)
	}
}

func TestPointsWriter_WritePointRows_NoStream_NoFieldIndex(t *testing.T) {
	streamDistribution = noStream
	enableFieldIndex = false
	pw := NewPointsWriter(time.Second * 10)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)
	err := pw.writePointRows("db0", "rp0", generateRows(10, rows))
	if err != nil {
		t.Fatal(err)
	}
}

func TestPointsWriter_WritePointRows_SameNodeForStream(t *testing.T) {
	streamDistribution = sameNode
	pw := NewPointsWriter(time.Second * 10)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)
	err := pw.writePointRows("db0", "rp0", generateRows(10, rows))
	if err != nil {
		t.Fatal(err)
	}
}

func TestPointsWriter_WritePointRows_SameMstForStream(t *testing.T) {
	streamDistribution = sameMst
	pw := NewPointsWriter(time.Second * 10)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)
	err := pw.writePointRows("db0", "rp0", generateRows(10, rows))
	if err != nil {
		t.Fatal(err)
	}
}

func TestPointsWriter_WritePointRows_CalculateOnSqlForStream(t *testing.T) {
	streamDistribution = diffDis
	pw := NewPointsWriter(time.Second * 10)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)
	err := pw.writePointRows("db0", "rp0", generateRows(10, rows))
	if err != nil {
		t.Fatal(err)
	}
}

func TestPointsWriter_TimeRangeLimit(t *testing.T) {
	streamDistribution = diffDis
	pw := NewPointsWriter(time.Second * 10)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)

	pw.ApplyTimeRangeLimit(nil)
	pw.ApplyTimeRangeLimit([]toml.Duration{0, 0})
	go pw.ApplyTimeRangeLimit([]toml.Duration{toml.Duration(time.Hour * 24), toml.Duration(time.Hour * 24)})

	time.Sleep(time.Second / 10)
	rows = generateRows(10, rows)
	rows[0].Timestamp = time.Now().Add(-time.Hour * 30).UnixNano()

	err := pw.writePointRows("db0", "rp0", rows)
	pw.Close()

	exp := "partial write: " + errno.NewError(errno.WritePointOutOfRP).Error() + " dropped=1"
	assert.EqualError(t, err, exp)
}

func TestPointsWriter_WritePointRows_DuplicateFields_TypeConflict(t *testing.T) {
	streamDistribution = diffDis
	pw := NewPointsWriter(time.Second * 10)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	// just one row for duplicateFields
	rows := []influx.Row{
		{
			Name: "mst_0000",
			Tags: []influx.Tag{
				{
					Key:   "t1",
					Value: "v1",
				},
			},
			Fields: influx.Fields{
				{
					Key:      "foo",
					Type:     influx.Field_Type_Float,
					NumValue: 1,
				},
				{
					Key:      "foo",
					Type:     influx.Field_Type_String,
					StrValue: "str",
				},
			},
			Timestamp: time.Now().UnixNano(),
		},
	}
	err := pw.writePointRows("db0", "rp0", rows)
	require.EqualError(t, err, "partial write: conflict field type: foo dropped=1")
}

func TestPointsWriter_WritePointRows_DuplicateFields2(t *testing.T) {
	streamDistribution = diffDis
	pw := NewPointsWriter(time.Second * 10)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	// two rows:
	// 1. duplicateFields
	// 2. normal row
	rows := []influx.Row{
		{
			Name: "mst_0000",
			Fields: influx.Fields{
				{
					Key:      "foo",
					Type:     influx.Field_Type_Float,
					NumValue: 1,
				},
				{
					Key:      "foo",
					Type:     influx.Field_Type_String,
					NumValue: 2,
				},
			},
			Timestamp: time.Now().UnixNano(),
		},
		{
			Name: "mst_0000",
			Fields: influx.Fields{
				{
					Key:  "foo",
					Type: influx.Field_Type_String,
				},
			},
			Timestamp: time.Now().UnixNano(),
		},
	}
	err := pw.writePointRows("db0", "rp0", rows)
	require.EqualError(t, err, "partial write: conflict field type: foo dropped=1")
}

func TestColumnToIndexUpdate(t *testing.T) {
	var k1Ptr, k2Ptr *string
	b1 := []byte{'a', 'b', 'c'}
	b2 := []byte{'a', 'b', 'c'}
	m := make(map[string]int)
	m[util.Bytes2str(b1)] = 0
	for k1 := range m {
		k1Ptr = &k1
	}
	m[util.Bytes2str(b2)] = 1
	for k2 := range m {
		k2Ptr = &k2
	}
	if k1Ptr == k2Ptr {
		t.Fatal("columnToIndex failed")
	}
}

func Benchmark_UnmarshalShardKey(t *testing.B) {
	t.N = 10
	row := buildRow()
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for j := 0; j < 1000000; j++ {
			err := row.UnmarshalShardKeyByTag([]string{})
			if err != nil {
				t.Fatal("UnmarshalShardKeyByTag fail")
			}
		}
		t.StopTimer()
	}
}

func Benchmark_UnmarshalShardKeyByTagOp(t *testing.B) {
	t.N = 10
	row := buildRow()

	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for j := 0; j < 1000000; j++ {
			err := row.UnmarshalShardKeyByTagOp([]string{})
			if err != nil {
				t.Fatal("UnmarshalShardKeyByTagOp fail")
			}
		}
		t.StopTimer()
	}
}

func Benchmark_UnmarshalShardKey_WithDims(t *testing.B) {
	t.N = 10
	tagKeys := buildTagKeys()
	row := buildRow()
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for j := 0; j < 1000000; j++ {
			err := row.UnmarshalShardKeyByTag(tagKeys)
			if err != nil {
				t.Fatal("UnmarshalShardKey_WithDims fail")
			}
		}
		t.StopTimer()
	}
}

func Benchmark_UnmarshalShardKeyByTagOp_WithDims(t *testing.B) {
	t.N = 10
	tagKeys := buildTagKeys()
	row := buildRow()
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for j := 0; j < 1000000; j++ {
			err := row.UnmarshalShardKeyByTagOp(tagKeys)
			if err != nil {
				t.Fatal("UnmarshalShardKeyByTagOp_WithDims fail")
			}
		}
		t.StopTimer()
	}
}

func Benchmark_selectIndexList(t *testing.B) {
	t.N = 10
	columnToIndex := map[string]int{}
	size := 1
	indexList := make([]string, size)
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("name%v", i)
		columnToIndex[key] = i
		indexList[i] = key
	}
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		for j := 0; j < 10000000; j++ {
			selectIndexList(columnToIndex, indexList)
		}
		t.Log("cost")
		t.StopTimer()
	}
}

func Benchmark_WritePointRows(t *testing.B) {
	t.N = 10
	now := time.Now()
	pw := NewPointsWriter(time.Second * 10)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	t.ReportAllocs()
	t.ResetTimer()
	tt := time.Now()
	t.Log("init cost", tt.Sub(now))
	now = tt
	rows := make([]influx.Row, 100000)
	for i := 0; i < t.N; i++ {
		generateRows(100000, rows)
		t.StartTimer()
		err := pw.writePointRows("db0", "rp0", rows)
		if err != nil {
			t.Fatal(err)
		}
		tt = time.Now()
		t.Log("write cost", tt.Sub(now))
		now = tt
		t.StopTimer()
	}
}

func TestPointsWriter_InvalidMst(t *testing.T) {
	streamDistribution = noStream
	pw := NewPointsWriter(time.Second * 10)
	mc := NewMockMetaClient()
	mc.MeasurementFn = func(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
		return nil, errno.NewError(errno.InvalidMeasurement, "a/a")
	}

	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)

	rows = generateRows(5, rows)
	err := pw.writePointRows("db0", "rp0", rows)
	pw.Close()

	exp := "partial write: " + errno.NewError(errno.InvalidMeasurement, "a/a").Error() + " dropped=5"
	assert.EqualError(t, err, exp)
}

func TestPointsWriter_TagLimit(t *testing.T) {
	streamDistribution = noStream
	pw := NewPointsWriter(time.Second * 10)
	mc := NewMockMetaClient()
	mc.MeasurementFn = func(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
		mst := NewMeasurement("mst", config.TSSTORE)
		binary, err := mst.MarshalBinary()
		if err != nil {
			return nil, err
		}
		err = mst.UnmarshalBinary(binary)
		return mst, err
	}

	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)

	SetTagLimit(1)
	rows = generateRows(5, rows)
	err := pw.writePointRows("db0", "rp0", rows)
	pw.Close()

	exp := "partial write: " + errno.NewError(errno.TooManyTagKeys).Error() + " dropped=2"
	assert.EqualError(t, err, exp)
}

func TestPointsWriter_LackOfShardKey(t *testing.T) {
	streamDistribution = noStream
	pw := NewPointsWriter(time.Second * 10)
	mc := NewMockMetaClient()
	mc.MeasurementFn = func(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
		mst := NewMeasurement("mst", config.COLUMNSTORE)
		binary, err := mst.MarshalBinary()
		if err != nil {
			return nil, err
		}
		err = mst.UnmarshalBinary(binary)
		return mst, err
	}

	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 1)
	rows = generateRows(1, rows)
	key := rows[0].Tags.FindPointTag("tk1")
	key.Key = "tk10"
	ctx := getInjestionCtx()
	defer putInjestionCtx(ctx)
	ctx.writeHelper = newWriteHelper(pw)
	ctx.ms = &meta2.MeasurementInfo{EngineType: config.COLUMNSTORE}
	shardKey := meta2.ShardKeyInfo{ShardKey: []string{"tk1"}}
	ctx.db = &meta2.DatabaseInfo{ShardKey: shardKey}
	err, _, pErr := pw.updateShardGroupAndShardKey("db0", "rp0", &rows[0], ctx, false, nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	pw.Close()
	exp := "point should have all shard key"
	assert.EqualError(t, pErr, exp)
}

func TestResetRowsRouter(t *testing.T) {
	pw := NewPointsWriter(time.Second)
	var rows []influx.Row
	rows = append(rows, influx.Row{ShardKey: bytesutil.ToUnsafeBytes("tag1")})
	pw.resetRowsRouter(rows)
	assert.Equal(t, 0, len(rows[0].ShardKey))
}

func TestPointsWriter_WritePointRows_TimeOutsideRange(t *testing.T) {
	streamDistribution = sameShard
	pw := NewPointsWriter(time.Second * 10)
	pw.MetaClient = NewMockMetaClient()
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)
	rows = generateRows(10, rows)
	for i := range rows {
		rows[i].Timestamp = 9223372036854775807
	}
	err := pw.writePointRows("db0", "rp0", rows)
	if err == nil {
		t.Fatal(err)
	}
}

func TestPointsWriter_TagLimit_CleanSchemOpen(t *testing.T) {
	meta2.SchemaCleanEn = true
	defer func() {
		meta2.SchemaCleanEn = false
	}()
	streamDistribution = noStream
	pw := NewPointsWriter(time.Second * 10)
	mc := NewMockMetaClient()
	mc.MeasurementFn = func(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
		mst := NewMeasurement("mst", config.TSSTORE)
		binary, err := mst.MarshalBinary()
		if err != nil {
			return nil, err
		}
		err = mst.UnmarshalBinary(binary)
		return mst, err
	}
	mc.GetSgEndTimeFn = func(database string, rp string, timestamp time.Time, engineType config.EngineType) (int64, error) {
		return 1 << 32, nil
	}

	pw.MetaClient = mc
	pw.TSDBStore = NewMockNetStore()
	rows := make([]influx.Row, 10)

	SetTagLimit(1)
	rows = generateRows(5, rows)
	err := pw.writePointRows("db0", "rp0", rows)
	pw.Close()

	exp := "partial write: " + errno.NewError(errno.TooManyTagKeys).Error() + " dropped=5"
	assert.EqualError(t, err, exp)
}

func TestName(t *testing.T) {
	localStore := &MockLocalStore{}

	pw := NewPointsWriter(time.Second * 10)
	pw.SetStore(localStore)
	pw.MetaClient = &MockMetaClient{
		DBPtViewFn: func(database string) (meta2.DBPtInfos, error) {
			return meta2.DBPtInfos{
				meta2.PtInfo{
					Owner:  meta2.PtOwner{},
					Status: 0,
					PtId:   0,
					Ver:    0,
					RGID:   0,
				},
			}, nil
		},
	}

	ctx := &netstorage.WriteContext{}
	ctx.Shard = &meta2.ShardInfo{
		Owners: []uint32{0},
	}
	ctx.Rows = append(ctx.Rows, influx.Row{
		Timestamp: 100,
		Name:      "mst",
		Tags: influx.PointTags{
			influx.Tag{
				Key:   "tid",
				Value: "t1",
			},
		},
		Fields: influx.Fields{
			influx.Field{
				Key:      "value",
				NumValue: 1,
				StrValue: "",
				Type:     influx.Field_Type_Float,
			},
		},
	})

	require.NoError(t, pw.writeRowToShard(ctx, "db", "rp"))

	localStore.err = errors.New("some error")
	require.EqualError(t, pw.writeRowToShard(ctx, "db", "rp"), localStore.err.Error())
}

type MockLocalStore struct {
	err error
}

func (s *MockLocalStore) WriteRows(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte) error {
	return s.err
}

func TestPointsWriter_routeAndMapOriginRows_MapToStreamMstFail(t *testing.T) {
	ctx := &injestionCtx{
		streamInfos: []*meta2.StreamInfo{{Name: "test_stream"}},
		streamDstMstNames: map[string]*meta2.StreamInfo{
			"dst_mst": {Name: "test_stream", Cond: "a > 0"},
		},
	}
	rows := []influx.Row{
		{Name: "dst_mst"},
	}
	writer := &PointsWriter{
		logger: logger.NewLogger(errno.NodeSql).SetZapLogger(zap.NewNop()),
	}
	partialErr, dropped, err := writer.routeAndMapOriginRows("db", "rp", rows, ctx)
	require.Equal(t, 1, dropped)
	require.EqualError(t, partialErr, "the stream destination measurement cannot be written. measurement name is dst_mst")
	require.NoError(t, err)
}
