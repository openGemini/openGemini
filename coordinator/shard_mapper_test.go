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
	"context"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	set "github.com/deckarep/golang-set"
	"github.com/influxdata/influxdb/models"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetriedErrorCode(t *testing.T) {
	r := IsRetriedError(errno.NewError(errno.PtNotFound))
	assert.Equal(t, true, r)
	r = IsRetriedError(errno.NewError(errno.DBPTClosed))
	assert.Equal(t, true, r)
	r = IsRetriedError(errno.NewError(errno.RpNotFound))
	assert.Equal(t, false, r)
}

type mocShardMapperMetaClient struct {
	databases map[string]*meta.DatabaseInfo
	cacheData *meta.Data
	metaclient.MetaClient
}

func (m mocShardMapperMetaClient) GetMaxCQChangeID() uint64 {
	return 0
}

func (m mocShardMapperMetaClient) ThermalShards(db string, start, end time.Duration) map[uint64]struct{} {
	//TODO implement me
	panic("implement me")
}

func (m mocShardMapperMetaClient) GetNodePtsMap(database string) (map[uint64][]uint32, error) {
	panic("implement me")
}

func (m mocShardMapperMetaClient) GetStreamInfos() map[string]*meta.StreamInfo {
	return nil
}

func (m mocShardMapperMetaClient) UpdateStreamMstSchema(database string, retentionPolicy string, mst string, stmt *influxql.SelectStatement) error {
	return nil
}

func (m mocShardMapperMetaClient) CreateStreamPolicy(info *meta.StreamInfo) error {
	return nil
}

func (m mocShardMapperMetaClient) ShowStreams(database string, showAll bool) (models.Rows, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) DropStream(name string) error {
	return nil
}

func (m mocShardMapperMetaClient) CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta.ShardKeyInfo, numOfShards int32, indexR *influxql.IndexRelation,
	engineType config.EngineType, colStoreInfo *meta.ColStoreInfo, schemaInfo []*proto.FieldSchema, options *meta2.Options) (*meta.MeasurementInfo, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) AlterShardKey(database, retentionPolicy, mst string, shardKey *meta.ShardKeyInfo) error {
	return nil
}

func (m mocShardMapperMetaClient) CreateDatabase(name string, enableTagArray bool, replicaN uint32, options *obs.ObsOptions) (*meta.DatabaseInfo, error) {
	return m.databases[name], nil
}

func (m mocShardMapperMetaClient) CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec, shardKey *meta.ShardKeyInfo, enableTagArray bool, replicaN uint32) (*meta.DatabaseInfo, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error) {
	return nil, nil
}
func (m mocShardMapperMetaClient) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return nil
}

func (m mocShardMapperMetaClient) CreateUser(name, password string, admin, rwuser bool) (meta.User, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) Databases() map[string]*meta.DatabaseInfo {
	return nil
}

func (m mocShardMapperMetaClient) Database(name string) (*meta.DatabaseInfo, error) {
	return m.databases[name], nil
}

func (m mocShardMapperMetaClient) DataNode(id uint64) (*meta.DataNode, error) {
	for i := range m.cacheData.DataNodes {
		if m.cacheData.DataNodes[i].ID == id {
			return &m.cacheData.DataNodes[i], nil
		}
	}
	return nil, meta.ErrNodeNotFound
}

func (m mocShardMapperMetaClient) DataNodes() ([]meta.DataNode, error) {
	nodes := m.cacheData.DataNodes
	return nodes, nil
}

func (m mocShardMapperMetaClient) DeleteDataNode(id uint64) error {
	return nil
}

func (m mocShardMapperMetaClient) AliveReadNodes() ([]meta2.DataNode, error) {
	var aliveReaders []meta2.DataNode
	for _, n := range m.cacheData.DataNodes {
		if n.Role == meta2.NodeReader {
			aliveReaders = append(aliveReaders, n)
		}
	}

	if len(aliveReaders) == 0 {
		return nil, fmt.Errorf("there is no data nodes for querying")
	}

	sort.Sort(meta2.DataNodeInfos(aliveReaders))
	return aliveReaders, nil
}

func (m mocShardMapperMetaClient) DeleteMetaNode(id uint64) error {
	return nil
}

func (m mocShardMapperMetaClient) DropShard(id uint64) error {
	return nil
}

func (m mocShardMapperMetaClient) DropDatabase(name string) error {
	return nil
}

func (m mocShardMapperMetaClient) DropRetentionPolicy(database, name string) error {
	return nil
}

func (m mocShardMapperMetaClient) DropSubscription(database, rp, name string) error {
	return nil
}

func (m mocShardMapperMetaClient) DropUser(name string) error {
	return nil
}

func (m mocShardMapperMetaClient) MetaNodes() ([]meta.NodeInfo, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) SetAdminPrivilege(username string, admin bool) error {
	return nil
}

func (m mocShardMapperMetaClient) SetPrivilege(username, database string, p originql.Privilege) error {
	return nil
}

func (m mocShardMapperMetaClient) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
	db := m.databases[database]
	if db == nil {
		return nil, nil
	}
	rp := db.RetentionPolicies[policy]
	if rp == nil {
		return nil, nil
	}
	groups := make([]meta.ShardGroupInfo, 0, len(rp.ShardGroups))
	for _, g := range rp.ShardGroups {
		if g.Deleted() || !g.Overlaps(min, max) {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

func (m mocShardMapperMetaClient) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error {
	return nil
}

func (m mocShardMapperMetaClient) UpdateUser(name, password string) error {
	return nil
}

func (m mocShardMapperMetaClient) UserPrivilege(username, database string) (*originql.Privilege, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) UserPrivileges(username string) (map[string]originql.Privilege, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) Users() []meta.UserInfo {
	return nil
}

func (m mocShardMapperMetaClient) MarkDatabaseDelete(name string) error {
	return nil
}

func (m mocShardMapperMetaClient) MarkRetentionPolicyDelete(database, name string) error {
	return nil
}

func (m mocShardMapperMetaClient) MarkMeasurementDelete(database, policy, measurement string) error {
	return nil
}

func (m mocShardMapperMetaClient) DBPtView(database string) (meta.DBPtInfos, error) {
	pts := m.cacheData.DBPtView(database)
	if pts == nil {
		return nil, errno.NewError(errno.DatabaseNotFound, database)
	}

	return pts, nil
}

func (m mocShardMapperMetaClient) DBRepGroups(database string) []meta.ReplicaGroup {
	return nil
}

func (m mocShardMapperMetaClient) GetReplicaN(database string) (int, error) {
	return 1, nil
}

func (m mocShardMapperMetaClient) ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo) {
	return
}

func (m mocShardMapperMetaClient) Measurement(database string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
	db, ok := m.databases[database]
	if !ok {
		return nil, fmt.Errorf("not found the database: %s", database)
	}

	rp, ok := db.RetentionPolicies[rpName]
	if !ok {
		return nil, fmt.Errorf("not found the retention policies: %s", rpName)
	}

	mst, ok := rp.Measurements[mstName]
	if !ok {
		return nil, fmt.Errorf("not found the measurement: %s", mstName)
	}
	return mst, nil
}

func (m mocShardMapperMetaClient) Schema(database string, retentionPolicy string, mst string) (fields map[string]int32, dimensions map[string]struct{}, err error) {
	fields = make(map[string]int32)
	dimensions = make(map[string]struct{})
	msti, err := m.Measurement(database, retentionPolicy, mst)
	if err != nil {
		return nil, nil, err
	}
	callback := func(k string, v int32) {
		if v == influx.Field_Type_Tag {
			dimensions[k] = struct{}{}
		} else {
			fields[k] = v
		}
	}
	msti.Schema.RangeTypCall(callback)
	return fields, dimensions, nil
}

func (m mocShardMapperMetaClient) GetMeasurements(mst *influxql.Measurement) ([]*meta.MeasurementInfo, error) {
	var measurements []*meta.MeasurementInfo
	db := m.databases[mst.Database]
	if db == nil {
		return nil, nil
	}
	rpi, err := db.GetRetentionPolicy(mst.RetentionPolicy)
	if err != nil {
		return nil, err
	}
	if mst.Regex != nil {
		rpi.EachMeasurements(func(msti *meta.MeasurementInfo) {
			if mst.Regex.Val.Match([]byte(msti.Name)) {
				measurements = append(measurements, msti)
			}
		})
		sort.Slice(measurements, func(i, j int) bool {
			return influx.GetOriginMstName(measurements[i].Name) < influx.GetOriginMstName(measurements[j].Name)
		})
	} else {
		rp := db.RetentionPolicies[mst.RetentionPolicy]
		if rp == nil {
			return nil, nil
		}
		if name, ok := rp.Measurements[mst.Name]; ok {
			measurements = append(measurements, name)
		}
	}
	if len(measurements) == 0 {
		return nil, errno.NewError(errno.ErrMeasurementNotFound)
	}
	return measurements, nil
}

func (m mocShardMapperMetaClient) TagKeys(database string) map[string]set.Set {
	return nil
}

func (m mocShardMapperMetaClient) FieldKeys(database string, ms influxql.Measurements) (map[string]map[string]int32, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) QueryTagKeys(database string, ms influxql.Measurements, cond influxql.Expr) (map[string]map[string]struct{}, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) MatchMeasurements(database string, ms influxql.Measurements) (map[string]*meta.MeasurementInfo, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) Measurements(database string, ms influxql.Measurements) ([]string, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) ShowShards(db string, rp string, mst string) models.Rows {
	return nil
}

func (m mocShardMapperMetaClient) ShowShardGroups() models.Rows {
	return nil
}

func (m mocShardMapperMetaClient) ShowSubscriptions() models.Rows {
	return nil
}

func (m mocShardMapperMetaClient) ShowRetentionPolicies(database string) (models.Rows, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) ShowCluster(nodeType string, ID uint64) (models.Rows, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) ShowClusterWithCondition(nodeType string, ID uint64) (models.Rows, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) ShowContinuousQueries() (models.Rows, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) GetAliveShards(database string, sgi *meta.ShardGroupInfo) []int {
	aliveShardIdxes := make([]int, 0, len(sgi.Shards))
	for i := range sgi.Shards {
		aliveShardIdxes = append(aliveShardIdxes, i)
	}
	return aliveShardIdxes
}

func (m mocShardMapperMetaClient) NewDownSamplePolicy(database, name string, info *meta.DownSamplePolicyInfo) error {
	return nil
}

func (m mocShardMapperMetaClient) DropDownSamplePolicy(database, name string, dropAll bool) error {
	return nil
}

func (m mocShardMapperMetaClient) ShowDownSamplePolicies(database string) (models.Rows, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) GetMstInfoWithInRp(dbName, rpName string, dataTypes []int64) (*meta.RpMeasurementsFieldsInfo, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) AdminUserExists() bool {
	return true
}

func (m mocShardMapperMetaClient) Authenticate(username, password string) (u meta.User, e error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) UpdateUserInfo() {
	return
}

func (m mocShardMapperMetaClient) UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error {
	return nil
}

func (m mocShardMapperMetaClient) OpenAtStore() error {
	return nil
}

func (mmc *mocShardMapperMetaClient) GetAllMst(dbName string) []string {
	return nil
}

func (m mocShardMapperMetaClient) RetryRegisterQueryIDOffset(host string) (uint64, error) {
	return 0, nil
}

func TestMapMstShards(t *testing.T) {
	timeStart := time.Date(2022, 1, 0, 0, 0, 0, 0, time.UTC)
	timeMid := time.Date(2022, 1, 15, 0, 0, 0, 0, time.UTC)
	timeEnd := time.Date(2022, 2, 0, 0, 0, 0, 0, time.UTC)
	timeMid1 := time.Date(2022, 2, 1, 0, 0, 0, 0, time.UTC)
	timeEnd1 := time.Date(2022, 2, 16, 0, 0, 0, 0, time.UTC)
	shards1 := []meta.ShardInfo{{ID: 1, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	shards2 := []meta.ShardInfo{{ID: 2, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	shards3 := []meta.ShardInfo{{ID: 3, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	csm := &ClusterShardMapper{
		Logger: logger.NewLogger(1),
	}
	csm.MetaClient = &mocShardMapperMetaClient{
		databases: map[string]*meta.DatabaseInfo{
			"db0": {
				Name:                   "db0",
				DefaultRetentionPolicy: "rp0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp0": {
						Name: "rp0",
						Measurements: map[string]*meta.MeasurementInfo{
							"mst1": {
								Name: "mst1",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"1", "2"},
										Type:       "hash",
										ShardGroup: 1,
									},
								},
								EngineType: config.COLUMNSTORE,
							},
							"mst2": {
								Name: "mst2",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"1", "2"},
										Type:       "hash",
										ShardGroup: 2,
									},
									{
										ShardKey:   []string{"3", "4"},
										Type:       "hash",
										ShardGroup: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID:         1,
								StartTime:  timeStart,
								EndTime:    timeMid,
								Shards:     shards1,
								EngineType: config.COLUMNSTORE,
							},
							{
								ID:         2,
								StartTime:  timeMid,
								EndTime:    timeEnd,
								Shards:     shards2,
								EngineType: config.TSSTORE,
							},
							{
								ID:         3,
								StartTime:  timeMid1,
								EndTime:    timeEnd1,
								Shards:     shards3,
								EngineType: config.TSSTORE,
							},
						},
					},
				},
				ShardKey: meta.ShardKeyInfo{
					ShardKey:   []string{"1", "2", "3"},
					Type:       "hash",
					ShardGroup: 3,
				},
			},
		},
	}
	opt := &query.SelectOptions{}
	source := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst1", EngineType: config.COLUMNSTORE,
	}
	join := &influxql.Join{
		LSrc:      source,
		RSrc:      source,
		Condition: &influxql.BinaryExpr{},
	}
	shardMapping := &ClusterShardMapping{
		ShardMap:  map[Source]map[uint32][]executor.ShardInfo{},
		seriesKey: make([]byte, 0),
	}
	seriesKey := make([]byte, 0)
	csm.mapShards(shardMapping, []influxql.Source{join}, timeStart, timeEnd, nil, opt)
	Val, _ := regexp.Compile("")
	sourceRegex := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "", EngineType: config.COLUMNSTORE,
		Regex: &influxql.RegexLiteral{Val: Val},
	}
	seriesKey = seriesKey[:0]
	csm.mapMstShards(sourceRegex, shardMapping, timeStart, timeEnd, nil, opt)
	source1 := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst2", EngineType: config.TSSTORE,
	}
	shardMapping1 := &ClusterShardMapping{
		ShardMap:  map[Source]map[uint32][]executor.ShardInfo{},
		seriesKey: make([]byte, 0),
	}
	seriesKey = seriesKey[:0]
	csm.mapMstShards(source1, shardMapping1, timeStart, timeEnd1, nil, opt)

	shardMapping2 := &ClusterShardMapping{
		ShardMap:  map[Source]map[uint32][]executor.ShardInfo{},
		seriesKey: make([]byte, 0),
	}
	subquery := &influxql.SubQuery{
		Statement: &influxql.SelectStatement{
			Sources: []influxql.Source{source1},
		},
	}
	opt1 := &query.SelectOptions{
		HintType: hybridqp.FullSeriesQuery,
	}
	seriesKey = seriesKey[:0]
	csm.mapShards(shardMapping2, []influxql.Source{subquery}, timeStart, timeEnd, nil, opt1)
}

func TestShardMapperExprRewriter(t *testing.T) {
	fields := make(map[string]*influxql.FieldNameSpace)
	fields["mst.f1"] = &influxql.FieldNameSpace{
		RealName: "f1",
		DataType: influxql.Unknown,
	}
	fields["f2"] = &influxql.FieldNameSpace{DataType: influxql.Unknown}
	fields["f3"] = &influxql.FieldNameSpace{DataType: influxql.Unknown}
	shardMapperExprRewriter(true, true, fields, "f1", "mst.f1", 1)
	shardMapperExprRewriter(true, false, fields, "f2", "f2", 1)
	shardMapperExprRewriter(false, false, fields, "f3", "f3", 1)
	if fields["mst.f1"].DataType != influxql.Integer {
		t.Fatal()
	}
	if fields["f2"].DataType != influxql.Integer {
		t.Fatal()
	}
	if fields["f3"].DataType != influxql.Tag {
		t.Fatal()
	}
}

func TestShardMappingGetSeriesKey(t *testing.T) {
	tr := influxql.TimeRange{}
	tmin := time.Unix(0, tr.MinTimeNano())
	tmax := time.Unix(0, tr.MaxTimeNano())
	csm := ClusterShardMapper{}
	csming := NewClusterShardMapping(&csm, tmin, tmax)
	if len(csming.GetSeriesKey()) != 0 {
		t.Error("csming getSeriesKey error")
	}
}

func TestShardMappingGetSources(t *testing.T) {
	tr := influxql.TimeRange{}
	tmin := time.Unix(0, tr.MinTimeNano())
	tmax := time.Unix(0, tr.MaxTimeNano())
	csm := ClusterShardMapper{}
	csm.MetaClient = &mocShardMapperMetaClient{
		databases: map[string]*meta.DatabaseInfo{
			"db0": {
				Name:                   "db0",
				DefaultRetentionPolicy: "rp0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp0": {
						Name: "rp0",
						Measurements: map[string]*meta.MeasurementInfo{
							"mst1": meta.NewMeasurementInfo("mst1_000", influx.GetOriginMstName("mst1_000"), config.TSSTORE, 0),
						},
					},
				},
			},
		},
	}
	csming := NewClusterShardMapping(&csm, tmin, tmax)
	sources := make(influxql.Sources, 0)
	sources = append(sources, &influxql.Measurement{
		Database:        "db0",
		RetentionPolicy: "rp0",
		Name:            "mst1",
		EngineType:      config.TSSTORE,
	})
	if (csming.GetSources(sources))[0].GetName() != "mst1_000" {
		t.Error("csming getSources error")
	}
}
func Test_FieldDimensions(t *testing.T) {
	timeStart := time.Date(2022, 1, 0, 0, 0, 0, 0, time.UTC)
	timeMid := time.Date(2022, 1, 15, 0, 0, 0, 0, time.UTC)
	timeEnd := time.Date(2022, 2, 0, 0, 0, 0, 0, time.UTC)
	timeMid1 := time.Date(2022, 2, 1, 0, 0, 0, 0, time.UTC)
	timeEnd1 := time.Date(2022, 2, 16, 0, 0, 0, 0, time.UTC)
	shards1 := []meta.ShardInfo{{ID: 1, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	shards2 := []meta.ShardInfo{{ID: 2, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	shards3 := []meta.ShardInfo{{ID: 3, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	csm := &ClusterShardMapper{
		Logger: logger.NewLogger(1),
	}
	csm.MetaClient = &mocShardMapperMetaClient{
		databases: map[string]*meta.DatabaseInfo{
			"db0": {
				Name:                   "db0",
				DefaultRetentionPolicy: "rp0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp0": {
						Name: "rp0",
						Measurements: map[string]*meta.MeasurementInfo{
							"mst1": {
								Name: "mst1",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"1", "2"},
										Type:       "hash",
										ShardGroup: 1,
									},
								},
								Schema: &meta.CleanSchema{
									"f1":   meta2.SchemaVal{Typ: influx.Field_Type_String},
									"tag1": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
								},
								EngineType: config.COLUMNSTORE,
							},
							"mst2": {
								Name: "mst2",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"1", "2"},
										Type:       "hash",
										ShardGroup: 2,
									},
									{
										ShardKey:   []string{"3", "4"},
										Type:       "hash",
										ShardGroup: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID:         1,
								StartTime:  timeStart,
								EndTime:    timeMid,
								Shards:     shards1,
								EngineType: config.COLUMNSTORE,
							},
							{
								ID:         2,
								StartTime:  timeMid,
								EndTime:    timeEnd,
								Shards:     shards2,
								EngineType: config.TSSTORE,
							},
							{
								ID:         3,
								StartTime:  timeMid1,
								EndTime:    timeEnd1,
								Shards:     shards3,
								EngineType: config.TSSTORE,
							},
						},
					},
				},
				ShardKey: meta.ShardKeyInfo{
					ShardKey:   []string{"1", "2", "3"},
					Type:       "hash",
					ShardGroup: 3,
				},
			},
		},
	}
	csm.Measurement("db0", "rp0", "mst1")
	m, _ := csm.MetaClient.(*mocShardMapperMetaClient)
	db, _ := m.databases["db0"]
	rp, _ := db.RetentionPolicies["rp0"]
	mst, _ := rp.Measurements["mst1"]
	mst.SetoriginName("mst1")

	opt := &query.SelectOptions{}
	source := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst1", EngineType: config.COLUMNSTORE,
	}
	join := &influxql.Join{
		LSrc:      source,
		RSrc:      source,
		Condition: &influxql.BinaryExpr{},
	}
	shardMapping := &ClusterShardMapping{
		ShardMap:  map[Source]map[uint32][]executor.ShardInfo{},
		seriesKey: make([]byte, 0),
	}
	seriesKey := make([]byte, 0)
	csm.mapShards(shardMapping, []influxql.Source{join}, timeStart, timeEnd, nil, opt)
	Val, _ := regexp.Compile("")
	sourceRegex := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst2", EngineType: config.COLUMNSTORE,
		Regex: &influxql.RegexLiteral{Val: Val},
	}
	seriesKey = seriesKey[:0]
	csm.mapMstShards(sourceRegex, shardMapping, timeStart, timeEnd, nil, opt)
	shardMapping.MetaClient = csm.MetaClient
	fields, dimensions, schema, err := shardMapping.FieldDimensions(source)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	expectedFields := map[string]influxql.DataType{
		"f1": influxql.String,
	}
	if !reflect.DeepEqual(fields, expectedFields) {
		t.Errorf("Expected fields to be %v, got %v", expectedFields, fields)
	}
	expectedDimensions := map[string]struct{}{
		"tag1": struct{}{},
	}
	if !reflect.DeepEqual(dimensions, expectedDimensions) {
		t.Errorf("Expected dimensions to be %v, got %v", expectedDimensions, dimensions)
	}
	expectedSchema := &influxql.Schema{
		MinTime: math.MaxInt64,
		MaxTime: math.MinInt64,
	}
	if !reflect.DeepEqual(schema, expectedSchema) {
		t.Errorf("Expected schema to be %v, got %v", expectedSchema, schema)
	}
}
func Test_CreateLogicalPlan(t *testing.T) {
	timeStart := time.Date(2022, 1, 0, 0, 0, 0, 0, time.UTC)
	timeMid := time.Date(2022, 1, 15, 0, 0, 0, 0, time.UTC)
	timeEnd := time.Date(2022, 2, 0, 0, 0, 0, 0, time.UTC)
	timeMid1 := time.Date(2022, 2, 1, 0, 0, 0, 0, time.UTC)
	timeEnd1 := time.Date(2022, 2, 16, 0, 0, 0, 0, time.UTC)
	shards1 := []meta.ShardInfo{{ID: 1, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	shards2 := []meta.ShardInfo{{ID: 2, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	shards3 := []meta.ShardInfo{{ID: 3, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	csm := &ClusterShardMapper{
		Logger: logger.NewLogger(1),
	}
	csm.MetaClient = &mocShardMapperMetaClient{
		databases: map[string]*meta.DatabaseInfo{
			"db0": {
				Name:                   "db0",
				DefaultRetentionPolicy: "rp0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp0": {
						Name: "rp0",
						Measurements: map[string]*meta.MeasurementInfo{
							"mst1": {
								Name: "mst1",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"v1", "u1"},
										Type:       "hash",
										ShardGroup: 1,
									},
								},
								Schema: &meta.CleanSchema{
									"f1":   meta2.SchemaVal{Typ: influx.Field_Type_String},
									"tag1": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
								},
								EngineType: config.COLUMNSTORE,
							},
							"mst2": {
								Name: "mst2",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"1", "2"},
										Type:       "hash",
										ShardGroup: 2,
									},
									{
										ShardKey:   []string{"3", "4"},
										Type:       "hash",
										ShardGroup: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID:         1,
								StartTime:  timeStart,
								EndTime:    timeMid,
								Shards:     shards1,
								EngineType: config.COLUMNSTORE,
							},
							{
								ID:         2,
								StartTime:  timeMid,
								EndTime:    timeEnd,
								Shards:     shards2,
								EngineType: config.TSSTORE,
							},
							{
								ID:         3,
								StartTime:  timeMid1,
								EndTime:    timeEnd1,
								Shards:     shards3,
								EngineType: config.TSSTORE,
							},
						},
					},
				},
				ShardKey: meta.ShardKeyInfo{
					ShardKey:   []string{"1", "2", "3"},
					Type:       "hash",
					ShardGroup: 3,
				},
			},
		},
	}
	m, _ := csm.MetaClient.(*mocShardMapperMetaClient)
	db, _ := m.databases["db0"]
	data := &meta.Data{
		PtNumPerNode: 1,
		ClusterPtNum: 1,
		DataNodes: []meta.DataNode{
			{
				NodeInfo: meta.NodeInfo{
					ID: 1,
				},
				ConnID:      1,
				AliveConnID: 1,
			},
		},
	}
	data.CreateDBPtView("db0")
	data.SetDatabase(db)
	opt := &query.SelectOptions{}
	opt1 := &query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	source := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst1", EngineType: config.COLUMNSTORE,
	}
	join := &influxql.Join{
		LSrc:      source,
		RSrc:      source,
		Condition: &influxql.BinaryExpr{},
	}
	shardMapping := &ClusterShardMapping{
		ShardMap:    map[Source]map[uint32][]executor.ShardInfo{},
		seriesKey:   make([]byte, 0),
		ShardMapper: csm,
	}
	seriesKey := make([]byte, 0)
	csm.mapShards(shardMapping, []influxql.Source{join}, timeStart, timeEnd, nil, opt)
	Val, _ := regexp.Compile("")
	sourceRegex := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst2", EngineType: config.COLUMNSTORE,
		Regex: &influxql.RegexLiteral{Val: Val},
	}
	seriesKey = seriesKey[:0]
	csm.mapMstShards(sourceRegex, shardMapping, timeStart, timeEnd, nil, opt)
	shardMapping.MetaClient = m
	sql := "SELECT v1,u1 FROM mst1 limit 10"
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	qry, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal()
	}
	stmt := qry.Statements[0]
	stmt, err = query.RewriteStatement(stmt)
	if err != nil {
		t.Fatal()
	}
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal()
	}
	selectStmt.OmitTime = true
	schema := executor.NewQuerySchema(selectStmt.Fields, selectStmt.ColumnNames(), opt1, nil)
	schema.SetFill(influxql.NoFill)
	var key = []uint64{1}
	ctx := context.WithValue(context.Background(), (query.QueryIDKey), key)
	ssqs := statistics.NewSqlSlowQueryStatistics("db0")
	locs := make([][2]int, 0)
	locs = append(locs, [2]int{0, 31})
	ssqs.SetQueryAndLocs("SELECT v1,u1 FROM mst1 limit 10", locs)
	ctx = context.WithValue(ctx, query.QueryDurationKey, ssqs)
	m.cacheData = data

	// Querying the tsstore engine and colstore engine at the same time
	plan, err := shardMapping.CreateLogicalPlan(ctx, []influxql.Source{source, sourceRegex}, schema)
	if !reflect.DeepEqual(plan.Schema().GetSourcesNames(), []string{"mst1", "mst2"}) {
		t.Fatal()
	}
	if !reflect.DeepEqual(plan.Schema().GetColumnNames(), []string{"v1", "u1"}) {
		t.Fatal()
	}
	require.Equal(t, shardMapping.NodeNumbers(), 1)

	// Querying the colstore engine
	plan, err = shardMapping.CreateLogicalPlan(ctx, []influxql.Source{source}, schema)
	if !reflect.DeepEqual(plan.Schema().GetSourcesNames(), []string{"mst1"}) {
		t.Fatal()
	}
	if !reflect.DeepEqual(plan.Schema().GetColumnNames(), []string{"v1", "u1"}) {
		t.Fatal()
	}
	require.Equal(t, shardMapping.NodeNumbers(), 1)

	// Querying the colstore engine by the unified plan
	source.IndexRelation.Oids = append(source.IndexRelation.Oids, uint32(index.BloomFilter))
	plan, err = shardMapping.CreateLogicalPlan(ctx, []influxql.Source{source}, schema)
	if !reflect.DeepEqual(plan.Schema().GetSourcesNames(), []string{"mst1"}) {
		t.Fatal()
	}
	if !reflect.DeepEqual(plan.Schema().GetColumnNames(), []string{"v1", "u1"}) {
		t.Fatal()
	}
	require.Equal(t, shardMapping.NodeNumbers(), 1)
}

func Test_MapTypeBatch(t *testing.T) {
	timeStart := time.Date(2022, 1, 0, 0, 0, 0, 0, time.UTC)
	timeMid := time.Date(2022, 1, 15, 0, 0, 0, 0, time.UTC)
	timeEnd := time.Date(2022, 2, 0, 0, 0, 0, 0, time.UTC)
	timeMid1 := time.Date(2022, 2, 1, 0, 0, 0, 0, time.UTC)
	timeEnd1 := time.Date(2022, 2, 16, 0, 0, 0, 0, time.UTC)
	shards1 := []meta.ShardInfo{{ID: 1, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	shards2 := []meta.ShardInfo{{ID: 2, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	shards3 := []meta.ShardInfo{{ID: 3, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	csm := &ClusterShardMapper{
		Logger: logger.NewLogger(1),
	}
	defer csm.Close()
	csm.MetaClient = &mocShardMapperMetaClient{
		databases: map[string]*meta.DatabaseInfo{
			"db0": {
				Name:                   "db0",
				DefaultRetentionPolicy: "rp0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp0": {
						Name: "rp0",
						Measurements: map[string]*meta.MeasurementInfo{
							"mst1": {
								Name: "mst1",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"1", "2"},
										Type:       "hash",
										ShardGroup: 1,
									},
								},
								Schema: &meta.CleanSchema{
									"f1":   meta2.SchemaVal{Typ: influx.Field_Type_String},
									"tag1": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
								},
								EngineType: config.COLUMNSTORE,
							},
							"mst2": {
								Name: "mst2",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"1", "2"},
										Type:       "hash",
										ShardGroup: 2,
									},
									{
										ShardKey:   []string{"3", "4"},
										Type:       "hash",
										ShardGroup: 3,
									},
								},
								EngineType: config.TSSTORE,
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID:         1,
								StartTime:  timeStart,
								EndTime:    timeMid,
								Shards:     shards1,
								EngineType: config.COLUMNSTORE,
							},
							{
								ID:         2,
								StartTime:  timeMid,
								EndTime:    timeEnd,
								Shards:     shards2,
								EngineType: config.TSSTORE,
							},
							{
								ID:         3,
								StartTime:  timeMid1,
								EndTime:    timeEnd1,
								Shards:     shards3,
								EngineType: config.TSSTORE,
							},
						},
					},
				},
				ShardKey: meta.ShardKeyInfo{
					ShardKey:   []string{"1", "2", "3"},
					Type:       "hash",
					ShardGroup: 3,
				},
			},
		},
	}
	csm.Measurement("db0", "rp0", "mst1")
	m, _ := csm.MetaClient.(*mocShardMapperMetaClient)
	db, _ := m.databases["db0"]
	rp, _ := db.RetentionPolicies["rp0"]
	mst, _ := rp.Measurements["mst1"]
	mst.SetoriginName("mst1")
	opt := &query.SelectOptions{}
	source := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst1", EngineType: config.COLUMNSTORE,
	}
	join := &influxql.Join{
		LSrc:      source,
		RSrc:      source,
		Condition: &influxql.BinaryExpr{},
	}
	ttime := influxql.TimeRange{
		Min: timeStart,
		Max: timeEnd,
	}
	csming, _ := csm.MapShards([]influxql.Source{join}, ttime, query.SelectOptions{}, nil)
	shardMapping := csming.(*ClusterShardMapping)
	seriesKey := make([]byte, 0)
	Val, _ := regexp.Compile("")
	sourceRegex := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst2", EngineType: config.COLUMNSTORE,
		Regex: &influxql.RegexLiteral{Val: Val},
	}
	seriesKey = seriesKey[:0]
	csm.mapMstShards(sourceRegex, shardMapping, timeStart, timeEnd, nil, opt)
	schema := &influxql.Schema{
		MinTime: math.MaxInt64,
		MaxTime: math.MinInt64,
	}
	myFields := map[string]*influxql.FieldNameSpace{
		"f1": {
			RealName: "f1",
			DataType: influx.Field_Type_String,
		},
		"f2": {
			RealName: "f2",
			DataType: influx.Field_Type_String,
		},
		"tag1": {
			RealName: "tag1",
			DataType: influx.Field_Type_Tag,
		},
	}
	err := shardMapping.MapTypeBatch(source, myFields, schema)
	if err != nil {
		t.Fatal()
	}
	mytype := shardMapping.MapType(source, "f1")
	if mytype != record.ToInfluxqlTypes(int(influx.Field_Type_String)) {
		t.Fatal()
	}
	mytype = shardMapping.MapType(source, "tag1")
	if mytype != record.ToInfluxqlTypes(int(influx.Field_Type_Tag)) {
		t.Fatal()
	}
}

func Test_CreateRemoteQuery(t *testing.T) {
	csm := &ClusterShardMapping{
		Logger: logger.NewLogger(1),
	}
	csm.MetaClient = &mocShardMapperMetaClient{
		databases: map[string]*meta.DatabaseInfo{
			"db0": {
				Name:                   "db0",
				DefaultRetentionPolicy: "rp0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp0": {
						Name: "rp0",
						Measurements: map[string]*meta.MeasurementInfo{
							"mst1": {
								Name: "mst1",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"v1", "u1"},
										Type:       "hash",
										ShardGroup: 1,
									},
								},
								Schema: &meta.CleanSchema{
									"f1":   meta2.SchemaVal{Typ: influx.Field_Type_String},
									"tag1": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
								},
								EngineType: config.COLUMNSTORE,
							},
						},
					},
				},
			},
		},
		cacheData: &meta.Data{
			PtNumPerNode: 1,
			ClusterPtNum: 1,
			DataNodes: []meta.DataNode{
				{
					NodeInfo: meta.NodeInfo{
						ID: 1,
					},
					ConnID:      1,
					AliveConnID: 1,
				},
			},
		},
	}

	var key uint64 = 1
	ctx := context.WithValue(context.Background(), (query.QueryIDKey), key)
	opt := query.ProcessorOptions{}
	source := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst1", EngineType: config.COLUMNSTORE,
	}
	_, err := csm.makeRemoteQuery(ctx, influxql.Sources{source}, opt, 1, 1, []executor.ShardInfo{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}, nil)
	if err != nil {
		t.Fatalf("makeRemoteQuery failed: %v", err)
	}
}

func Test_CreateLogicalPlanForRWSplit(t *testing.T) {
	timeStart := time.Date(2023, 1, 0, 0, 0, 0, 0, time.UTC)
	timeMid := time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)
	timeEnd := time.Date(2023, 2, 0, 0, 0, 0, 0, time.UTC)
	timeMid1 := time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC)
	timeEnd1 := time.Date(2023, 2, 16, 0, 0, 0, 0, time.UTC)
	shards1 := []meta.ShardInfo{{ID: 1, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	shards2 := []meta.ShardInfo{{ID: 2, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	shards3 := []meta.ShardInfo{{ID: 3, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	csm := &ClusterShardMapper{
		Logger: logger.NewLogger(1),
	}
	obsOpts := &obs.ObsOptions{Enabled: true, BucketName: "test_bucket_name", Endpoint: "test_endpoint", Ak: "test_ak", Sk: "test_sk", BasePath: "test_basepath"}
	csm.MetaClient = &mocShardMapperMetaClient{
		databases: map[string]*meta.DatabaseInfo{
			"db0": {
				Name:                   "db0",
				DefaultRetentionPolicy: "rp0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp0": {
						Name: "rp0",
						Measurements: map[string]*meta.MeasurementInfo{
							"mst1": {
								Name: "mst1",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"v1", "u1"},
										Type:       "hash",
										ShardGroup: 1,
									},
								},
								Schema: &meta.CleanSchema{
									"f1":   meta2.SchemaVal{Typ: influx.Field_Type_String},
									"tag1": meta2.SchemaVal{Typ: influx.Field_Type_Tag},
								},
								ObsOptions: obsOpts,
								EngineType: config.COLUMNSTORE,
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID:         1,
								StartTime:  timeStart,
								EndTime:    timeMid,
								Shards:     shards1,
								EngineType: config.COLUMNSTORE,
							},
							{
								ID:         2,
								StartTime:  timeMid,
								EndTime:    timeEnd,
								Shards:     shards2,
								EngineType: config.COLUMNSTORE,
							},
							{
								ID:         3,
								StartTime:  timeMid1,
								EndTime:    timeEnd1,
								Shards:     shards3,
								EngineType: config.COLUMNSTORE,
							},
						},
					},
				},
				ShardKey: meta.ShardKeyInfo{
					ShardKey:   []string{"1", "2", "3"},
					Type:       "hash",
					ShardGroup: 3,
				},
			},
		},
	}
	m, _ := csm.MetaClient.(*mocShardMapperMetaClient)
	db, _ := m.databases["db0"]
	data := &meta.Data{
		PtNumPerNode: 1,
		ClusterPtNum: 1,
		DataNodes: []meta.DataNode{
			{
				NodeInfo: meta.NodeInfo{
					ID: 1,
				},
				ConnID:      1,
				AliveConnID: 1,
			},
			{
				NodeInfo: meta.NodeInfo{
					ID:   2,
					Role: "reader",
				},
				ConnID:      2,
				AliveConnID: 2,
			},
		},
	}
	data.CreateDBPtView("db0")
	data.SetDatabase(db)
	opt := &query.SelectOptions{}
	opt1 := &query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{"host"},
		Ascending:  true,
		ChunkSize:  100,
	}
	source := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst1", ObsOptions: obsOpts, EngineType: config.COLUMNSTORE,
	}
	join := &influxql.Join{
		LSrc:      source,
		RSrc:      source,
		Condition: &influxql.BinaryExpr{},
	}
	shardMapping := &ClusterShardMapping{
		ShardMap:    map[Source]map[uint32][]executor.ShardInfo{},
		seriesKey:   make([]byte, 0),
		ShardMapper: csm,
	}
	csm.mapShards(shardMapping, []influxql.Source{join}, timeStart, timeEnd, nil, opt)
	shardMapping.MetaClient = m
	sql := "SELECT v1,u1 FROM mst1 limit 10"
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	qry, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal()
	}
	stmt := qry.Statements[0]
	stmt, err = query.RewriteStatement(stmt)
	if err != nil {
		t.Fatal()
	}
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal()
	}
	selectStmt.OmitTime = true
	schema := executor.NewQuerySchema(selectStmt.Fields, selectStmt.ColumnNames(), opt1, nil)
	schema.SetFill(influxql.NoFill)
	var key = []uint64{1}
	ctx := context.WithValue(context.Background(), (query.QueryIDKey), key)
	m.cacheData = data

	// Querying the colstore engine
	plan, err := shardMapping.CreateLogicalPlan(ctx, []influxql.Source{source}, schema)
	if !reflect.DeepEqual(plan.Schema().GetSourcesNames(), []string{"mst1"}) {
		t.Fatal()
	}
	if !reflect.DeepEqual(plan.Schema().GetColumnNames(), []string{"v1", "u1"}) {
		t.Fatal()
	}
}

func Test_MapTypeBatchBinOp(t *testing.T) {
	timeStart := time.Date(2022, 1, 0, 0, 0, 0, 0, time.UTC)
	timeMid := time.Date(2022, 1, 15, 0, 0, 0, 0, time.UTC)
	timeEnd := time.Date(2022, 2, 0, 0, 0, 0, 0, time.UTC)
	shards1 := []meta.ShardInfo{{ID: 1, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	csm := &ClusterShardMapper{
		Logger: logger.NewLogger(1),
	}
	defer csm.Close()
	mc := &mocShardMapperMetaClient{
		databases: map[string]*meta.DatabaseInfo{
			"db0": {
				Name:                   "db0",
				DefaultRetentionPolicy: "rp0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp0": {
						Name: "rp0",
						Measurements: map[string]*meta.MeasurementInfo{
							"mst1": {
								Name: "mst1",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"1", "2"},
										Type:       "hash",
										ShardGroup: 1,
									},
								},
								Schema: &meta.CleanSchema{
									"value": meta2.SchemaVal{Typ: influx.Field_Type_Float},
									"tag1":  meta2.SchemaVal{Typ: influx.Field_Type_Tag},
								},
								EngineType: config.TSSTORE,
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID:         1,
								StartTime:  timeStart,
								EndTime:    timeMid,
								Shards:     shards1,
								EngineType: config.TSSTORE,
							},
						},
					},
				},
				ShardKey: meta.ShardKeyInfo{
					ShardKey:   []string{"1"},
					Type:       "hash",
					ShardGroup: 3,
				},
			},
		},
	}
	csm.MetaClient = mc
	csm.Measurement("db0", "rp0", "mst1")
	m, _ := csm.MetaClient.(*mocShardMapperMetaClient)
	db, _ := m.databases["db0"]
	rp, _ := db.RetentionPolicies["rp0"]
	mst, _ := rp.Measurements["mst1"]
	mst.SetoriginName("mst1")
	source := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst1", EngineType: config.TSSTORE,
	}
	binOp := &influxql.BinOp{
		LSrc: source,
		RSrc: source,
	}
	ttime := influxql.TimeRange{
		Min: timeStart,
		Max: timeEnd,
	}
	csming, _ := csm.MapShards([]influxql.Source{binOp}, ttime, query.SelectOptions{}, nil)
	shardMapping := csming.(*ClusterShardMapping)
	schema := &influxql.Schema{
		MinTime: math.MaxInt64,
		MaxTime: math.MinInt64,
	}
	myFields := map[string]*influxql.FieldNameSpace{
		"value": {
			RealName: "value",
			DataType: influx.Field_Type_Float,
		},
		"tag1": {
			RealName: "tag1",
			DataType: influx.Field_Type_Tag,
		},
	}
	err := shardMapping.MapTypeBatch(source, myFields, schema)
	if err != nil {
		t.Fatal()
	}
	mytype := shardMapping.MapType(source, "value")
	if mytype != record.ToInfluxqlTypes(int(influx.Field_Type_Float)) {
		t.Fatal()
	}
	mytype = shardMapping.MapType(source, "tag1")
	if mytype != record.ToInfluxqlTypes(int(influx.Field_Type_Tag)) {
		t.Fatal()
	}
	dbinfo := mc.databases["db0"]
	delete(dbinfo.RetentionPolicies, "rp0")
	_, err = csm.MapShards([]influxql.Source{binOp}, ttime, query.SelectOptions{}, nil)
	if err == nil {
		t.Fatal("Test_MapTypeBatchBinOp err")
	}
}

func Test_MapTypeBatchBinOpNilMst(t *testing.T) {
	timeStart := time.Date(2022, 1, 0, 0, 0, 0, 0, time.UTC)
	timeMid := time.Date(2022, 1, 15, 0, 0, 0, 0, time.UTC)
	timeEnd := time.Date(2022, 2, 0, 0, 0, 0, 0, time.UTC)
	shards1 := []meta.ShardInfo{{ID: 1, Owners: []uint32{0}, Min: "", Max: "", Tier: util.Hot, IndexID: 1, DownSampleID: 0, DownSampleLevel: 0, ReadOnly: false, MarkDelete: false}}
	csm := &ClusterShardMapper{
		Logger: logger.NewLogger(1),
	}
	defer csm.Close()
	mc := &mocShardMapperMetaClient{
		databases: map[string]*meta.DatabaseInfo{
			"db0": {
				Name:                   "db0",
				DefaultRetentionPolicy: "rp0",
				RetentionPolicies: map[string]*meta.RetentionPolicyInfo{
					"rp0": {
						Name: "rp0",
						Measurements: map[string]*meta.MeasurementInfo{
							"mst1": {
								Name: "mst1",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"1", "2"},
										Type:       "hash",
										ShardGroup: 1,
									},
								},
								Schema: &meta.CleanSchema{
									"value": meta2.SchemaVal{Typ: influx.Field_Type_Float},
									"tag1":  meta2.SchemaVal{Typ: influx.Field_Type_Tag},
								},
								EngineType: config.TSSTORE,
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID:         1,
								StartTime:  timeStart,
								EndTime:    timeMid,
								Shards:     shards1,
								EngineType: config.TSSTORE,
							},
						},
					},
				},
				ShardKey: meta.ShardKeyInfo{
					ShardKey:   []string{"1"},
					Type:       "hash",
					ShardGroup: 3,
				},
			},
		},
	}
	csm.MetaClient = mc
	csm.Measurement("db0", "rp0", "mst1")
	m, _ := csm.MetaClient.(*mocShardMapperMetaClient)
	db, _ := m.databases["db0"]
	rp, _ := db.RetentionPolicies["rp0"]
	mst, _ := rp.Measurements["mst1"]
	mst.SetoriginName("mst1")
	csm.Measurement("db0", "rp0", "mst1")
	source1 := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst1", EngineType: config.TSSTORE,
	}
	source2 := &influxql.Measurement{
		Database: "db0", RetentionPolicy: "rp0", Name: "mst2", EngineType: config.TSSTORE,
	}
	binOp := &influxql.BinOp{
		LSrc:   source1,
		RSrc:   source2,
		OpType: parser.LOR,
		NilMst: influxql.RNilMst,
	}
	ttime := influxql.TimeRange{
		Min: timeStart,
		Max: timeEnd,
	}
	csming, _ := csm.MapShards([]influxql.Source{binOp}, ttime, query.SelectOptions{}, nil)
	shardMapping := csming.(*ClusterShardMapping)
	schema := &influxql.Schema{
		MinTime: math.MaxInt64,
		MaxTime: math.MinInt64,
	}
	myFields := map[string]*influxql.FieldNameSpace{
		"value": {
			RealName: "value",
			DataType: influx.Field_Type_Float,
		},
		"tag1": {
			RealName: "tag1",
			DataType: influx.Field_Type_Tag,
		},
	}
	err := shardMapping.MapTypeBatch(source1, myFields, schema)
	if err != nil {
		t.Fatal()
	}
	binOp.NilMst = influxql.LNilMst
	csming, _ = csm.MapShards([]influxql.Source{binOp}, ttime, query.SelectOptions{}, nil)
	if csming != nil {
		t.Fatal()
	}
	binOp.LSrc, binOp.RSrc = source2, source1
	csming, _ = csm.MapShards([]influxql.Source{binOp}, ttime, query.SelectOptions{}, nil)
	shardMapping = csming.(*ClusterShardMapping)
	err = shardMapping.MapTypeBatch(source1, myFields, schema)
	if err != nil {
		t.Fatal()
	}
}
