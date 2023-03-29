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
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	set "github.com/openGemini/openGemini/open_src/github.com/deckarep/golang-set"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
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
}

func (m mocShardMapperMetaClient) GetStreamInfos() map[string]*meta.StreamInfo {
	return nil
}

func (m mocShardMapperMetaClient) GetStreamInfosStore() map[string]*meta.StreamInfo {
	//TODO implement me
	panic("implement me")
}

func (m mocShardMapperMetaClient) GetMeasurementInfoStore(database string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
	//TODO implement me
	panic("implement me")
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

func (m mocShardMapperMetaClient) CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta.ShardKeyInfo, indexR *meta.IndexRelation) (*meta.MeasurementInfo, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) AlterShardKey(database, retentionPolicy, mst string, shardKey *meta.ShardKeyInfo) error {
	return nil
}

func (m mocShardMapperMetaClient) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	return m.databases[name], nil
}

func (m mocShardMapperMetaClient) CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec, shardKey *meta.ShardKeyInfo) (*meta.DatabaseInfo, error) {
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
	return nil, nil
}

func (m mocShardMapperMetaClient) DataNodes() ([]meta.DataNode, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) DeleteDataNode(id uint64) error {
	return nil
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

func (m mocShardMapperMetaClient) TruncateShardGroups(t time.Time) error {
	return nil
}

func (m mocShardMapperMetaClient) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error {
	return nil
}

func (m mocShardMapperMetaClient) UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto.FieldSchema) error {
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

func (m mocShardMapperMetaClient) MarkMeasurementDelete(database, mst string) error {
	return nil
}

func (m mocShardMapperMetaClient) DBPtView(database string) (meta.DBPtInfos, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo) {
	return
}

func (m mocShardMapperMetaClient) Measurement(database string, rpName string, mstName string) (*meta.MeasurementInfo, error) {
	return nil, nil
}

func (m mocShardMapperMetaClient) Schema(database string, retentionPolicy string, mst string) (fields map[string]int32, dimensions map[string]struct{}, err error) {
	return nil, nil, nil
}

func (m mocShardMapperMetaClient) GetMeasurements(mst *influxql.Measurement) ([]*meta.MeasurementInfo, error) {
	db := m.databases[mst.Database]
	if db == nil {
		return nil, nil
	}
	rp := db.RetentionPolicies[mst.RetentionPolicy]
	if rp == nil {
		return nil, nil
	}
	return []*meta.MeasurementInfo{rp.Measurements[mst.Name]}, nil
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

func (m mocShardMapperMetaClient) ShowShards() models.Rows {
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

func (m mocShardMapperMetaClient) OpenAtStore() {
	return
}

func TestMapMstShards(t *testing.T) {
	timeStart := time.Date(2022, 1, 0, 0, 0, 0, 0, time.UTC)
	timeMid := time.Date(2022, 1, 15, 0, 0, 0, 0, time.UTC)
	timeEnd := time.Date(2022, 2, 0, 0, 0, 0, 0, time.UTC)
	shards1 := []meta.ShardInfo{{1, []uint32{0}, "", "", util.Hot, 1, 0, 0, false, false}}
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
							"mst": {
								Name: "mst",
								ShardKeys: []meta.ShardKeyInfo{
									{
										ShardKey:   []string{"1", "2"},
										Type:       "hash",
										ShardGroup: 1,
									},
								},
							},
						},
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID:        1,
								StartTime: timeStart,
								EndTime:   timeMid,
								Shards:    shards1,
							},
							{
								ID:        1,
								StartTime: timeMid,
								EndTime:   timeEnd,
								Shards:    shards1,
							},
						},
					},
				},
				ShardKey: meta.ShardKeyInfo{
					ShardKey:   []string{"1", "2"},
					Type:       "hash",
					ShardGroup: 1,
				},
			},
		},
	}
	opt := &query.SelectOptions{}
	source := &influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: "mst"}
	join := &influxql.Join{
		LSrc:      source,
		RSrc:      source,
		Condition: &influxql.BinaryExpr{},
	}
	shardMapping := &ClusterShardMapping{
		ShardMap: map[Source]map[uint32][]uint64{},
	}
	csm.mapShards(shardMapping, []influxql.Source{join}, timeStart, timeEnd, nil, opt)
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
