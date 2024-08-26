// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package meta

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	_ "github.com/mattn/go-sqlite3"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	logger1 "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	assert2 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardGroupSort(t *testing.T) {
	sg1 := ShardGroupInfo{
		ID:          1,
		StartTime:   time.Unix(1000, 0),
		EndTime:     time.Unix(1100, 0),
		TruncatedAt: time.Unix(1050, 0),
	}

	sg2 := ShardGroupInfo{
		ID:        2,
		StartTime: time.Unix(1000, 0),
		EndTime:   time.Unix(1100, 0),
	}

	sgs := ShardGroupInfos{sg2, sg1}

	sort.Sort(sgs)

	if sgs[len(sgs)-1].ID != 2 {
		t.Fatal("unstable sort for ShardGroupInfos")
	}
}

func Test_Data_CreateMeasurement(t *testing.T) {
	data := initData()

	dbName := "foo"
	rpName := "bar"
	err := data.CreateDatabase(dbName, &RetentionPolicyInfo{
		Name:     rpName,
		ReplicaN: 1,
		Duration: 24 * time.Hour,
	}, nil, false, 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	rp, err := data.RetentionPolicy(dbName, rpName)
	if err != nil {
		t.Fatal(err)
	}

	if rp == nil {
		t.Fatal("creation of retention policy failed")
	}

	mstName := "cpu"
	err = data.CreateMeasurement(dbName, rpName, mstName,
		&proto2.ShardKeyInfo{ShardKey: []string{"hostName", "location"}, Type: proto.String(influxql.RANGE)}, 0, nil, 0, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// try to recreate measurement with same shardKey, should success
	err = data.CreateMeasurement(dbName, rpName, mstName,
		&proto2.ShardKeyInfo{ShardKey: []string{"hostName", "location"}, Type: proto.String(influxql.RANGE)}, 0, nil, 0, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// try to recreate measurement with same shardKey, should fail
	err = data.CreateMeasurement(dbName, rpName, mstName,
		&proto2.ShardKeyInfo{ShardKey: []string{"hostName", "region"}, Type: proto.String(influxql.RANGE)}, 0, nil, 0, nil, nil, nil)
	if err == nil || err != ErrMeasurementExists {
		t.Fatalf("unexpected error.  got: %v, exp: %s", err, ErrMeasurementExists)
	}
}

func Test_Data_AlterShardKey(t *testing.T) {
	data := initData()

	dbName := "foo"
	rpName := "bar"
	err := data.CreateDatabase(dbName, &RetentionPolicyInfo{
		Name:     rpName,
		ReplicaN: 1,
		Duration: 24 * time.Hour,
	}, nil, false, 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	rp, err := data.RetentionPolicy(dbName, rpName)
	if err != nil {
		t.Fatal(err)
	}

	if rp == nil {
		t.Fatal("creation of retention policy failed")
	}

	mstName := "cpu"
	err = data.CreateMeasurement(dbName, rpName, mstName,
		&proto2.ShardKeyInfo{ShardKey: []string{"hostName", "location"}, Type: proto.String(influxql.RANGE)}, 0, nil, 0, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	shardKey1 := []string{"hostName", "service"}
	err = data.AlterShardKey(dbName, rpName, mstName,
		&proto2.ShardKeyInfo{ShardKey: shardKey1, Type: proto.String(influxql.RANGE)})
	if err != nil {
		t.Fatal(err)
	}

	mst, err := data.Measurement(dbName, rpName, mstName)
	if err != nil {
		t.Fatal(err)
	}

	expectShardKey := []ShardKeyInfo{{[]string{"hostName", "service"}, "range", 1}}
	if got, exp := mst.ShardKeys, expectShardKey; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got shardKey %v, expected %v", got, exp)
	}

	err = data.CreateShardGroup(dbName, rpName, time.Unix(0, 0), util.Hot, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	sg0, err := data.ShardGroupByTimestampAndEngineType(dbName, rpName, time.Unix(0, 0), config.TSSTORE)
	if err != nil {
		t.Fatal("Failed to find shard group:", err)
	}

	if sg0.ID != 1 {
		t.Fatalf("got shard group id %d, expected %d", sg0.ID, 1)
	}

	shardKey2 := []string{"region", "service"}
	err = data.AlterShardKey(dbName, rpName, mstName,
		&proto2.ShardKeyInfo{ShardKey: shardKey2, Type: proto.String(influxql.RANGE)})
	if err != nil {
		t.Fatal(err)
	}

	expShardKeys := []ShardKeyInfo{{[]string{"hostName", "service"}, "range", 1},
		{[]string{"region", "service"}, "range", 2}}
	if got, exp := mst.ShardKeys, expShardKeys; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got shardKey %v, expected %v", got, exp)
	}
}

func Test_Data_ReSharding(t *testing.T) {
	data := initData()
	DataLogger = logger.New(os.Stderr)
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	data.CreateDBPtView("foo")
	must(data.CreateDatabase("foo", nil, nil, false, 1, nil))
	rp := NewRetentionPolicyInfo("bar")
	rp.ShardGroupDuration = 24 * time.Hour
	must(data.CreateRetentionPolicy("foo", rp, false))
	must(data.CreateMeasurement("foo", "bar", "cpu",
		&proto2.ShardKeyInfo{ShardKey: []string{"hostname"}, Type: proto.String(influxql.RANGE)}, 0, nil, 0, nil, nil, nil))
	must(data.CreateShardGroup("foo", "bar", time.Unix(0, 0), util.Hot, config.TSSTORE, 0))

	sg0, err := data.ShardGroupByTimestampAndEngineType("foo", "bar", time.Unix(0, 0), config.TSSTORE)
	if err != nil {
		t.Fatal("Failed to find shard group:", err)
	}

	ptinfo := data.PtView["foo"]
	dbptInfo := []PtInfo{{PtOwner{1}, Offline, 0, 1, 0},
		{PtOwner{2}, Offline, 1, 1, 0}}
	if !reflect.DeepEqual([]PtInfo(ptinfo), dbptInfo) {
		t.Fatalf("got %v, exp %v", ptinfo, dbptInfo)
	}
	splitTime := sg0.StartTime.Add(2 * time.Hour)
	bounds := []string{"cpu,hostname=host_5"}

	err = data.ReSharding(&ReShardingInfo{"foo", "bar", 1, splitTime.UnixNano(), bounds})
	if err != nil {
		t.Fatal(err)
	}

	shardgroups, err := data.ShardGroups("foo", "bar")
	shards1 := []ShardInfo{{1, []uint32{0}, "", "", util.Hot, 1, 0, 0, false, false}}
	sg1 := ShardGroupInfo{1, sg0.StartTime, sg0.EndTime,
		sg0.DeletedAt, shards1, sg0.TruncatedAt, config.TSSTORE, 0}
	shards2 := []ShardInfo{{2, []uint32{0}, "", "cpu,hostname=host_5", util.Hot, 3, 0, 0, false, false},
		{3, []uint32{1}, "cpu,hostname=host_5", "", util.Hot, 4, 0, 0, false, false}}
	sg2 := ShardGroupInfo{2, time.Unix(0, splitTime.UnixNano()+1).UTC(), sg0.EndTime,
		sg0.DeletedAt, shards2, sg0.TruncatedAt, config.TSSTORE, 0}
	expSgs := []ShardGroupInfo{sg1, sg2}
	if got, exp := shardgroups, expSgs; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	sg := shardgroups[1]
	if got, exp := sg.DestShard("cpu,hostname=host_1"), &shards2[0]; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v exp %v", got, exp)
	}

	if got, exp := sg.DestShard("cpu,hostname=host_7"), &shards2[1]; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v exp %v", got, exp)
	}
}

func NewMockData(database, policy string) Data {
	data := Data{}

	sgInfo1 := ShardGroupInfo{ID: 1, StartTime: time.Now(), EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []ShardInfo{{ID: 1, MarkDelete: true}, {ID: 2}, {ID: 3}, {ID: 4}}}
	sgInfo2 := ShardGroupInfo{ID: 2, StartTime: time.Now(), EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []ShardInfo{{ID: 5, MarkDelete: false}, {ID: 6}, {ID: 7}, {ID: 8}}}
	sgInfo3 := ShardGroupInfo{ID: 3, StartTime: time.Now(), EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []ShardInfo{{ID: 9, MarkDelete: true}, {ID: 10}, {ID: 11}, {ID: 12}}}

	sgInfos := []ShardGroupInfo{sgInfo1, sgInfo2, sgInfo3}

	rpInfo := RetentionPolicyInfo{
		Name:        policy,
		ShardGroups: sgInfos,
		Measurements: map[string]*MeasurementInfo{
			"mst1": {
				InitNumOfShards: 0,
				ShardIdexes:     nil,
			},
			"mst2": {
				InitNumOfShards: 2,
				ShardIdexes: map[uint64][]int{
					1: {0, 1},
					2: {0, 1},
					3: {0, 1},
				},
			},
			"mst3": {
				InitNumOfShards: 1,
				ShardIdexes: map[uint64][]int{
					1: {2},
					2: {2},
					3: {2},
				},
			},
		},
	}
	rps := make(map[string]*RetentionPolicyInfo)
	rps[policy] = &rpInfo
	dbInfo := DatabaseInfo{Name: database, RetentionPolicies: rps}

	data.Databases = make(map[string]*DatabaseInfo)
	data.Databases[database] = &dbInfo
	return data
}

func TestData_DeleteShardGroup(t *testing.T) {
	db := "foo"
	rp := "bar"

	data := NewMockData(db, rp)
	if err := data.DeleteShardGroup(db, rp, 1, 0, MarkDelete); err != nil {
		t.Fatal(err)
	}

	rpRes, err := data.RetentionPolicy(db, rp)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(rpRes.ShardGroups); i++ {
		if rpRes.ShardGroups[i].ID == 1 {
			assert(!rpRes.ShardGroups[i].DeletedAt.IsZero(), "expect shard group id %d DeleteAt.IsZero is true, but got false", rpRes.ShardGroups[i].ID)
		} else {
			assert(rpRes.ShardGroups[i].DeletedAt.IsZero(), "expect shard group id %d DeleteAt.IsZero is false, but got true", rpRes.ShardGroups[i].ID)
		}
	}
}

func TestData_PruneGroups(t *testing.T) {
	db := "foo"
	rp := "bar"

	data := NewMockData(db, rp)
	if err := data.DeleteShardGroup(db, rp, 1, 0, MarkDelete); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 4; i++ {
		err := data.pruneShardGroups(uint64(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	rpInfo, err := data.RetentionPolicy(db, rp)
	if err != nil {
		t.Fatal(err)
	}

	assert(len(rpInfo.ShardGroups) == 2, "expect len(ShardGroups) == 2, but got %d", len(rpInfo.ShardGroups))
	assert(rpInfo.ShardGroups[0].ID == 2, "expect left ShardGroup ID == 2, but got %d", rpInfo.ShardGroups[0].ID)
	mst2ShardLists := rpInfo.Measurements["mst2"].ShardIdexes
	mst3ShardLists := rpInfo.Measurements["mst3"].ShardIdexes
	ShardListsWant2 := map[uint64][]int{
		2: {0, 1},
		3: {0, 1},
	}
	ShardListsWant3 := map[uint64][]int{
		2: {2},
		3: {2},
	}
	if !reflect.DeepEqual(mst2ShardLists, ShardListsWant2) {
		t.Errorf("shardList got %v, want %v", mst2ShardLists, ShardListsWant2)
	}
	if !reflect.DeepEqual(mst3ShardLists, ShardListsWant3) {
		t.Errorf("shardList got %v, want %v", mst3ShardLists, ShardListsWant3)
	}
}

func initData() *Data {
	data := &Data{PtNumPerNode: 1}
	DataLogger = logger.New(os.Stderr)
	data.CreateDataNode("127.0.0.1:8086", "127.0.0.1:8188", "", "")
	data.CreateDataNode("127.0.0.2:8086", "127.0.0.2:8188", "", "")
	return data
}

func generateMeasurement(data *Data, dbName, rpName, mstName string) error {
	err := data.CreateDatabase(dbName, nil, nil, false, 1, nil)
	if err != nil {
		return err
	}
	rp := NewRetentionPolicyInfo(rpName)
	rp.ShardGroupDuration = 24 * time.Hour
	err = data.CreateRetentionPolicy(dbName, rp, false)
	if err != nil {
		return err
	}
	return data.CreateMeasurement(dbName, rpName, mstName,
		&proto2.ShardKeyInfo{ShardKey: []string{"hostname"}, Type: proto.String(influxql.RANGE)}, 0, nil, 0, nil, nil, nil)
}

func Test_Data_DeleteCmd(t *testing.T) {
	data := initData()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	dbName := "foo"
	rpName := "bar"
	mstName := "cpu"
	must(generateMeasurement(data, dbName, rpName, mstName))
	must(data.CreateShardGroup(dbName, rpName, time.Unix(0, 0), util.Hot, config.TSSTORE, 0))

	must(data.MarkDatabaseDelete(dbName))

	if got, exp := data.Database(dbName).MarkDeleted, true; got != exp {
		t.Fatalf("mark database delete got %v exp %v", got, exp)
	}

	err := data.MarkRetentionPolicyDelete(dbName, rpName)
	assert(errno.Equal(err, errno.DatabaseIsBeingDelete), "mark retention policy should fail when db is being delete")
	if got, exp := data.Database(dbName).RetentionPolicy(rpName).MarkDeleted, false; got != exp {
		t.Fatalf("mark retention policy delete got %v exp %v", got, exp)
	}

	if got, exp := data.Database(dbName).RetentionPolicy(rpName).Measurement(mstName).MarkDeleted, false; got != exp {
		t.Fatalf("mark measurement delete got %v exp %v", got, exp)
	}

	data.DropDatabase(dbName)
	if got, exp := len(data.Databases), 0; got != exp {
		t.Fatalf("length of databases got %v exp %v", got, exp)
	}

	must(generateMeasurement(data, dbName, rpName, mstName))
	must(data.MarkRetentionPolicyDelete(dbName, rpName))
	if got, exp := data.Database(dbName).RetentionPolicy(rpName).MarkDeleted, true; got != exp {
		t.Fatalf("mark retention policy delete got %v exp %v", got, exp)
	}

	err = data.MarkMeasurementDelete(dbName, rpName, mstName)
	assert(err == ErrRetentionPolicyIsBeingDelete, "mark measurement should fail when rp is being delete")
	if got, exp := data.Database(dbName).RetentionPolicy(rpName).Measurement(mstName).MarkDeleted, false; got != exp {
		t.Fatalf("mark measurement delete got %v exp %v", got, exp)
	}

	must(data.DropRetentionPolicy(dbName, rpName))
	if got, exp := len(data.Database(dbName).RetentionPolicies), 0; got != exp {
		t.Fatalf("length of retention policy got %v exp %v", got, exp)
	}

	must(generateMeasurement(data, dbName, rpName, mstName))
	must(data.MarkMeasurementDelete(dbName, rpName, mstName))
	if got, exp := data.Database(dbName).RetentionPolicy(rpName).Measurement(mstName).MarkDeleted, true; got != exp {
		t.Fatalf("mark measurement delete got %v exp %v", got, exp)
	}
	nameWithVer := mstName + "_0000"
	must(data.DropMeasurement(dbName, rpName, nameWithVer))
	if got, exp := len(data.Database(dbName).RetentionPolicy(rpName).Measurements), 0; got != exp {
		t.Fatalf("length of measurement got %v exp %v", got, exp)
	}
}

func TestData_UserCmd(t *testing.T) {
	data := &Data{PtNumPerNode: 1}
	require.EqualError(t, data.CreateUser("", "xxxxhashxxxx", true, false), ErrUsernameRequired.Error())
	require.NoError(t, data.CreateUser("admin", "xxxxhashxxxx", true, false))
	require.EqualError(t, data.CreateUser("admin", "xxxxhashxxxx", true, false), ErrUserExists.Error())
	require.EqualError(t, data.CreateUser("admin_new", "xxxxhashxxxx", true, false), ErrUserForbidden.Error())
	require.NoError(t, data.CreateUser("user1", "xxxxhashxxxx", false, false))

	require.EqualError(t, data.UpdateUser("user1", "xxxxhashxxxx"), ErrPwdUsed.Error())
	require.NoError(t, data.UpdateUser("user1", "new_xxxxhashxxxx"))

	require.EqualError(t, data.SetAdminPrivilege("user_notfound", true), ErrUserNotFound.Error())
	require.EqualError(t, data.SetAdminPrivilege("user1", true), ErrGrantOrRevokeAdmin.Error())

	require.NoError(t, data.DropUser("user1"))
	require.EqualError(t, data.DropUser("admin"), ErrUserDropSelf.Error())
	require.EqualError(t, data.DropUser(""), ErrUserNotFound.Error())
}

func TestData_UpdateRetentionPolicy(t *testing.T) {
	data := initData()
	database := "alterDb"
	policy := "alterRpTest"
	rpInfo := RetentionPolicyInfo{Name: policy}
	dbInfo := DatabaseInfo{Name: database}
	dbInfo.RetentionPolicies = make(map[string]*RetentionPolicyInfo)
	dbInfo.RetentionPolicies[policy] = &rpInfo
	data.Databases = make(map[string]*DatabaseInfo)
	data.Databases[database] = &dbInfo
	duration := 120 * time.Hour
	shardDuration := time.Hour
	rpu := &RetentionPolicyUpdate{Duration: &duration, ShardGroupDuration: &shardDuration}
	if err := data.UpdateRetentionPolicy(database, policy, rpu, false); err != nil {
		t.Fatal(err)
	}

	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got, exp := rpi.ShardGroupDuration, shardDuration; !reflect.DeepEqual(got, exp) {
		t.Fatalf("update retention policy failed shard group duration got %v, exp %v", got, exp)
	}

	if got, exp := rpi.Duration, duration; !reflect.DeepEqual(got, exp) {
		t.Fatalf("update retention policy failed duration got %v, exp %v", got, exp)
	}

	hotDuration := 3 * time.Hour
	rpu = &RetentionPolicyUpdate{HotDuration: &hotDuration}
	if err := data.UpdateRetentionPolicy(database, policy, rpu, false); err != nil {
		t.Fatal(err)
	}

	rpi, err = data.RetentionPolicy(database, policy)
	if err != nil {
		t.Fatal(err)
	}

	if got, exp := rpi.HotDuration, hotDuration; !reflect.DeepEqual(got, exp) {
		t.Fatalf("update retention policy failed hot duration got %v exp %v", got, exp)
	}

	hotDuration = 30 * time.Second
	rpu = &RetentionPolicyUpdate{HotDuration: &hotDuration}
	if err := data.UpdateRetentionPolicy(database, policy, rpu, false); err == nil {
		t.Fatalf("unexpected error.  got: %v, exp: %s", err, ErrIncompatibleShardGroupDurations)
	}
}

func TestShardInfo_ContainPrefix(t *testing.T) {
	shard1 := ShardInfo{Min: "", Max: "cpu,hostname=host1,ip=127.0.0.1"}
	shard2 := ShardInfo{Min: "cpu,hostname=host1,ip=127.0.0.1", Max: ""}
	if got, exp := shard1.ContainPrefix("mem"), false; got != exp {
		t.Fatalf("contain prefix got %v exp %v", got, exp)
	}

	if got, exp := shard2.ContainPrefix("mem"), true; got != exp {
		t.Fatalf("contain prefix got %v exp %v", got, exp)
	}

	if !shard1.ContainPrefix("cpu") {
		t.Fatalf("shard1 should contain prefix cpu")
	}

	if !shard2.ContainPrefix("cpu") {
		t.Fatalf("shard2 should contain prefix cpu")
	}

	shard3 := ShardInfo{Min: "", Max: "cpu,hostname=host8"}
	if !shard3.ContainPrefix("carry,location=beijing") {
		t.Fatalf("shard3 should contain prefix carry,location=beijing")
	}

	shard4 := ShardInfo{Min: "cpu,hostname=host1", Max: "cpu,hostname=host21"}
	if shard4.ContainPrefix("carry,location=beijing") {
		t.Fatalf("shard4 should not contain prefix carry,location=beijing")
	}

	if !shard4.ContainPrefix("cpu,hostname=host11") {
		t.Fatalf("shard4 should contain prefix cpu,hostname=host11")
	}
}

func TestShardInfo_Contain(t *testing.T) {
	shard1 := ShardInfo{Min: "", Max: ""}
	if !shard1.Contain("cpu") {
		t.Fatalf("shard1 should contain cpu")
	}

	shard2 := ShardInfo{Min: "", Max: "cpu,hostname=host8"}
	if !shard2.Contain("cpu,hostname=host1") {
		t.Fatalf("shard2 should contain shard key cpu,hostname=host1")
	}

	if shard2.Contain("demo") {
		t.Fatalf("shard2 should not contain shard key demo")
	}

	shard3 := ShardInfo{Min: "cpu,hostname=host8", Max: "mem,hostname=host1"}
	if !shard3.Contain("cpu,hostname=host8") {
		t.Fatalf("shard3 should contain shard key cpu,hostname=host8")
	}

	if shard3.Contain("mem,hostname=host1") {
		t.Fatalf("shard3 should not contain shard key mem,hostname=host1")
	}
}

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

func TestData_CreateRetentionPolicy(t *testing.T) {
	data := initData()
	dbName := "test"
	rpName := "default"
	mstName := "foo"
	rpi := &RetentionPolicyInfo{
		Name:               rpName,
		ReplicaN:           1,
		ShardGroupDuration: 12 * time.Hour,
		IndexGroupDuration: 30 * 365 * 24 * time.Hour}
	err := data.CreateDatabase(dbName, rpi, nil, false, 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = data.CreateMeasurement(dbName, rpName, mstName, &proto2.ShardKeyInfo{
		ShardKey: []string{"hostname"},
		Type:     proto.String(influxql.HASH),
	}, 0, nil, 0, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	insertTime := mustParseTime(time.RFC3339Nano, "2022-06-14T10:20:00Z")
	err = data.CreateShardGroup(dbName, rpName, insertTime, util.Hot, config.TSSTORE, 0)
	if err != nil {
		t.Fatal(err)
	}
	sg, err := data.ShardGroupByTimestampAndEngineType(dbName, rpName, insertTime, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	assert(sg.StartTime.Equal(mustParseTime(time.RFC3339Nano, "2022-06-14T00:00:00Z")), "shard group startTime error")
	assert(sg.EndTime.Equal(mustParseTime(time.RFC3339Nano, "2022-06-14T12:00:00Z")), "shard group endTime error")
	igs := data.Database(dbName).RetentionPolicy(rpName).IndexGroups
	assert(igs[0].StartTime.Equal(mustParseTime(time.RFC3339Nano, "2009-09-01T00:00:00Z")), "index group startTime error")
	assert(igs[0].EndTime.Equal(mustParseTime(time.RFC3339Nano, "2039-08-25T00:00:00Z")), "index group endTime error")
}

func TestShardGroupOutOfOrder(t *testing.T) {
	data := Data{}
	data.PtNumPerNode = 1
	DataLogger = logger.New(os.Stderr)
	_, err := data.CreateDataNode("127.0.0.1:8400", "127.0.0.1:8401", "", "")
	if err != nil {
		t.Fatal(err)
	}
	dbName := "test"
	rpName := "default"
	mstName := "foo"
	err = data.CreateDatabase(dbName, nil, nil, false, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	rpi := &RetentionPolicyInfo{
		Name:               rpName,
		ReplicaN:           1,
		ShardGroupDuration: 12 * time.Hour,
		IndexGroupDuration: 24 * time.Hour}
	err = data.CreateRetentionPolicy(dbName, rpi, true)
	if err != nil {
		t.Fatal(err)
	}

	err = data.CreateMeasurement(dbName, rpName, mstName, &proto2.ShardKeyInfo{
		ShardKey: []string{"hostname"},
		Type:     proto.String(influxql.HASH),
	}, 0, nil, 0, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	insertTime := mustParseTime(time.RFC3339Nano, "2021-11-26T13:00:00Z")
	err = data.CreateShardGroup(dbName, rpName, insertTime, util.Hot, config.TSSTORE, 0)
	if err != nil {
		t.Fatal(err)
	}
	sg, err := data.ShardGroupByTimestampAndEngineType(dbName, rpName, insertTime, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	assert(sg.StartTime.Equal(mustParseTime(time.RFC3339Nano, "2021-11-26T12:00:00Z")), "shard group startTime error")
	assert(sg.EndTime.Equal(mustParseTime(time.RFC3339Nano, "2021-11-27T00:00:00Z")), "shard group endTime error")
	igs := data.Database(dbName).RetentionPolicy(rpName).IndexGroups
	assert(len(igs) == 1, "err num of index groups")
	assert(igs[0].StartTime.Equal(mustParseTime(time.RFC3339Nano, "2021-11-26T00:00:00Z")), "index group startTime error")
	assert(igs[0].EndTime.Equal(mustParseTime(time.RFC3339Nano, "2021-11-27T00:00:00Z")), "index group endTime error")
	insertTime = mustParseTime(time.RFC3339Nano, "2021-11-25T13:00:00Z")
	sg, err = data.ShardGroupByTimestampAndEngineType(dbName, rpName, insertTime, config.TSSTORE)
	require.NoError(t, err)
	assert(sg == nil, "shard group contain time %v should not exist", insertTime)
	err = data.CreateShardGroup(dbName, rpName, insertTime, util.Hot, config.TSSTORE, 0)
	if err != nil {
		t.Fatal(err)
	}
	sg, err = data.ShardGroupByTimestampAndEngineType(dbName, rpName, insertTime, config.TSSTORE)
	require.NoError(t, err)
	assert(sg != nil, "shard group contain time %v should exist", insertTime)
	index := sg.Shards[0].IndexID
	igs = data.Database(dbName).RetentionPolicy(rpName).IndexGroups
	assert(len(igs) == 2, "err num of index groups")
	assert(index == igs[0].Indexes[0].ID, "shard owner index wrong actual indexID %d expect indexID %d", index, igs[0].Indexes[0].ID)
}

func TestData_CreateShardGroup(t *testing.T) {
	data := initData()
	dbName := "test"
	rpName := "default"
	mstName := "foo"
	err := data.CreateDatabase(dbName, nil, nil, false, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = data.CreateDBPtView(dbName)
	if err != nil {
		t.Fatal(err)
	}
	rpi := &RetentionPolicyInfo{
		Name:               rpName,
		ReplicaN:           1,
		ShardGroupDuration: 1 * time.Hour,
		IndexGroupDuration: 2 * time.Hour}
	err = data.CreateRetentionPolicy(dbName, rpi, true)
	if err != nil {
		t.Fatal(err)
	}

	err = data.CreateMeasurement(dbName, rpName, mstName, &proto2.ShardKeyInfo{
		ShardKey: []string{"hostname"},
		Type:     proto.String(influxql.HASH),
	}, 0, nil, 0, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	insertTime := mustParseTime(time.RFC3339Nano, "2022-06-08T09:00:00Z")
	err = data.CreateShardGroup(dbName, rpName, insertTime, util.Hot, config.TSSTORE, 0)
	if err != nil {
		t.Fatal(err)
	}
	sg, err := data.ShardGroupByTimestampAndEngineType(dbName, rpName, insertTime, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	assert(sg.StartTime.Equal(mustParseTime(time.RFC3339Nano, "2022-06-08T09:00:00Z")), "shard group startTime error")
	assert(sg.EndTime.Equal(mustParseTime(time.RFC3339Nano, "2022-06-08T10:00:00Z")), "shard group endTime error")
	igs := data.Database(dbName).RetentionPolicy(rpName).IndexGroups
	assert(len(igs) == 1, "err num of index groups")
	assert(igs[0].StartTime.Equal(mustParseTime(time.RFC3339Nano, "2022-06-08T08:00:00Z")), "index group startTime error")
	assert(igs[0].EndTime.Equal(mustParseTime(time.RFC3339Nano, "2022-06-08T10:00:00Z")), "index group endTime error")
	data.ExpandShardsEnable = true
	_, err = data.CreateDataNode("127.0.0.3:8400", "127.0.0.3:8401", "", "")
	if err != nil {
		t.Fatal(err)
	}
	insertTime = mustParseTime(time.RFC3339Nano, "2022-06-08T08:30:00Z")
	err = data.CreateShardGroup(dbName, rpName, insertTime, util.Hot, config.TSSTORE, 0)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDatabase_Clone(t *testing.T) {
	data := initDataWithDataNode()
	dbName := "testDb"
	rpName := "cloneRp"
	if err := data.CreateDatabase(dbName, nil, nil, false, 1, nil); err != nil {
		t.Fatal(err)
	}
	rpi := &RetentionPolicyInfo{Name: rpName, ReplicaN: 1, ShardGroupDuration: 24 * time.Hour, Duration: 7 * 24 * time.Hour}
	if err := data.CreateRetentionPolicy(dbName, rpi, true); err != nil {
		t.Fatal(err)
	}
	dbi := data.Database(dbName)
	cloneDbi := dbi.clone()
	cloneDbi.MarkDeleted = true
	assert(cloneDbi.MarkDeleted, "Clone database should mark delete")
	assert(!dbi.MarkDeleted, "origin database should not change")
	assert(cloneDbi.RetentionPolicy(rpName) != nil, "rp should be Clone")

	testRp := "testRp"
	rpNew := &RetentionPolicyInfo{Name: testRp, ReplicaN: 1, ShardGroupDuration: 24 * time.Hour, Duration: 7 * 24 * time.Hour}
	if err := data.CreateRetentionPolicy(dbName, rpNew, true); err != nil {
		t.Fatal(err)
	}
	assert(dbi.RetentionPolicy(testRp) != nil, "origin database should have rp")
	assert(cloneDbi.RetentionPolicy(testRp) == nil, "Clone database should not have rp")
}

func TestData_MarkRetentionPolicyDelete(t *testing.T) {
	data := initData()
	dbName := "testDb"
	err := data.CreateDatabase(dbName, nil, nil, false, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	rpName := "testRp"
	rpi := &RetentionPolicyInfo{Name: rpName, ReplicaN: 1, ShardGroupDuration: 24 * time.Hour, Duration: 7 * 24 * time.Hour}
	err = data.CreateRetentionPolicy(dbName, rpi, true)
	if err != nil {
		t.Fatal(err)
	}
	err = data.MarkRetentionPolicyDelete(dbName, rpName)
	if err != nil {
		t.Fatal(err)
	}
	_, err = data.RetentionPolicy(dbName, rpName)
	assert(err == ErrRetentionPolicyIsBeingDelete, "rp should being delete")
}

func TestData_createIndexGroupIfNeeded(t *testing.T) {
	data := initData()
	duration := 24 * time.Hour
	now := time.Now().Truncate(duration).UTC()
	rpi := &RetentionPolicyInfo{
		IndexGroupDuration: duration,
		IndexGroups: IndexGroupInfos{
			{
				ID:        0,
				StartTime: now,
				EndTime:   now.Add(duration),
				Indexes:   make([]IndexInfo, 2),
			},
			{
				ID:        1,
				StartTime: now.Add(duration),
				EndTime:   now.Add(2 * duration),
				Indexes:   make([]IndexInfo, 2),
			},
			{
				ID:        2,
				StartTime: now.Add(2 * duration),
				EndTime:   now.Add(3 * duration),
				Indexes:   make([]IndexInfo, 2),
			},
		}}
	ptNum := data.GetClusterPtNum()
	ig := data.createIndexGroupIfNeeded(rpi, now.Add(-time.Hour), config.TSSTORE, ptNum)
	require.Equal(t, 4, len(rpi.IndexGroups))
	require.Equal(t, &rpi.IndexGroups[0], ig)

	ig = data.createIndexGroupIfNeeded(rpi, now.Add(time.Hour), config.TSSTORE, ptNum)
	require.Equal(t, 4, len(rpi.IndexGroups))
	require.Equal(t, &rpi.IndexGroups[1], ig)

	ig = data.createIndexGroupIfNeeded(rpi, now.Add(duration+time.Hour), config.TSSTORE, ptNum)
	require.Equal(t, 4, len(rpi.IndexGroups))
	require.Equal(t, &rpi.IndexGroups[2], ig)

	ig = data.createIndexGroupIfNeeded(rpi, now.Add(2*duration+time.Hour), config.TSSTORE, ptNum)
	require.Equal(t, 4, len(rpi.IndexGroups))
	require.Equal(t, &rpi.IndexGroups[3], ig)

	ig = data.createIndexGroupIfNeeded(rpi, now.Add(3*duration+time.Hour), config.TSSTORE, ptNum)
	require.Equal(t, 5, len(rpi.IndexGroups))
	require.Equal(t, &rpi.IndexGroups[4], ig)

	rpi.IndexGroups = nil
	ig = data.createIndexGroupIfNeeded(rpi, now, config.TSSTORE, ptNum)
	require.Equal(t, 1, len(rpi.IndexGroups))
	require.Equal(t, &rpi.IndexGroups[0], ig)
}

func initDataWithDataNode() *Data {
	data := &Data{PtNumPerNode: 1}
	DataLogger = logger.New(os.Stderr)
	prefix := "127.0.0."
	selectPort := ":8401"
	writePort := ":8400"

	for i := 1; i <= 40; i++ {
		nodeIp := prefix + fmt.Sprint(i)
		_, err := data.CreateDataNode(nodeIp+fmt.Sprint(selectPort), nodeIp+fmt.Sprint(writePort), "", "")
		if err != nil {
			panic(err)
		}
	}
	return data
}

func BenchmarkData_CreateDatabase(b *testing.B) {
	data := initDataWithDataNode()

	n := 10000
	dbPrefix := "db"
	databases := make([]string, n)
	for i := 1; i <= n; i++ {
		dbName := dbPrefix + fmt.Sprint(i)
		databases[i-1] = dbName
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = data.CreateDatabase(databases[i%n], nil, nil, false, 1, nil)
	}
	b.StopTimer()
}

func BenchmarkData_CreateRetentionPolicy(b *testing.B) {
	data := initDataWithDataNode()
	n := 10000
	dbPrefix := "db"
	databases := make([]string, n)
	rpName := "testRp"
	rps := make([]*RetentionPolicyInfo, n)
	for i := 1; i <= n; i++ {
		dbName := dbPrefix + fmt.Sprint(i)
		err := data.CreateDatabase(dbName, nil, nil, false, 1, nil)
		if err != nil {
			b.Fatal(err)
		}
		databases[i-1] = dbName
		rps[i-1] = &RetentionPolicyInfo{Name: rpName, ReplicaN: 1, ShardGroupDuration: 24 * time.Hour, Duration: 7 * 24 * time.Hour}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data.CreateRetentionPolicy(databases[i%n], rps[i%n], true)
	}
	b.StopTimer()
}

func BenchmarkData_CreateMeasurement(b *testing.B) {
	data := initDataWithDataNode()
	n := 10000
	dbPrefix := "db"
	databases := make([]string, n)
	rpName := "testRp"
	mstName := "testMst"
	for i := 1; i <= n; i++ {
		dbName := dbPrefix + fmt.Sprint(i)
		err := data.CreateDatabase(dbName, nil, nil, false, 1, nil)
		if err != nil {
			b.Fatal(err)
		}
		databases[i-1] = dbName
		rpi := &RetentionPolicyInfo{Name: rpName, ReplicaN: 1, ShardGroupDuration: 24 * time.Hour, Duration: 7 * 24 * time.Hour}
		if err := data.CreateRetentionPolicy(dbName, rpi, true); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = data.CreateMeasurement(databases[i%n], rpName, mstName, nil, 0, nil, 0, nil, nil, nil)
	}
	b.StopTimer()
}

func BenchmarkData_CreateShardGroup(b *testing.B) {
	data := initDataWithDataNode()
	n := 10000
	dbPrefix := "db"
	databases := make([]string, n)
	rpName := "testRp"
	mstName := "testMst"
	times := make([]time.Time, n)
	currentTime := time.Now()
	for i := 1; i <= n; i++ {
		dbName := dbPrefix + fmt.Sprint(i)
		err := data.CreateDatabase(dbName, nil, nil, false, 1, nil)
		if err != nil {
			b.Fatal(err)
		}
		databases[i-1] = dbName
		rpi := &RetentionPolicyInfo{Name: rpName, ReplicaN: 1, ShardGroupDuration: 24 * time.Hour, Duration: 7 * 24 * time.Hour}
		if err := data.CreateRetentionPolicy(dbName, rpi, true); err != nil {
			b.Fatal(err)
		}
		if err = data.CreateMeasurement(dbName, rpName, mstName, &proto2.ShardKeyInfo{
			ShardKey: []string{"hostname"},
			Type:     proto.String(influxql.HASH),
		}, 0, nil, 0, nil, nil, nil); err != nil {
			b.Fatal(err)
		}

		times[i-1] = currentTime.Add(24 * time.Hour)
		currentTime = times[i-1]
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = data.CreateShardGroup(databases[i%n], rpName, times[i%n], util.Hot, config.TSSTORE, 0)
	}
	b.StopTimer()
}

func BenchmarkData_DeleteShardGroup(b *testing.B) {
	data := initDataWithDataNode()
	n := 10000
	dbPrefix := "db"
	databases := make([]string, n)
	rpName := "testRp"
	mstName := "testMst"
	sgIds := make([]uint64, n)
	currentTime := time.Now()
	for i := 1; i <= n; i++ {
		dbName := dbPrefix + fmt.Sprint(i)
		err := data.CreateDatabase(dbName, nil, nil, false, 1, nil)
		if err != nil {
			b.Fatal(err)
		}
		databases[i-1] = dbName
		rpi := &RetentionPolicyInfo{Name: rpName, ReplicaN: 1, ShardGroupDuration: 24 * time.Hour, Duration: 7 * 24 * time.Hour}
		if err := data.CreateRetentionPolicy(dbName, rpi, true); err != nil {
			b.Fatal(err)
		}
		if err = data.CreateMeasurement(dbName, rpName, mstName, &proto2.ShardKeyInfo{
			ShardKey: []string{"hostname"},
			Type:     proto.String(influxql.HASH),
		}, 0, nil, 0, nil, nil, nil); err != nil {
			b.Fatal(err)
		}

		if err = data.CreateShardGroup(dbName, rpName, currentTime, util.Hot, config.TSSTORE, 0); err != nil {
			b.Fatal(err)
		}
		var sg *ShardGroupInfo
		if sg, err = data.ShardGroupByTimestampAndEngineType(dbName, rpName, currentTime, config.TSSTORE); err != nil {
			b.Fatal(err)
		}
		sgIds[i-1] = sg.ID
		currentTime = currentTime.Add(24 * time.Hour)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data.DeleteShardGroup(databases[i%n], rpName, sgIds[i%n], 0, MarkDelete)
		//data.pruneShardGroups()
	}
	b.StopTimer()
}

func BenchmarkData_DropDatabase(b *testing.B) {
	data := initDataWithDataNode()

	n := 10000
	dbPrefix := "db"
	databases := make([]string, n)
	for i := 1; i <= n; i++ {
		dbName := dbPrefix + fmt.Sprint(i)
		databases[i-1] = dbName
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data.DropDatabase(databases[i%n])
	}
	b.StopTimer()
}

func BenchmarkData_MarkDatabaseDelete(b *testing.B) {
	data := initDataWithDataNode()

	n := 10000
	dbPrefix := "db"
	databases := make([]string, n)
	for i := 1; i <= n; i++ {
		dbName := dbPrefix + fmt.Sprint(i)
		databases[i-1] = dbName
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data.MarkDatabaseDelete(databases[i%n])
	}
	b.StopTimer()
}

func BenchmarkData_UpdateSchema(b *testing.B) {
	data := initDataWithDataNode()
	n := 10000
	dbPrefix := "db"
	databases := make([]string, n)
	rpName := "testRp"
	mstName := "testMst"
	fieldNum := 5

	schemas := make([][]*proto2.FieldSchema, n)
	for i := 1; i <= n; i++ {
		dbName := dbPrefix + fmt.Sprint(i)
		err := data.CreateDatabase(dbName, nil, nil, false, 1, nil)
		if err != nil {
			b.Fatal(err)
		}
		databases[i-1] = dbName
		rpi := &RetentionPolicyInfo{Name: rpName, ReplicaN: 1, ShardGroupDuration: 24 * time.Hour, Duration: 7 * 24 * time.Hour}
		if err := data.CreateRetentionPolicy(dbName, rpi, true); err != nil {
			b.Fatal(err)
		}
		if err = data.CreateMeasurement(dbName, rpName, mstName, nil, 0, nil, 0, nil, nil, nil); err != nil {
			b.Fatal(err)
		}
		tags := make([]*proto2.FieldSchema, fieldNum)
		for i := range tags {
			tags[i] = &proto2.FieldSchema{FieldName: proto.String("tk1"), FieldType: proto.Int32(influx.Field_Type_Tag)}
		}
		schemas[i-1] = tags
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data.UpdateSchema(databases[i%n], rpName, mstName, schemas[i%n])
	}
	b.StopTimer()
}

func BenchmarkData_ShardGroupsByTimeRange(b *testing.B) {
	data := initDataWithDataNode()
	dbPrefix := "db"
	rpName := "testRp"
	mstName := "testMst"
	n := 10000
	retainSgNum := 7
	currentTime := time.Now()
	startTimes := make([]time.Time, n)
	endTimes := make([]time.Time, n)
	databases := make([]string, n)
	for i := 1; i <= n; i++ {
		dbName := dbPrefix + fmt.Sprint(i)
		err := data.CreateDatabase(dbName, nil, nil, false, 1, nil)
		if err != nil {
			b.Fatal(err)
		}
		rpi := &RetentionPolicyInfo{Name: rpName, ReplicaN: 1, ShardGroupDuration: 24 * time.Hour, Duration: 7 * 24 * time.Hour}
		if err := data.CreateRetentionPolicy(dbName, rpi, true); err != nil {
			b.Fatal(err)
		}

		err = data.CreateMeasurement(dbName, rpName, mstName, &proto2.ShardKeyInfo{
			ShardKey: []string{"hostname"},
			Type:     proto.String(influxql.HASH),
		}, 0, nil, 0, nil, nil, nil)
		if err != nil {
			b.Fatal(err)
		}

		databases[i-1] = dbName
		currentTime = time.Now()
		startTimes[i-1] = currentTime
		endTimes[i-1] = currentTime.Add(3 * 24 * time.Hour)
		for j := 0; j < retainSgNum; j++ {
			err = data.CreateShardGroup(dbName, rpName, currentTime, util.Hot, config.TSSTORE, 0)
			if err != nil {
				b.Fatal(err)
			}
			currentTime = currentTime.Add(24 * time.Hour)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data.ShardGroupsByTimeRange(databases[i%n], rpName, startTimes[i%n], endTimes[i%n])
	}
	b.StopTimer()
}

func TestBigMetaDataWith_400thousandMeasurements(t *testing.T) {
	data := initDataWithDataNode()

	if got, exp := len(data.DataNodes), 40; got != exp {
		t.Fatalf("wrong dataNodes len, got %d exp %d", got, exp)
	}

	dbPrefix := "db"
	rpName := "testRp"
	mstName := "testMst"
	n := 10000
	retainSgNum := 7
	currentTime := time.Now()
	for i := 1; i <= n; i++ {
		dbName := dbPrefix + fmt.Sprint(i)
		err := data.CreateDatabase(dbName, nil, nil, false, 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		rpi := &RetentionPolicyInfo{Name: rpName, ReplicaN: 1, ShardGroupDuration: 24 * time.Hour, Duration: 7 * 24 * time.Hour}
		if err := data.CreateRetentionPolicy(dbName, rpi, true); err != nil {
			t.Fatal(err)
		}

		err = data.CreateMeasurement(dbName, rpName, mstName, &proto2.ShardKeyInfo{
			ShardKey: []string{"hostname"},
			Type:     proto.String(influxql.HASH),
		}, 0, nil, 0, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		currentTime = time.Now()
		for j := 0; j < retainSgNum; j++ {
			err = data.CreateShardGroup(dbName, rpName, currentTime, util.Hot, config.TSSTORE, 0)
			if err != nil {
				t.Fatal(err)
			}
			currentTime = currentTime.Add(24 * time.Hour)
		}

		sgs, err := data.ShardGroups(dbName, rpName)
		assert(len(sgs) == retainSgNum, "expect shard groups num %d got %d", retainSgNum, len(sgs))
	}

	if got, exp := len(data.Databases), n; !reflect.DeepEqual(got, exp) {
		t.Fatalf("wrong database num got %d exp %d", got, exp)
	}
	PrintMemUsage()
}

func generateDbPtInfo(db string, ptId uint32, ownerId uint64, dbBriefInfo *DatabaseBriefInfo) *DbPtInfo {
	return &DbPtInfo{Db: db, Pti: &PtInfo{
		PtId:   ptId,
		Status: Offline,
		Owner:  PtOwner{ownerId}},
		DBBriefInfo: dbBriefInfo,
	}
}

func TestData_CloneMigrateEvents(t *testing.T) {
	data := &Data{MigrateEvents: make(map[string]*MigrateEventInfo)}
	dbBriefInfo := &DatabaseBriefInfo{Name: "db0", EnableTagArray: false}
	dbPt1 := generateDbPtInfo("db0", 0, 1, dbBriefInfo)
	mei := NewMigrateEventInfo(dbPt1.String(), 1, dbPt1, 1, 1)
	data.MigrateEvents[mei.eventId] = mei
	dataClone := data.Clone()
	assert2.Equal(t, dataClone.MigrateEvents[mei.eventId] != nil, true)
	assert2.Equal(t, dataClone.MigrateEvents[mei.eventId].opId, mei.opId)
	assert2.Equal(t, dataClone.MigrateEvents[mei.eventId].src, mei.src)
	assert2.Equal(t, dataClone.MigrateEvents[mei.eventId].dest, mei.dest)
	assert2.Equal(t, dataClone.MigrateEvents[mei.eventId].currState, mei.currState)
}

func TestData_Marshal_Unmarshal(t *testing.T) {
	data := &Data{MigrateEvents: make(map[string]*MigrateEventInfo)}

	dbBriefInfo := &DatabaseBriefInfo{Name: "db0", EnableTagArray: false}
	dbPt1 := generateDbPtInfo("db0", 0, 1, dbBriefInfo)
	mei := NewMigrateEventInfo(dbPt1.String(), 1, dbPt1, 1, 1)
	data.MigrateEvents[mei.eventId] = mei
	pb := data.Marshal(false)
	dataUnMarshal := &Data{}
	dataUnMarshal.Unmarshal(pb)

	assert2.Equal(t, dataUnMarshal.MigrateEvents[mei.eventId] != nil, true)
	assert2.Equal(t, dataUnMarshal.MigrateEvents[mei.eventId].opId, mei.opId)
	assert2.Equal(t, dataUnMarshal.MigrateEvents[mei.eventId].src, mei.src)
	assert2.Equal(t, dataUnMarshal.MigrateEvents[mei.eventId].dest, mei.dest)
	assert2.Equal(t, dataUnMarshal.MigrateEvents[mei.eventId].currState, mei.currState)
}

func TestData_CreateMigrateEvent(t *testing.T) {
	data := &Data{}
	dbBriefInfo := &DatabaseBriefInfo{Name: "db0", EnableTagArray: false}
	dbPt1 := generateDbPtInfo("db0", 0, 1, dbBriefInfo)
	pb := &proto2.MigrateEventInfo{EventId: proto.String(dbPt1.String()),
		Dest:      proto.Uint64(1),
		Pti:       dbPt1.Marshal(),
		EventType: proto.Int(1)}
	err := data.CreateMigrateEvent(pb)
	assert2.NoError(t, err)
	assert2.Equal(t, uint64(1), data.MigrateEvents[dbPt1.String()].opId)
}

func TestData_CqReport(t *testing.T) {
	data := &Data{
		Databases: map[string]*DatabaseInfo{
			"db0": {
				Name: "db0",
				ContinuousQueries: map[string]*ContinuousQueryInfo{
					"cq0": {
						Name:        "cq0",
						Query:       `CREATE CONTINUOUS QUERY "cq0" ON "db0" RESAMPLE EVERY 2h FOR 30m BEGIN SELECT max("passengers") INTO "max_passengers" FROM "bus_data" GROUP BY time(10m) END`,
						LastRunTime: time.Time{},
					},
				},
			},
		},
	}
	ts := time.Now()
	cqStat := []*proto2.CQState{
		{
			Name:        proto.String("cq0"),
			LastRunTime: proto.Int64(ts.UnixNano()),
		},
	}
	err := data.BatchUpdateContinuousQueryStat(cqStat)
	assert2.NoError(t, err)

	lastRunTime := data.Databases["db0"].ContinuousQueries["cq0"].LastRunTime
	if !ts.Equal(lastRunTime) {
		t.Fatal()
	}

	cqStat = []*proto2.CQState{
		{
			Name:        proto.String("cq1"),
			LastRunTime: proto.Int64(ts.UnixNano()),
		},
	}
	err = data.BatchUpdateContinuousQueryStat(cqStat) // No cq1
	assert2.NoError(t, err)
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %.2f MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %.2f MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %.2f MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) float32 {
	return float32(b) / 1024. / 1024.
}

func TestUpdateShardInfo(t *testing.T) {
	data := &Data{
		Databases: map[string]*DatabaseInfo{
			"db0": {
				Name: "db0",
				RetentionPolicies: map[string]*RetentionPolicyInfo{
					"rp0": {
						ShardGroups: []ShardGroupInfo{{Shards: []ShardInfo{
							{ID: 1},
						}}},
					},
				},
			},
		},
	}
	data.UpdateShardDownSampleInfo(&ShardIdentifier{
		OwnerDb:         "db0",
		Policy:          "rp0",
		ShardID:         1,
		DownSampleID:    2,
		DownSampleLevel: 2,
	})
	s := data.Databases["db0"].RetentionPolicies["rp0"].ShardGroups[0].Shards[0]
	if s.DownSampleID != 2 || s.DownSampleLevel != 2 {
		t.Fatal()
	}
}

func TestData_RegisterQueryIDOffset(t *testing.T) {
	type fields struct {
		QueryIDInit map[SQLHost]uint64
	}
	type args struct {
		host SQLHost
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   uint64
	}{
		{
			name:   "Success",
			fields: fields{QueryIDInit: map[SQLHost]uint64{"127.0.0.1:1234": 0, "127.0.0.2:1234": QueryIDSpan}},
			args:   args{host: "127.0.0.1:7890"},
			want:   QueryIDSpan * 2,
		},
		{
			name:   "DuplicateRegister",
			fields: fields{QueryIDInit: map[SQLHost]uint64{"127.0.0.1:1234": 0}},
			args:   args{host: "127.0.0.1:1234"},
			want:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := &Data{
				QueryIDInit: tt.fields.QueryIDInit,
			}
			err := data.RegisterQueryIDOffset(tt.args.host)
			assert2.NoError(t, err)
			assert(data.QueryIDInit[tt.args.host] == tt.want, "register failed")
		})
	}
}

func TestData_Create_Subscription(t *testing.T) {
	data := &Data{
		Databases: map[string]*DatabaseInfo{
			"db0": {
				Name: "db0",
				RetentionPolicies: map[string]*RetentionPolicyInfo{
					"rp0": {},
				},
			},
		},
	}
	destinations := []string{"http://192.168.35.1:8080", "http://10.123.65.4:9172"}
	err := data.CreateSubscription("db0", "rp0", "subs1", "ALL", destinations)
	assert2.NoError(t, err)
	err = data.CreateSubscription("db0", "rp0", "subs1", "ALL", destinations)
	assert2.Equal(t, err == ErrSubscriptionExists, true)
	err = data.CreateSubscription("db2", "rp0", "subs1", "ALL", destinations)
	assert2.Equal(t, err != nil, true)
	rpi, err := data.RetentionPolicy("db0", "rp0")
	assert2.NoError(t, err)
	assert2.Equal(t, len(rpi.Subscriptions), 1)
	assert2.Equal(t, rpi.Subscriptions[0].Name, "subs1")
	assert2.Equal(t, rpi.Subscriptions[0].Mode, "ALL")
	for i := range destinations {
		assert2.Equal(t, rpi.Subscriptions[0].Destinations[i], destinations[i])
	}
}

func TestData_Drop_Subscription(t *testing.T) {
	Databases := make(map[string]*DatabaseInfo)
	for i := 0; i < 5; i++ {
		db := fmt.Sprintf(`db%v`, i)
		rp := fmt.Sprintf(`rp%v`, i)
		Databases[db] = &DatabaseInfo{
			Name: db,
			RetentionPolicies: map[string]*RetentionPolicyInfo{
				rp: {},
			},
		}
	}
	data := &Data{Databases: Databases}
	for i := 0; i < 5; i++ {
		db := fmt.Sprintf(`db%v`, i)
		rp := fmt.Sprintf(`rp%v`, i)
		for j := 0; j < 3; j++ {
			subs := fmt.Sprintf(`subs%v`, j)
			err := data.CreateSubscription(db, rp, subs, "All", []string{"http://192.168.35.1:8080"})
			assert2.NoError(t, err)
		}
	}

	// drop all subscriptions on db0
	err := data.DropSubscription("db0", "", "")
	assert2.NoError(t, err)
	rpi, err := data.RetentionPolicy("db0", "rp0")
	assert2.NoError(t, err)
	assert2.Equal(t, len(rpi.Subscriptions), 0)

	// drop subs2 on db2.rp2, but do not specify rp
	err = data.DropSubscription("db2", "", "subs2")
	assert2.NoError(t, err)

	// drop subs1 on db3.rp3
	err = data.DropSubscription("db3", "rp3", "subs1")
	assert2.NoError(t, err)
	rpi, err = data.RetentionPolicy("db3", "rp3")
	assert2.NoError(t, err)
	assert2.Equal(t, len(rpi.Subscriptions), 2)
	for i := 0; i < 2; i++ {
		assert2.NotEqual(t, rpi.Subscriptions[i].Name, "subs1")
	}

	// try to drop non-exist subscription
	err = data.DropSubscription("db2", "rp2", "subs10")
	assert2.Equal(t, err != nil, true)

	// drop all subscriptions
	rows := data.ShowSubscriptions()
	assert2.Equal(t, rows.Len(), 4)
	err = data.DropSubscription("", "", "")
	assert2.NoError(t, err)
	rows = data.ShowSubscriptions()
	assert2.Equal(t, rows.Len(), 0)

	// try drop subscription when there is no subscription
	err = data.DropSubscription("db0", "rp0", "")
	assert2.NoError(t, err)
	err = data.DropSubscription("", "", "")
	assert2.NoError(t, err)
}

func TestExpandGroups(t *testing.T) {
	data := &Data{
		PtNumPerNode: 3,
		ClusterPtNum: 3,
		Databases: map[string]*DatabaseInfo{
			"database2": {
				Name: "database2",
				RetentionPolicies: map[string]*RetentionPolicyInfo{
					"rp1": {
						ShardGroups: []ShardGroupInfo{
							{Shards: []ShardInfo{{ID: 1}}},
							{Shards: []ShardInfo{{ID: 2}}},
						},
					},
					"rp0": {
						ShardGroups: []ShardGroupInfo{
							{Shards: []ShardInfo{{ID: 3}}},
							{Shards: []ShardInfo{{ID: 4}}},
						},
					},
				},
			},
			"database0": {
				Name: "database0",
				RetentionPolicies: map[string]*RetentionPolicyInfo{
					"rp2": {
						ShardGroups: []ShardGroupInfo{
							{Shards: []ShardInfo{{ID: 5}}},
							{Shards: []ShardInfo{{ID: 6}}},
						},
					},
					"rp3": {
						ShardGroups: []ShardGroupInfo{
							{Shards: []ShardInfo{{ID: 7}}},
							{Shards: []ShardInfo{{ID: 8}}},
						},
					},
				},
			},
		},
		MaxShardID:         8,
		ExpandShardsEnable: true,
	}
	DataLogger = logger.New(os.Stderr)
	data.ExpandGroups()
	firstShardId := data.Databases["database0"].RetentionPolicies["rp2"].ShardGroups[0].Shards[1].ID
	if firstShardId != 9 {
		t.Fatalf("the shard id is wrong, expect: %d, real: %d", 1, firstShardId)
	}
	lastShardId := data.Databases["database2"].RetentionPolicies["rp1"].ShardGroups[1].Shards[2].ID
	if lastShardId != 24 {
		t.Fatalf("the shard id is wrong, expect: %d, real: %d", 24, lastShardId)
	}
}

func TestUpdateMeasurement(t *testing.T) {
	data := initData()
	dbName := "testDb"
	logStream := "testLogstream"
	if err := data.CreateDatabase(dbName, nil, nil, false, 1, nil); err != nil {
		t.Fatal(err)
	}
	var ttl int64 = 1
	options := &proto2.Options{Ttl: &ttl}
	rp := NewRetentionPolicyInfo(logStream)
	if err := data.CreateRetentionPolicy(dbName, rp, false); err != nil {
		t.Fatal(err)
	}
	if err := data.CreateMeasurement(dbName, logStream, logStream, nil, 0, nil, 0, nil, nil, options); err != nil {
		t.Fatal(err)
	}

	// 0 day
	ttl = 0
	options.Ttl = &ttl
	if err := data.UpdateMeasurement(dbName, logStream, logStream, options); err != nil {
		t.Fatal(err)
	}
	rpInfo, err := data.RetentionPolicy(dbName, logStream)
	if err != nil {
		t.Fatal(err)
	}
	if int64(rpInfo.Duration) != ttl*int64(time.Hour)*24 {
		t.Fatalf("duration update failed")
	}

	// 1 day
	ttl = int64(time.Hour) * 24
	options.Ttl = &ttl
	if err := data.UpdateMeasurement(dbName, logStream, logStream, options); err != nil {
		t.Fatal(err)
	}
	rpInfo, err = data.RetentionPolicy(dbName, logStream)
	if err != nil {
		t.Fatal(err)
	}
	if int64(rpInfo.Duration) != ttl {
		t.Fatalf("duration update failed")
	}

	// 3000 day
	ttl = 3000
	options.Ttl = &ttl
	if err := data.UpdateMeasurement(dbName, logStream, logStream, options); err != nil {
		t.Fatal(err)
	}
	rpInfo, err = data.RetentionPolicy(dbName, logStream)
	if err != nil {
		t.Fatal(err)
	}
	if int64(rpInfo.Duration) != ttl*int64(time.Hour)*24 {
		t.Fatalf("duration set failed")
	}
}

func TestInitDataNodePtView(t *testing.T) {
	data := &Data{}
	data.PtNumPerNode = 1
	data.DataNodes = append(data.DataNodes, DataNode{NodeInfo{ID: 5, Role: NodeWriter}, 0, 0, 0, ""})
	data.DataNodes = append(data.DataNodes, DataNode{NodeInfo{ID: 6, Role: NodeDefault}, 0, 0, 0, ""})
	data.DataNodes = append(data.DataNodes, DataNode{NodeInfo{ID: 7, Role: NodeReader}, 0, 0, 0, ""})
	data.initDataNodePtView(data.GetClusterPtNum())
	if data.ClusterPtNum != 2 {
		t.Fatalf("calculate ClusterPtNum failed")
	}
}

func TestData_mapShardsToMstWithNilShardIdx(t *testing.T) {
	DataLogger = logger1.GetLogger()
	data1 := &Data{Databases: make(map[string]*DatabaseInfo)}
	data1.Databases["db0"] = &DatabaseInfo{RetentionPolicies: make(map[string]*RetentionPolicyInfo)}
	data1.Databases["db0"].RetentionPolicies["rp0"] = &RetentionPolicyInfo{Measurements: make(map[string]*MeasurementInfo)}
	data1.Databases["db0"].RetentionPolicies["rp0"].Measurements["mst0"] = &MeasurementInfo{InitNumOfShards: 1}
	sg := &ShardGroupInfo{}
	data1.mapShardsToMst("db0", data1.Databases["db0"].RetentionPolicies["rp0"], sg)
	assert2.NotEqual(t, data1.Databases["db0"].RetentionPolicies["rp0"].Measurements["mst0"].ShardIdexes, nil)
}

func TestData_MstCopy(t *testing.T) {
	mst := &MeasurementInfo{}
	mst1 := mst.clone()
	assert2.Equal(t, 0, len(mst1.ShardIdexes))
	mst = &MeasurementInfo{ShardIdexes: make(map[uint64][]int)}
	mst.ShardIdexes[1] = []int{1}
	mst1 = mst.clone()
	assert2.NotEqual(t, 0, len(mst1.ShardIdexes))
}

func TestData_mapShardsToMst(t *testing.T) {
	type args struct {
		database string
		rpi      *RetentionPolicyInfo
		sgi      *ShardGroupInfo
	}
	data := &Data{PtNumPerNode: 1}
	DataLogger = logger.New(os.Stderr)
	data.CreateDataNode("127.0.0.1:8086", "127.0.0.1:8188", "", "")
	data.CreateDataNode("127.0.0.2:8086", "127.0.0.2:8188", "", "")
	data.CreateDataNode("127.0.0.3:8086", "127.0.0.3:8188", "", "")
	data.CreateDataNode("127.0.0.4:8086", "127.0.0.4:8188", "", "")
	data.CreateDataNode("127.0.0.5:8086", "127.0.0.5:8188", "", "")
	data.CreateDataNode("127.0.0.6:8086", "127.0.0.6:8188", "", "")
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	data.CreateDBPtView("foo")
	must(data.CreateDatabase("foo", nil, nil, false, 1, nil))
	rp := NewRetentionPolicyInfo("bar")
	rp.ShardGroupDuration = 24 * time.Hour
	must(data.CreateRetentionPolicy("foo", rp, false))
	must(data.CreateMeasurement("foo", "bar", "cpu",
		&proto2.ShardKeyInfo{ShardKey: []string{"hostname"}, Type: proto.String(influxql.HASH)}, 3, nil, 0, nil, nil, nil))
	tests := []struct {
		name   string
		fields Data
		args   args
		want   map[uint64][]int
	}{
		{
			name:   "test1",
			fields: *data,
			args: args{
				database: "foo",
				rpi:      rp,
				sgi: &ShardGroupInfo{
					ID: 1,
					Shards: []ShardInfo{
						{
							ID: 1,
						},
						{
							ID: 2,
						},
						{
							ID: 3,
						},
						{
							ID: 4,
						},
						{
							ID: 5,
						},
						{
							ID: 6,
						},
					},
				},
			},
			want: map[uint64][]int{
				1: {0, 1, 5},
			},
		},
		{
			name:   "test1",
			fields: *data,
			args: args{
				database: "foo",
				rpi:      rp,
				sgi: &ShardGroupInfo{
					ID: 1,
					Shards: []ShardInfo{
						{
							ID: 11,
						},
						{
							ID: 12,
						},
						{
							ID: 13,
						},
						{
							ID: 14,
						},
						{
							ID: 15,
						},
						{
							ID: 16,
						},
					},
				},
			},
			want: map[uint64][]int{
				1: {0, 1, 5},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := &Data{
				Term:               tt.fields.Term,
				Index:              tt.fields.Index,
				ClusterID:          tt.fields.ClusterID,
				ClusterPtNum:       tt.fields.ClusterPtNum,
				PtNumPerNode:       tt.fields.PtNumPerNode,
				NumOfShards:        tt.fields.NumOfShards,
				MetaNodes:          tt.fields.MetaNodes,
				DataNodes:          tt.fields.DataNodes,
				PtView:             tt.fields.PtView,
				ReplicaGroups:      tt.fields.ReplicaGroups,
				Databases:          tt.fields.Databases,
				Streams:            tt.fields.Streams,
				Users:              tt.fields.Users,
				MigrateEvents:      tt.fields.MigrateEvents,
				QueryIDInit:        tt.fields.QueryIDInit,
				AdminUserExists:    tt.fields.AdminUserExists,
				TakeOverEnabled:    tt.fields.TakeOverEnabled,
				BalancerEnabled:    tt.fields.BalancerEnabled,
				ExpandShardsEnable: tt.fields.ExpandShardsEnable,
				MaxNodeID:          tt.fields.MaxNodeID,
				MaxShardGroupID:    tt.fields.MaxShardGroupID,
				MaxShardID:         tt.fields.MaxShardID,
				MaxIndexGroupID:    tt.fields.MaxIndexGroupID,
				MaxIndexID:         tt.fields.MaxIndexID,
				MaxEventOpId:       tt.fields.MaxEventOpId,
				MaxDownSampleID:    tt.fields.MaxDownSampleID,
				MaxStreamID:        tt.fields.MaxStreamID,
				MaxConnID:          tt.fields.MaxConnID,
				MaxSubscriptionID:  tt.fields.MaxSubscriptionID,
				MaxCQChangeID:      tt.fields.MaxCQChangeID,
			}
			data.mapShardsToMst(tt.args.database, tt.args.rpi, tt.args.sgi)
			got := data.Databases["foo"].RetentionPolicies["bar"].Measurements["cpu_0000"].ShardIdexes
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mapShards got: %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapShards(t *testing.T) {
	type args struct {
		mstName     string
		shards      []ShardInfo
		numOfShards int32
		shardList   []int
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{
			name: "test1",
			args: args{
				mstName: "mst1",
				shards: []ShardInfo{
					{
						ID: 11,
					},
					{
						ID: 12,
					},
					{
						ID: 13,
					},
					{
						ID: 14,
					},
					{
						ID: 15,
					},
					{
						ID: 16,
					},
				},
				numOfShards: 2,
			},
			want: []int{4, 5},
		},
		{
			name: "test1",
			args: args{
				mstName: "mst2",
				shards: []ShardInfo{
					{
						ID: 11,
					},
					{
						ID: 12,
					},
					{
						ID: 13,
					},
					{
						ID: 14,
					},
					{
						ID: 15,
					},
					{
						ID: 16,
					},
				},
				numOfShards: 4,
			},
			want: []int{0, 2, 3, 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.args.shardList = mapShards(tt.args.mstName, tt.args.shards, tt.args.numOfShards)
		})
		got := tt.args.shardList
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("mapShards got: %v, want %v", got, tt.want)
		}
	}
}

func TestData_GetShardDurationsByDbPtForRetention(t *testing.T) {
	db := "foo"
	rp := "bar"

	data := NewMockData(db, rp)
	shards := data.GetShardDurationsByDbPtForRetention(db, 0)
	assert2.Equal(t, len(shards), 1)
	for k, v := range shards {
		assert2.Equal(t, k, uint64(5))
		assert2.Equal(t, v.Ident.ShardGroupID, uint64(2))
	}
}

func Test_SQLITE_SIMPLE(t *testing.T) {
	os.Remove("./simple.db")
	defer os.Remove("./simple.db")

	db, err := sql.Open("sqlite3", "./simple.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sqlStmt := `
	create table foo (id integer not null primary key, name text);
	delete from foo;
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare("insert into foo(id, name) values(?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < 100; i++ {
		_, err = stmt.Exec(i, fmt.Sprintf("hello world%03d", i))
		if err != nil {
			t.Fatal(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select id, name from foo")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err = db.Prepare("select name from foo where id = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var name string
	err = stmt.QueryRow("3").Scan(&name)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("delete from foo")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		t.Fatal(err)
	}

	rows, err = db.Query("select id, name from foo")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
}

func Test_SQLite_TESTSQL(t *testing.T) {
	os.Remove("./testsql.db")
	defer os.Remove("./testsql.db")

	db, err := sql.Open("sqlite3", "./testsql.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	query, err := os.ReadFile("./test.sql")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(string(query))
	if err != nil {
		t.Fatal(err)
	}

	// create database databases
	insertDB, err := db.Prepare("INSERT INTO dbs(name) values(?)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, err = insertDB.Exec(fmt.Sprintf("db%02d", i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// get database id
	getDBID, err := db.Prepare("SELECT id, name FROM dbs WHERE name = ?")
	if err != nil {
		t.Fatal(err)
	}
	var dbid int
	var rpid int32
	var mstid int32
	var name string
	err = getDBID.QueryRow("db01").Scan(&dbid, &name)
	if err != nil {
		t.Fatal(err)
	}

	// create retention policies
	insertRP, err := db.Prepare("INSERT INTO rps(db_id, name) values(?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, err = insertRP.Exec(dbid, fmt.Sprintf("rp%02d", i))
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = insertRP.Exec(3, fmt.Sprintf("rp%02d", 3))
	if err != nil {
		t.Fatal(err)
	}
	_, err = insertRP.Exec(3, fmt.Sprintf("rp%02d", 3))
	if err.Error() != "UNIQUE constraint failed: rps.db_id, rps.name" {
		t.Fatal(err)
	}
	_, err = insertRP.Exec(30, fmt.Sprintf("rp%02d", 3))
	if err.Error() != "FOREIGN KEY constraint failed" {
		t.Fatal(err)
	}

	// get retention policy id
	getRPID, err := db.Prepare("SELECT id, name FROM rps WHERE name = ? AND db_id = ?")
	if err != nil {
		t.Fatal(err)
	}
	err = getRPID.QueryRow("rp02", dbid).Scan(&rpid, &name)
	if err != nil {
		t.Fatal(err)
	}

	// create measurements
	insertMST, err := db.Prepare("INSERT INTO msts(rp_id, name) values(?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, err = insertMST.Exec(rpid, fmt.Sprintf("mst%02d", i))
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = insertMST.Exec(6, fmt.Sprintf("mst%02d", 0))
	if err != nil {
		t.Fatal(err)
	}
	_, err = insertMST.Exec(6, fmt.Sprintf("mst%02d", 0))
	if err.Error() != "UNIQUE constraint failed: msts.rp_id, msts.name" {
		t.Fatal(err)
	}
	_, err = insertMST.Exec(60, fmt.Sprintf("mst%02d", 0))
	if err.Error() != "FOREIGN KEY constraint failed" {
		t.Fatal(err)
	}

	// get measurement id
	getMSTID, err := db.Prepare("SELECT id, name FROM msts WHERE name = ? AND rp_id = ?")
	if err != nil {
		t.Fatal(err)
	}
	err = getMSTID.QueryRow("mst03", rpid).Scan(&mstid, &name)
	if err != nil {
		t.Fatal(err)
	}

	// create files
	insertFILE, err := db.Prepare("INSERT INTO files(mst_id, sequence, level, extent, merge) values(?, ?, ?, ?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, err = insertFILE.Exec(6, i, 0, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
	}

	// delete db
	deleteDB, err := db.Prepare("DELETE FROM dbs WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	_, err = deleteDB.Exec(dbid)
	if err != nil {
		t.Fatal(err)
	}

	// check delete
	var numDBs, numRPs, numMSTs int
	err = db.QueryRow("SELECT COUNT(*) FROM dbs").Scan(&numDBs)
	if err != nil || numDBs != 4 {
		t.Fatal(err)
	}
	err = db.QueryRow("SELECT COUNT(*) FROM rps").Scan(&numRPs)
	if err != nil || numRPs != 1 {
		t.Fatal(err)
	}
	err = db.QueryRow("SELECT COUNT(*) FROM msts").Scan(&numMSTs)
	if err != nil || numMSTs != 1 {
		t.Fatal(err)
	}

	var numFiles int
	err = db.QueryRow("SELECT COUNT(*) FROM files").Scan(&numFiles)
	if err != nil || numFiles != 10 {
		t.Fatal(err)
	}

	// select files from db.rp.msts
	selectFiles, err := db.Query("SELECT files.sequence, msts.name, rps.name, dbs.name FROM files, msts, rps, dbs WHERE dbs.id=rps.db_id AND rps.id=msts.rp_id AND msts.id=files.mst_id")
	if err != nil {
		t.Fatal(err)
	}
	defer selectFiles.Close()
	var id int
	var mstname, rpname, dbname string
	for selectFiles.Next() {
		err = selectFiles.Scan(&id, &mstname, &rpname, &dbname)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = selectFiles.Err()
	if err != nil {
		t.Fatal(err)
	}
}

func TestData_Files(t *testing.T) {
	type args struct {
		insertFiles  []FileInfo // insert
		updateFiles  []uint64   // mark delete
		deleteFiles  []uint64   // file id
		deleteFiles2 []uint64   // shard id
		deleteFiles3 []uint64   // for compaction
		insertFiles3 []FileInfo // for compaction
		mstID        uint64
		shardID      uint64
	}
	tests := []struct {
		name    string
		args    args
		want    []FileInfo
		wantErr bool
		errMsg  string
	}{
		{
			name: "test1",
			args: args{
				insertFiles: []FileInfo{
					{
						MstID:         1,
						ShardID:       1,
						Sequence:      1,
						Level:         0,
						Merge:         0,
						Extent:        0,
						CreatedAt:     1,
						DeletedAt:     0,
						MinTime:       1,
						MaxTime:       100,
						RowCount:      11,
						FileSizeBytes: 1011,
					},
					{
						MstID:         21,
						ShardID:       31,
						Sequence:      2,
						Level:         3,
						Merge:         4,
						Extent:        5,
						CreatedAt:     1,
						DeletedAt:     0,
						MinTime:       100,
						MaxTime:       200,
						RowCount:      21,
						FileSizeBytes: 2022,
					},
				},
				mstID:   21,
				shardID: 31,
			},
			want: []FileInfo{
				{
					MstID:         21,
					ShardID:       31,
					Sequence:      2,
					Level:         3,
					Merge:         4,
					Extent:        5,
					CreatedAt:     1,
					DeletedAt:     0,
					MinTime:       100,
					MaxTime:       200,
					RowCount:      21,
					FileSizeBytes: 2022,
				},
			},
			wantErr: false,
		},
		{
			name: "test2",
			args: args{
				insertFiles: []FileInfo{
					{
						MstID:         1,
						ShardID:       1,
						Sequence:      1,
						Level:         0,
						Merge:         0,
						Extent:        0,
						CreatedAt:     1,
						DeletedAt:     0,
						MinTime:       1,
						MaxTime:       100,
						RowCount:      11,
						FileSizeBytes: 1011,
					},
					{
						MstID:         211,
						ShardID:       311,
						Sequence:      22,
						Level:         23,
						Merge:         24,
						Extent:        25,
						CreatedAt:     21,
						DeletedAt:     0,
						MinTime:       100,
						MaxTime:       200,
						RowCount:      221,
						FileSizeBytes: 2022,
					},
					{
						MstID:         211,
						ShardID:       311,
						Sequence:      122,
						Level:         123,
						Merge:         124,
						Extent:        125,
						CreatedAt:     121,
						DeletedAt:     0,
						MinTime:       100,
						MaxTime:       200,
						RowCount:      221,
						FileSizeBytes: 2022,
					},
				},
				updateFiles: []uint64{3},
				mstID:       211,
				shardID:     311,
			},
			want: []FileInfo{
				{
					MstID:         211,
					ShardID:       311,
					Sequence:      22,
					Level:         23,
					Merge:         24,
					Extent:        25,
					CreatedAt:     21,
					DeletedAt:     0,
					MinTime:       100,
					MaxTime:       200,
					RowCount:      221,
					FileSizeBytes: 2022,
				},
			},
			wantErr: false,
		},
		{
			name: "testDeleteFilesByShard",
			args: args{
				insertFiles: []FileInfo{
					{
						MstID:         1,
						ShardID:       1,
						Sequence:      1,
						Level:         0,
						Merge:         0,
						Extent:        0,
						CreatedAt:     1,
						DeletedAt:     0,
						MinTime:       1,
						MaxTime:       100,
						RowCount:      11,
						FileSizeBytes: 1011,
					},
					{
						MstID:         211,
						ShardID:       311,
						Sequence:      22,
						Level:         23,
						Merge:         24,
						Extent:        25,
						CreatedAt:     21,
						DeletedAt:     0,
						MinTime:       100,
						MaxTime:       200,
						RowCount:      221,
						FileSizeBytes: 2022,
					},
					{
						MstID:         211,
						ShardID:       311,
						Sequence:      122,
						Level:         123,
						Merge:         124,
						Extent:        125,
						CreatedAt:     121,
						DeletedAt:     0,
						MinTime:       100,
						MaxTime:       200,
						RowCount:      221,
						FileSizeBytes: 2022,
					},
				},
				deleteFiles: []uint64{2},
				mstID:       211,
				shardID:     311,
			},
			want: []FileInfo{
				{
					MstID:         211,
					ShardID:       311,
					Sequence:      122,
					Level:         123,
					Merge:         124,
					Extent:        125,
					CreatedAt:     121,
					DeletedAt:     0,
					MinTime:       100,
					MaxTime:       200,
					RowCount:      221,
					FileSizeBytes: 2022,
				},
			},
			wantErr: false,
		},
		{
			name: "testDeleteFiles",
			args: args{
				insertFiles: []FileInfo{
					{
						MstID:         1,
						ShardID:       1,
						Sequence:      1,
						Level:         0,
						Merge:         0,
						Extent:        0,
						CreatedAt:     1,
						DeletedAt:     0,
						MinTime:       1,
						MaxTime:       100,
						RowCount:      11,
						FileSizeBytes: 1011,
					},
					{
						MstID:         211,
						ShardID:       311,
						Sequence:      22,
						Level:         23,
						Merge:         24,
						Extent:        25,
						CreatedAt:     21,
						DeletedAt:     0,
						MinTime:       100,
						MaxTime:       200,
						RowCount:      221,
						FileSizeBytes: 2022,
					},
					{
						MstID:         211,
						ShardID:       311,
						Sequence:      122,
						Level:         123,
						Merge:         124,
						Extent:        125,
						CreatedAt:     121,
						DeletedAt:     0,
						MinTime:       100,
						MaxTime:       200,
						RowCount:      221,
						FileSizeBytes: 2022,
					},
				},
				deleteFiles2: []uint64{311},
				mstID:        211,
				shardID:      311,
			},
			want:    []FileInfo{},
			wantErr: false,
		},
		{
			name: "testCompactFiles",
			args: args{
				insertFiles: []FileInfo{
					{
						MstID:         1,
						ShardID:       1,
						Sequence:      1,
						Level:         0,
						Merge:         0,
						Extent:        0,
						CreatedAt:     1,
						DeletedAt:     0,
						MinTime:       1,
						MaxTime:       100,
						RowCount:      11,
						FileSizeBytes: 1011,
					},
					{
						MstID:         211,
						ShardID:       311,
						Sequence:      22,
						Level:         23,
						Merge:         24,
						Extent:        25,
						CreatedAt:     21,
						DeletedAt:     0,
						MinTime:       100,
						MaxTime:       200,
						RowCount:      221,
						FileSizeBytes: 2022,
					},
					{
						MstID:         211,
						ShardID:       311,
						Sequence:      122,
						Level:         123,
						Merge:         124,
						Extent:        125,
						CreatedAt:     121,
						DeletedAt:     0,
						MinTime:       100,
						MaxTime:       200,
						RowCount:      221,
						FileSizeBytes: 2022,
					},
				},
				deleteFiles3: []uint64{2, 3},
				insertFiles3: []FileInfo{
					{
						MstID:         211,
						ShardID:       311,
						Sequence:      1122,
						Level:         123,
						Merge:         124,
						Extent:        125,
						CreatedAt:     121,
						DeletedAt:     0,
						MinTime:       100,
						MaxTime:       200,
						RowCount:      221,
						FileSizeBytes: 4044,
					},
				},
				mstID:   211,
				shardID: 311,
			},
			want: []FileInfo{
				{
					MstID:         211,
					ShardID:       311,
					Sequence:      1122,
					Level:         123,
					Merge:         124,
					Extent:        125,
					CreatedAt:     121,
					DeletedAt:     0,
					MinTime:       100,
					MaxTime:       200,
					RowCount:      221,
					FileSizeBytes: 4044,
				},
			},
			wantErr: false,
		},
		{
			name: "testInsertErr",
			args: args{
				insertFiles: []FileInfo{
					{
						MstID:         1,
						ShardID:       1,
						Sequence:      1,
						Level:         0,
						Merge:         0,
						Extent:        0,
						CreatedAt:     1,
						DeletedAt:     0,
						MinTime:       1,
						MaxTime:       100,
						RowCount:      11,
						FileSizeBytes: 1011,
					},
					{
						MstID:         1,
						ShardID:       1,
						Sequence:      1,
						Level:         0,
						Merge:         0,
						Extent:        0,
						CreatedAt:     1,
						DeletedAt:     0,
						MinTime:       1,
						MaxTime:       100,
						RowCount:      11,
						FileSizeBytes: 1011,
					},
				},
				updateFiles: []uint64{},
				mstID:       21,
				shardID:     31,
			},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed: files.mst_id, files.shard_id, files.sequence, files.level, files.extent, files.merge",
		},
	}
	for _, tt := range tests {
		if tt.name != "testCompactFiles" {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			os.Remove("./files.db")
			defer os.Remove("./files.db")

			db, err := sql.Open("sqlite3", "./files.db?cache=shared")
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.Exec(INITSQL)
			if err != nil {
				t.Fatal(err)
			}
			sqlite := &SQLiteWrapper{
				database: db,
			}
			err = sqlite.InsertFiles(tt.args.insertFiles, nil)
			if err != nil {
				if !(tt.wantErr && err.Error() == tt.errMsg) {
					t.Errorf(err.Error())
				}
			}
			err = sqlite.UpdateFiles(tt.args.updateFiles, nil)
			if err != nil {
				t.Fatal(err.Error())
			}
			err = sqlite.DeleteFiles(tt.args.deleteFiles)
			if err != nil {
				t.Fatal(err.Error())
			}
			err = sqlite.DeleteFilesByShard(tt.args.deleteFiles2)
			if err != nil {
				t.Fatal(err.Error())
			}
			err = sqlite.CompactFiles(tt.args.deleteFiles3, tt.args.insertFiles3)
			if err != nil {
				t.Fatal(err.Error())
			}
			got, err := sqlite.QueryFiles(tt.args.mstID, tt.args.shardID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) && (len(got) != 0 && len(tt.want) != 0) {
				t.Errorf("got: %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_SQLite_Transaction(t *testing.T) {
	os.Remove("./transaction.db")
	defer os.Remove("./transaction.db")
	db, err := sql.Open("sqlite3", "file:transaction.db//?cache=shared")

	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY,
			name TEXT
		);
	`)
	if err != nil {
		t.Fatal(err)
	}

	// first transaction
	tx1, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx1.Rollback()

	// insert data for first transaction
	_, err = tx1.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
	if err != nil {
		log.Fatal(err)
	}

	// second transaction
	tx2, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx2.Rollback()

	// query for second transaction
	rows, err := tx2.Query("SELECT name FROM users")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("Users in second transaction:")
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(name)
	}

	// commit the first transaction
	err = tx1.Commit()
	if err != nil {
		log.Fatal(err)
	}

	// query for second transaction
	rows, err = tx2.Query("SELECT name FROM users")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("Users after first transaction commit:")
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(name)
	}
}

func TestData_ShowShardsFromMst(t *testing.T) {
	data := &Data{PtNumPerNode: 1}
	DataLogger = logger.New(os.Stderr)
	data.CreateDataNode("127.0.0.1:8086", "127.0.0.1:8188", "", "")
	data.CreateDataNode("127.0.0.2:8086", "127.0.0.2:8188", "", "")
	data.CreateDataNode("127.0.0.3:8086", "127.0.0.3:8188", "", "")
	data.CreateDataNode("127.0.0.4:8086", "127.0.0.4:8188", "", "")
	data.CreateDataNode("127.0.0.5:8086", "127.0.0.5:8188", "", "")
	data.CreateDataNode("127.0.0.6:8086", "127.0.0.6:8188", "", "")
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	data.CreateDBPtView("foo")
	must(data.CreateDatabase("foo", nil, nil, false, 1, nil))
	rp := NewRetentionPolicyInfo("bar")
	rp.ShardGroupDuration = 24 * time.Hour
	must(data.CreateRetentionPolicy("foo", rp, false))
	shardGroups := make([]ShardGroupInfo, 0, 10)
	sg := []ShardGroupInfo{
		{
			ID:        1,
			DeletedAt: time.Now(),
			Shards: []ShardInfo{
				{
					ID: 1,
				},
				{
					ID: 2,
				},
				{
					ID: 3,
				},
				{
					ID: 4,
				},
				{
					ID: 5,
				},
				{
					ID: 6,
				},
			},
		},
		{
			ID: 2,
			Shards: []ShardInfo{
				{
					ID: 11,
				},
				{
					ID: 12,
				},
				{
					ID: 13,
				},
				{
					ID: 14,
				},
				{
					ID: 15,
				},
				{
					ID: 16,
				},
			},
		},
	}
	shardGroups = append(shardGroups, sg...)
	must(data.CreateMeasurement("foo", "bar", "cpu",
		&proto2.ShardKeyInfo{ShardKey: []string{"hostname"}, Type: proto.String(influxql.HASH)}, 3, nil, 0, nil, nil, nil))
	must(data.CreateMeasurement("foo", "bar", "mem",
		&proto2.ShardKeyInfo{ShardKey: []string{"hostname"}, Type: proto.String(influxql.HASH)}, 0, nil, 0, nil, nil, nil))
	data.Databases["foo"].RetentionPolicies["bar"].ShardGroups = shardGroups
	data.mapShardsToMst("db0", data.Databases["foo"].RetentionPolicies["bar"], &shardGroups[0])
	data.mapShardsToMst("db0", data.Databases["foo"].RetentionPolicies["bar"], &shardGroups[1])

	type args struct {
		db  string
		rp  string
		mst string
	}
	tests := []struct {
		name   string
		fields Data
		args   args
		want   models.Rows
	}{
		{
			name:   "test1",
			fields: *data,
			args: args{
				db:  "foo",
				rp:  "bar",
				mst: "cpu",
			},
			want: []*models.Row{
				{
					Name:    "foo.bar.cpu",
					Columns: []string{"id", "database", "retention_policy", "measurement", "shard_group"},
					Values: [][]interface{}{
						{
							uint64(11),
							"foo",
							"bar",
							"cpu",
							uint64(2),
						},
						{
							uint64(12),
							"foo",
							"bar",
							"cpu",
							uint64(2),
						},
						{
							uint64(16),
							"foo",
							"bar",
							"cpu",
							uint64(2),
						},
					},
				},
			},
		},
		{
			name:   "test1",
			fields: *data,
			args: args{
				db:  "foo",
				rp:  "bar",
				mst: "mem",
			},
			want: []*models.Row{
				{
					Name:    "foo.bar.mem",
					Columns: []string{"id", "database", "retention_policy", "measurement", "shard_group"},
					Values: [][]interface{}{
						{
							uint64(11),
							"foo",
							"bar",
							"mem",
							uint64(2),
						},
						{
							uint64(12),
							"foo",
							"bar",
							"mem",
							uint64(2),
						},
						{
							uint64(13),
							"foo",
							"bar",
							"mem",
							uint64(2),
						},
						{
							uint64(14),
							"foo",
							"bar",
							"mem",
							uint64(2),
						},
						{
							uint64(15),
							"foo",
							"bar",
							"mem",
							uint64(2),
						},
						{
							uint64(16),
							"foo",
							"bar",
							"mem",
							uint64(2),
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := &Data{
				Term:               tt.fields.Term,
				Index:              tt.fields.Index,
				ClusterID:          tt.fields.ClusterID,
				ClusterPtNum:       tt.fields.ClusterPtNum,
				PtNumPerNode:       tt.fields.PtNumPerNode,
				NumOfShards:        tt.fields.NumOfShards,
				MetaNodes:          tt.fields.MetaNodes,
				DataNodes:          tt.fields.DataNodes,
				SqlNodes:           tt.fields.SqlNodes,
				PtView:             tt.fields.PtView,
				ReplicaGroups:      tt.fields.ReplicaGroups,
				Databases:          tt.fields.Databases,
				Streams:            tt.fields.Streams,
				Users:              tt.fields.Users,
				MigrateEvents:      tt.fields.MigrateEvents,
				QueryIDInit:        tt.fields.QueryIDInit,
				AdminUserExists:    tt.fields.AdminUserExists,
				TakeOverEnabled:    tt.fields.TakeOverEnabled,
				BalancerEnabled:    tt.fields.BalancerEnabled,
				ExpandShardsEnable: tt.fields.ExpandShardsEnable,
				MaxNodeID:          tt.fields.MaxNodeID,
				MaxShardGroupID:    tt.fields.MaxShardGroupID,
				MaxShardID:         tt.fields.MaxShardID,
				MaxMstID:           tt.fields.MaxMstID,
				MaxIndexGroupID:    tt.fields.MaxIndexGroupID,
				MaxIndexID:         tt.fields.MaxIndexID,
				MaxEventOpId:       tt.fields.MaxEventOpId,
				MaxDownSampleID:    tt.fields.MaxDownSampleID,
				MaxStreamID:        tt.fields.MaxStreamID,
				MaxConnID:          tt.fields.MaxConnID,
				MaxSubscriptionID:  tt.fields.MaxSubscriptionID,
				MaxCQChangeID:      tt.fields.MaxCQChangeID,
				opsMapMu:           tt.fields.opsMapMu,
				OpsMap:             tt.fields.OpsMap,
				OpsMapMinIndex:     tt.fields.OpsMapMinIndex,
				OpsMapMaxIndex:     tt.fields.OpsMapMaxIndex,
				OpsToMarshalIndex:  tt.fields.OpsToMarshalIndex,
				SQLite:             tt.fields.SQLite,
			}
			got := data.ShowShardsFromMst(tt.args.db, tt.args.rp, tt.args.mst)
			if len(got) != len(tt.want) {
				t.Fatal("got wrong num of shards")
			}
			for i := range got {
				if !reflect.DeepEqual(got[i], tt.want[i]) {
					t.Errorf("Data.ShowShardsFromMst() = %v, want %v", got[i], tt.want[i])
				}
			}

		})
	}
}

func TestGetPtInfosByDbnameForRep(t *testing.T) {
	data := &Data{
		TakeOverEnabled: true,
		PtNumPerNode:    2,
		Databases: map[string]*DatabaseInfo{
			"db0": &DatabaseInfo{
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		ReplicaGroups: map[string][]ReplicaGroup{
			"db0": []ReplicaGroup{
				{
					ID:         0,
					MasterPtID: 1,
					Peers:      []Peer{{ID: 3}},
					Status:     SubHealth,
				},
				{
					ID:         1,
					MasterPtID: 2,
					Peers:      []Peer{{ID: 4}},
					Status:     UnFull,
				},
			},
		},
		PtView: map[string]DBPtInfos{
			"db0": {
				{
					PtId:   1,
					Owner:  PtOwner{NodeID: 1},
					Status: Offline,
					RGID:   0,
				},
				{
					PtId:   2,
					Owner:  PtOwner{NodeID: 1},
					Status: Offline,
					RGID:   1,
				},
				{
					PtId:   3,
					Owner:  PtOwner{NodeID: 2},
					Status: Offline,
					RGID:   0,
				},
				{
					PtId:   4,
					Owner:  PtOwner{NodeID: 2},
					Status: Offline,
					RGID:   1,
				},
			},
		},
	}
	config.SetHaPolicy(config.RepPolicy)
	res, err := data.GetPtInfosByDbname("db0", true, 3)
	assert2.Equal(t, nil, err)
	assert2.Equal(t, 2, len(res))
	assert2.Equal(t, uint32(1), res[0].Pti.PtId)
	assert2.Equal(t, uint32(3), res[1].Pti.PtId)
}

func TestUpdatePtStatusForRep1(t *testing.T) {
	data := &Data{
		TakeOverEnabled: true,
		ClusterPtNum:    1,
		Databases: map[string]*DatabaseInfo{
			"db0": &DatabaseInfo{
				Name:     "db0",
				ReplicaN: 1,
			},
		},
		ReplicaGroups: map[string][]ReplicaGroup{
			"db0": []ReplicaGroup{
				{
					ID:         0,
					MasterPtID: 0,
					Status:     SubHealth,
				},
			},
		},
		PtView: map[string]DBPtInfos{
			"db0": {
				{
					PtId:   0,
					Owner:  PtOwner{NodeID: 1},
					Status: Offline,
					RGID:   0,
				},
			},
		},
	}
	DataLogger = logger1.GetLogger()
	config.SetHaPolicy(config.RepPolicy)
	data.updatePtStatus("db0", 0, 1, Online)
	assert2.Equal(t, Health, data.ReplicaGroups["db0"][0].Status)
}

func TestUpdatePtStatusForRep2(t *testing.T) {
	data := &Data{
		TakeOverEnabled: true,
		ClusterPtNum:    3,
		Databases: map[string]*DatabaseInfo{
			"db0": &DatabaseInfo{
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		ReplicaGroups: map[string][]ReplicaGroup{
			"db0": []ReplicaGroup{
				{
					ID:         0,
					MasterPtID: 0,
					Peers:      []Peer{Peer{ID: 1}, Peer{ID: 2}},
					Status:     SubHealth,
				},
			},
		},
		PtView: map[string]DBPtInfos{
			"db0": {
				{
					PtId:   0,
					Owner:  PtOwner{NodeID: 1},
					Status: Offline,
					RGID:   0,
				},
				{
					PtId:   1,
					Owner:  PtOwner{NodeID: 2},
					Status: Offline,
					RGID:   0,
				},
				{
					PtId:   2,
					Owner:  PtOwner{NodeID: 3},
					Status: Offline,
					RGID:   0,
				},
			},
		},
	}
	DataLogger = logger1.GetLogger()
	config.SetHaPolicy(config.RepPolicy)
	data.updatePtStatus("db0", 0, 1, Online)
	assert2.Equal(t, SubHealth, data.ReplicaGroups["db0"][0].Status)
	data.updatePtStatus("db0", 1, 1, Online)
	assert2.Equal(t, Health, data.ReplicaGroups["db0"][0].Status)
	data.updatePtStatus("db0", 2, 1, Online)
	assert2.Equal(t, Health, data.ReplicaGroups["db0"][0].Status)
	data.ReplicaGroups["db0"][0].Status = UnFull
	data.updatePtStatus("db0", 2, 1, Online)
}

func TestUpdatePtViewStatusForRep1(t *testing.T) {
	data := &Data{
		TakeOverEnabled: true,
		ClusterPtNum:    3,
		Databases: map[string]*DatabaseInfo{
			"db0": &DatabaseInfo{
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		ReplicaGroups: map[string][]ReplicaGroup{
			"db0": []ReplicaGroup{
				{
					ID:         0,
					MasterPtID: 0,
					Peers:      []Peer{Peer{ID: 1}, Peer{ID: 2}},
					Status:     Health,
				},
			},
		},
		PtView: map[string]DBPtInfos{
			"db0": {
				{
					PtId:   0,
					Owner:  PtOwner{NodeID: 1},
					Status: Online,
					RGID:   0,
				},
				{
					PtId:   1,
					Owner:  PtOwner{NodeID: 2},
					Status: Online,
					RGID:   0,
				},
				{
					PtId:   2,
					Owner:  PtOwner{NodeID: 3},
					Status: Online,
					RGID:   0,
				},
			},
		},
	}
	DataLogger = logger1.GetLogger()
	config.SetHaPolicy(config.RepPolicy)
	data.updatePtViewStatus(2, Offline)
	assert2.Equal(t, Health, data.ReplicaGroups["db0"][0].Status)
	data.updatePtViewStatus(1, Offline)
	assert2.Equal(t, SubHealth, data.ReplicaGroups["db0"][0].Status)
	data.updatePtViewStatus(3, Offline)
}
