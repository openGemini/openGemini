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

package meta

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
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
		&proto2.ShardKeyInfo{ShardKey: []string{"hostName", "location"}, Type: proto.String(influxql.RANGE)}, nil, 0, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// try to recreate measurement with same shardKey, should success
	err = data.CreateMeasurement(dbName, rpName, mstName,
		&proto2.ShardKeyInfo{ShardKey: []string{"hostName", "location"}, Type: proto.String(influxql.RANGE)}, nil, 0, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// try to recreate measurement with same shardKey, should fail
	err = data.CreateMeasurement(dbName, rpName, mstName,
		&proto2.ShardKeyInfo{ShardKey: []string{"hostName", "region"}, Type: proto.String(influxql.RANGE)}, nil, 0, nil, nil, nil)
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
		&proto2.ShardKeyInfo{ShardKey: []string{"hostName", "location"}, Type: proto.String(influxql.RANGE)}, nil, 0, nil, nil, nil)
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
		&proto2.ShardKeyInfo{ShardKey: []string{"hostname"}, Type: proto.String(influxql.RANGE)}, nil, 0, nil, nil, nil))
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
		Shards: []ShardInfo{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}}
	sgInfo2 := ShardGroupInfo{ID: 2, StartTime: time.Now(), EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []ShardInfo{{ID: 5}, {ID: 6}, {ID: 7}, {ID: 8}}}
	sgInfo3 := ShardGroupInfo{ID: 3, StartTime: time.Now(), EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []ShardInfo{{ID: 9}, {ID: 10}, {ID: 11}, {ID: 12}}}

	sgInfos := []ShardGroupInfo{sgInfo1, sgInfo2, sgInfo3}

	rpInfo := RetentionPolicyInfo{Name: policy, ShardGroups: sgInfos}
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
	if err := data.DeleteShardGroup(db, rp, 1); err != nil {
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
	if err := data.DeleteShardGroup(db, rp, 1); err != nil {
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
}

func initData() *Data {
	data := &Data{PtNumPerNode: 1}
	DataLogger = logger.New(os.Stderr)
	data.CreateDataNode("127.0.0.1:8086", "127.0.0.1:8188", "")
	data.CreateDataNode("127.0.0.2:8086", "127.0.0.2:8188", "")
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
		&proto2.ShardKeyInfo{ShardKey: []string{"hostname"}, Type: proto.String(influxql.RANGE)}, nil, 0, nil, nil, nil)
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

	err = data.CreateMeasurement(dbName, rpName, mstName, nil, nil, 0, nil, nil, nil)
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
	err, _ := data.CreateDataNode("127.0.0.1:8400", "127.0.0.1:8401", "")
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

	err = data.CreateMeasurement(dbName, rpName, mstName, nil, nil, 0, nil, nil, nil)
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
	err = data.CreateDBPtView(dbName)
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

	err = data.CreateMeasurement(dbName, rpName, mstName, nil, nil, 0, nil, nil, nil)
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
	err, _ = data.CreateDataNode("127.0.0.3:8400", "127.0.0.3:8401", "")
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
		err, _ := data.CreateDataNode(nodeIp+fmt.Sprint(selectPort), nodeIp+fmt.Sprint(writePort), "")
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
		_ = data.CreateMeasurement(databases[i%n], rpName, mstName, nil, nil, 0, nil, nil, nil)
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
		if err = data.CreateMeasurement(dbName, rpName, mstName, nil, nil, 0, nil, nil, nil); err != nil {
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
		if err = data.CreateMeasurement(dbName, rpName, mstName, nil, nil, 0, nil, nil, nil); err != nil {
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
		data.DeleteShardGroup(databases[i%n], rpName, sgIds[i%n])
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
		if err = data.CreateMeasurement(dbName, rpName, mstName, nil, nil, 0, nil, nil, nil); err != nil {
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

		err = data.CreateMeasurement(dbName, rpName, mstName, nil, nil, 0, nil, nil, nil)
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

		err = data.CreateMeasurement(dbName, rpName, mstName, nil, nil, 0, nil, nil, nil)
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
	pb := data.Marshal()
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
	if err := data.CreateMeasurement(dbName, logStream, logStream, nil, nil, 0, nil, nil, options); err != nil {
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
	data.DataNodes = append(data.DataNodes, DataNode{NodeInfo{ID: 5, Role: NodeWriter}, 0, 0})
	data.DataNodes = append(data.DataNodes, DataNode{NodeInfo{ID: 6, Role: NodeDefault}, 0, 0})
	data.DataNodes = append(data.DataNodes, DataNode{NodeInfo{ID: 7, Role: NodeReader}, 0, 0})
	data.initDataNodePtView(data.GetClusterPtNum())
	if data.ClusterPtNum != 2 {
		t.Fatalf("calculate ClusterPtNum failed")
	}
}
