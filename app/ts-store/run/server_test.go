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

package run

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/toml"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	set "github.com/openGemini/openGemini/open_src/github.com/deckarep/golang-set"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/meta/proto"
)

var storageDataPath = "/tmp/data/"
var metaPath = "/tmp/meta"
var addr = "127.0.0.1:8502"
var addr1 = "127.0.0.1:8503"

func mockStorage() *storage.Storage {
	node := metaclient.NewNode(metaPath)
	storeConfig := config.NewStore()
	config.SetHaEnable(true)
	monitorConfig := config.Monitor{
		Pushers: "http",
	}
	config := &config.TSStore{
		Data:    storeConfig,
		Monitor: monitorConfig,
		Common:  config.NewCommon(),
	}

	storage, err := storage.OpenStorage(storageDataPath, node, nil, config)
	if err != nil {
		return nil
	}
	return storage
}

func mockHTTPServer(server *Server, authEnabled bool, addr string, t *testing.T) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("%v", err)
	}

	pusher := mockStatisticsPusher(server)
	conf := &config.Store{}
	opsConfig := &config.OpsMonitor{}
	conf.OpsMonitor = opsConfig
	conf.OpsMonitor.AuthEnabled = authEnabled
	h := NewHttpHandler(conf)
	server.StoreService.handler = h
	h.SetstatisticsPusher(pusher)

	server.StoreService.handler.metaClient = mockMetaClient()

	err = http.Serve(ln, h)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("listener failed: addr=%s, err=%s", ln.Addr(), err)
	}
}

func mockCollect(buf []byte) ([]byte, error) {
	return buf, nil
}

func mockStatisticsPusher(server *Server) *statisticsPusher.StatisticsPusher {
	return server.statisticsPusher
}

func mockServer() *Server {
	server := &Server{}
	server.Logger = logger.NewLogger(errno.ModuleStorageEngine)
	server.config = config.NewTSStore()
	server.config.Monitor.HttpEndPoint = "127.0.0.1:8502"
	server.config.Monitor.StoreDatabase = "_internal"
	server.config.Monitor.StoreEnabled = true
	server.config.Monitor.Pushers = "http"
	server.config.Monitor.StoreInterval = toml.Duration(1 * time.Second)
	server.config.Data.OpsMonitor.HttpAddress = addr1
	server.storage = mockStorage()
	server.StoreService = NewService(&server.config.Data)

	app.SwitchToSingle()
	server.initStatisticsPusher()
	server.statisticsPusher.RegisterOps(stat.CollectOpsPerfStatistics)
	server.statisticsPusher.Register(mockCollect)
	server.statisticsPusher.Start()

	return server
}

func TestDebugVars(t *testing.T) {
	server := mockServer()

	go mockHTTPServer(server, false, addr, t)

	time.Sleep(1 * time.Second)
	resp, err := http.Get(fmt.Sprintf("http://%s/debug/vars", addr))
	if err != nil {
		t.Fatalf("%v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if !strings.Contains(string(body), "performance") {
		t.Fatalf("invalid response data. exp get performance")
	}
}

func TestDebugVars1(t *testing.T) {
	adminUserExists = true
	server := mockServer()
	server.config.Data.OpsMonitor.AuthEnabled = true

	go mockHTTPServer(server, true, addr1, t)

	time.Sleep(1 * time.Second)
	client := http.Client{}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/debug/vars", addr1), http.NoBody)
	if err != nil {
		log.Fatal(err)
	}

	// TEST1: normal path
	req.SetBasicAuth("test", "test")

	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	defer res.Body.Close()

	_, err = io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// TEST2: no username
	req.SetBasicAuth("", "test")
	res, err = client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	_, err = io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// TEST3: auth fail
	authenticateOk = false
	req.SetBasicAuth("test", "test")

	res, err = client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	_, err = io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("%v", err)
	}
	server.statisticsPusher.Stop()
}

func TestNilService(t *testing.T) {
	server := &Server{}
	server.Logger = logger.NewLogger(errno.ModuleStorageEngine)
	server.config = config.NewTSStore()
	server.storage = mockStorage()
	server.StoreService = NewService(&server.config.Data)
	if server.StoreService != nil {
		t.Fatal("new service fail")
	}
}

type MockMetaClient struct {
}

func (client *MockMetaClient) GetStreamInfosStore() map[string]*meta2.StreamInfo {
	//TODO implement me
	panic("implement me")
}

func (client *MockMetaClient) GetMeasurementInfoStore(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (client *MockMetaClient) UpdateStreamMstSchema(database string, retentionPolicy string, mst string, stmt *influxql.SelectStatement) error {
	return nil
}

func (client *MockMetaClient) CreateStreamPolicy(info *meta2.StreamInfo) error {
	return nil
}

func (client *MockMetaClient) ShowStreams(database string, showAll bool) (models.Rows, error) {
	return nil, nil
}

func (client *MockMetaClient) DropStream(name string) error {
	return nil
}

func (client *MockMetaClient) OpenAtStore() {
	return
}

func (client *MockMetaClient) UpdateShardDownSampleInfo(Ident *meta2.ShardIdentifier) error {
	return nil
}

func mockMetaClient() *MockMetaClient {
	return &MockMetaClient{}
}

func (client *MockMetaClient) CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, indexR *meta2.IndexRelation) (*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) AlterShardKey(database, retentionPolicy, mst string, shardKey *meta2.ShardKeyInfo) error {
	return nil
}
func (client *MockMetaClient) CreateDatabase(name string) (*meta2.DatabaseInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) CreateDatabaseWithRetentionPolicy(name string, spec *meta2.RetentionPolicySpec, shardKey *meta2.ShardKeyInfo) (*meta2.DatabaseInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) CreateRetentionPolicy(database string, spec *meta2.RetentionPolicySpec, makeDefault bool) (*meta2.RetentionPolicyInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return nil
}
func (client *MockMetaClient) CreateUser(name, password string, admin, rwuser bool) (meta2.User, error) {
	return nil, nil
}
func (client *MockMetaClient) Databases() map[string]*meta2.DatabaseInfo {
	return nil
}
func (client *MockMetaClient) Database(name string) (*meta2.DatabaseInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) DataNode(id uint64) (*meta2.DataNode, error) {
	return nil, nil
}
func (client *MockMetaClient) DataNodes() ([]meta2.DataNode, error) {
	return nil, nil
}
func (client *MockMetaClient) DeleteDataNode(id uint64) error {
	return nil
}
func (client *MockMetaClient) DeleteMetaNode(id uint64) error {
	return nil
}
func (client *MockMetaClient) DropShard(id uint64) error {
	return nil
}
func (client *MockMetaClient) DropDatabase(name string) error {
	return nil
}
func (client *MockMetaClient) DropRetentionPolicy(database, name string) error {
	return nil
}
func (client *MockMetaClient) DropSubscription(database, rp, name string) error {
	return nil
}
func (client *MockMetaClient) DropUser(name string) error {
	return nil
}
func (client *MockMetaClient) MetaNodes() ([]meta2.NodeInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) RetentionPolicy(database, name string) (rpi *meta2.RetentionPolicyInfo, err error) {
	return nil, nil
}
func (client *MockMetaClient) SetAdminPrivilege(username string, admin bool) error {
	return nil
}
func (client *MockMetaClient) SetPrivilege(username, database string, p originql.Privilege) error {
	return nil
}
func (client *MockMetaClient) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta2.ShardInfo, err error) {
	return nil, nil
}
func (client *MockMetaClient) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta2.ShardGroupInfo, err error) {
	return nil, nil
}
func (client *MockMetaClient) TruncateShardGroups(t time.Time) error {
	return nil
}
func (client *MockMetaClient) UpdateRetentionPolicy(database, name string, rpu *meta2.RetentionPolicyUpdate, makeDefault bool) error {
	return nil
}
func (client *MockMetaClient) UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto.FieldSchema) error {
	return nil
}
func (client *MockMetaClient) UpdateUser(name, password string) error {
	return nil
}
func (client *MockMetaClient) UserPrivilege(username, database string) (*originql.Privilege, error) {
	return nil, nil
}
func (client *MockMetaClient) UserPrivileges(username string) (map[string]originql.Privilege, error) {
	return nil, nil
}
func (client *MockMetaClient) Users() []meta2.UserInfo {
	return nil
}
func (client *MockMetaClient) MarkDatabaseDelete(name string) error {
	return nil
}
func (client *MockMetaClient) MarkRetentionPolicyDelete(database, name string) error {
	return nil
}
func (client *MockMetaClient) MarkMeasurementDelete(database, mst string) error {
	return nil
}
func (client *MockMetaClient) DBPtView(database string) (meta2.DBPtInfos, error) {
	return nil, nil
}
func (client *MockMetaClient) ShardOwner(shardID uint64) (database, policy string, sgi *meta2.ShardGroupInfo) {
	return "", "", nil
}
func (client *MockMetaClient) Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) Schema(database string, retentionPolicy string, mst string) (fields map[string]int32, dimensions map[string]struct{}, err error) {
	return nil, nil, nil
}

func (client *MockMetaClient) GetMeasurements(m *influxql.Measurement) ([]*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) TagKeys(database string) map[string]set.Set {
	return nil
}
func (client *MockMetaClient) FieldKeys(database string, ms influxql.Measurements) (map[string]map[string]int32, error) {
	return nil, nil
}
func (client *MockMetaClient) QueryTagKeys(database string, ms influxql.Measurements, cond influxql.Expr) (map[string]map[string]struct{}, error) {
	return nil, nil
}
func (client *MockMetaClient) MatchMeasurements(database string, ms influxql.Measurements) (map[string]*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) Measurements(database string, ms influxql.Measurements) ([]string, error) {
	return nil, nil
}
func (client *MockMetaClient) ShowShards() models.Rows {
	return nil
}
func (client *MockMetaClient) ShowShardGroups() models.Rows {
	return nil
}
func (client *MockMetaClient) ShowSubscriptions() models.Rows {
	return nil
}
func (client *MockMetaClient) ShowRetentionPolicies(database string) (models.Rows, error) {
	return nil, nil
}
func (client *MockMetaClient) GetAliveShards(database string, sgi *meta2.ShardGroupInfo) []int {
	return nil
}

var adminUserExists bool = true

func (client *MockMetaClient) AdminUserExists() bool {
	return adminUserExists
}

var authenticateOk bool = true

func (client *MockMetaClient) Authenticate(username, password string) (u meta2.User, e error) {
	if authenticateOk {
		return nil, nil
	}
	return nil, meta2.ErrUserLocked
}

func (client *MockMetaClient) DropDownSamplePolicy(database, name string, dropAll bool) error {
	return nil
}

func (client *MockMetaClient) NewDownSamplePolicy(database, name string, info *meta2.DownSamplePolicyInfo) error {
	return nil
}

func (client *MockMetaClient) ShowDownSamplePolicies(database string) (models.Rows, error) {
	return nil, nil
}

func (client *MockMetaClient) UpdateUserInfo() {}

func (client *MockMetaClient) GetMstInfoWithInRp(dbName, rpName string, dataTypes []int64) (*meta2.RpMeasurementsFieldsInfo, error) {
	return nil, nil
}

func (mmc *MockMetaClient) GetStreamInfos() map[string]*meta2.StreamInfo {
	return nil
}

func (mmc *MockMetaClient) GetDstStreamInfos(db, rp string, dstSis *[]*meta2.StreamInfo) bool {
	return false
}
