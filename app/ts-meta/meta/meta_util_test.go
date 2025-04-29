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

package meta

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/tcp"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	logger2 "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"go.uber.org/zap"
)

type MetaService struct {
	httpLn     net.Listener
	ln         net.Listener
	metaServer *MetaServer
	store      *Store
	c          *config.Meta
}

func (m *MetaService) GetStore() *Store {
	return m.store
}

func (m *MetaService) GetConfig() *config.Meta {
	return m.c
}

func (m *MetaService) Close() {
	if m.metaServer != nil {
		m.metaServer.Stop()
	}
	if m.store != nil {
		m.store.Close()
	}
	if m.httpLn != nil && !reflect.ValueOf(m.httpLn).IsNil() {
		m.httpLn.Close()
	}
	if m.ln != nil && !reflect.ValueOf(m.ln).IsNil() {
		m.ln.Close()
	}
}

func NewMetaConfig(dir, ip string) (*config.Meta, error) {
	c := config.NewMeta()
	c.ClusterTracing = true
	_, err := toml.Decode(fmt.Sprintf(`
dir = "%s"
logging-enabled = true
bind-address="%s:9088"
http-bind-address = "%s:9091"
rpc-bind-address = "%s:9092"
retention-autocreate = true
election-timeout = "1s"
heartbeat-timeout = "1s"
leader-lease-timeout = "500ms"
commit-timeout = "50ms"
split-row-threshold = 1000
imbalance-factor = 0.3
sqlite-enabled = true
`, dir, ip, ip, ip), c)
	c.JoinPeers = []string{ip + ":9092"}
	return c, err
}

func MakeRaftListen(c *config.Meta) (net.Listener, net.Listener, error) {
	ln, err := net.Listen("tcp", c.BindAddress)
	if err != nil {
		return ln, nil, err
	}
	mux := tcp.NewMux(tcp.MuxLogger(c.Logging.NewLumberjackLogger("meta_mux")))
	log := zap.NewNop()
	go func() {
		if err := mux.Serve(ln); err != nil {
			log.Warn("mux server closed", zap.Error(err))
		}
	}()

	return ln, mux.Listen(MuxHeader), nil
}

func InitStore(dir string, ip string) (*MetaService, error) {
	ms := &MetaService{}
	var err error
	ms.c, err = NewMetaConfig(dir, ip)
	if err != nil {
		return ms, err
	}

	ms.store = NewStore(ms.c, ms.c.HTTPBindAddress, ms.c.RPCBindAddress, ms.c.BindAddress)
	log := zap.NewNop()
	ms.store.Logger = logger2.NewLogger(errno.ModuleMeta).SetZapLogger(log)
	ms.store.Node = metaclient.NewNode(ms.c.Dir)
	meta2.DataLogger = logger2.GetLogger()

	handler := &MockHandler{}
	handler.store = ms.store
	ms.httpLn, err = net.Listen("tcp", ms.c.HTTPBindAddress)
	if err != nil {
		return ms, err
	}
	go http.Serve(ms.httpLn, handler)

	ms.metaServer = NewMetaServer(ms.c.RPCBindAddress, ms.store, ms.c)
	if err := ms.metaServer.Start(); err != nil {
		return ms, err
	}

	var raftListener net.Listener
	ms.ln, raftListener, err = MakeRaftListen(ms.c)
	if err != nil {
		return ms, err
	}
	ms.store.SetClusterManager(NewClusterManager(ms.store))
	if err := ms.store.Open(raftListener); err != nil {
		return ms, err
	}
	return ms, nil
}

func InitStoreBindPeers(dir string, ip string) (*MetaService, error) {
	ms := &MetaService{}
	var err error
	ms.c, err = NewMetaConfig(dir, ip)
	ms.c.BindPeers = []string{ip + ":8088"}
	ms.c.JoinPeers = append(ms.c.JoinPeers, ip+":9092")
	if err != nil {
		return ms, err
	}

	ms.store = NewStore(ms.c, ms.c.HTTPBindAddress, ms.c.RPCBindAddress, ms.c.BindAddress)
	globalService = &Service{store: ms.store}
	log := zap.NewNop()
	ms.store.Logger = logger2.NewLogger(errno.ModuleMeta).SetZapLogger(log)
	ms.store.Node = metaclient.NewNode(ms.c.Dir)
	meta2.DataLogger = logger2.GetLogger()

	handler := &MockHandler{}
	handler.store = ms.store
	ms.httpLn, err = net.Listen("tcp", ms.c.HTTPBindAddress)
	if err != nil {
		return ms, err
	}
	go http.Serve(ms.httpLn, handler)

	ms.metaServer = NewMetaServer(ms.c.RPCBindAddress, ms.store, ms.c)
	if err := ms.metaServer.Start(); err != nil {
		return ms, err
	}

	var raftListener net.Listener
	ms.ln, raftListener, err = MakeRaftListen(ms.c)
	if err != nil {
		return ms, err
	}
	ms.store.SetClusterManager(NewClusterManager(ms.store))

	if err := ms.store.Open(raftListener); err != nil {
		return ms, err
	}
	return ms, nil
}

type MockMetaService struct {
	ln      net.Listener
	service *Service
}

func NewMockMetaService(dir, ip string) (*MockMetaService, error) {
	transport.NewMetaNodeManager().Clear()
	c, err := NewMetaConfig(dir, ip)
	if err != nil {
		return nil, err
	}
	config.SetHaPolicy(config.SSPolicy)
	mms := &MockMetaService{}
	mms.service = NewService(c, nil, nil)
	mms.service.Node = metaclient.NewNode(c.Dir)
	mms.ln, mms.service.RaftListener, err = MakeRaftListen(c)
	if err != nil {
		return nil, err
	}
	mms.service.msm = NewMigrateStateMachine()
	mms.service.balanceManager = NewBalanceManager(SerialBalanceAlgoName)
	balanceInterval = 200 * time.Millisecond

	if err := mms.service.Open(); err != nil {
		mms.Close()
		return nil, err
	}
	time.Sleep(time.Second / 2)
	return mms, nil
}

func BuildMockMetaService(dir, ip string) (*MockMetaService, error) {
	c, err := NewMetaConfig(dir, ip)
	if err != nil {
		return nil, err
	}
	config.SetHaPolicy(config.SSPolicy)
	mms := &MockMetaService{}
	mms.service = NewService(c, nil, nil)
	mms.service.Node = metaclient.NewNode(c.Dir)
	ar := NewAddrRewriter()
	ar.SetHostname(mms.service.config.RemoteHostname)
	mms.service.initStore(ar)
	return mms, nil
}

func (mms *MockMetaService) Close() {
	mms.service.Close()
	mms.ln.Close()
	config.SetHaPolicy(config.WAFPolicy)
}

func (mms *MockMetaService) GetService() *Service {
	return mms.service
}

func (mms *MockMetaService) GetListener() net.Listener {
	return mms.ln
}

func (mms *MockMetaService) GetStore() *Store {
	return mms.service.store
}

func (mms *MockMetaService) GetConfig() *config.Meta {
	return mms.service.config
}

type MockHandler struct {
	store interface {
		Join(n *meta2.NodeInfo) (*meta2.NodeInfo, error)
		GetData() *meta2.Data
	}
}

func (h *MockHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		switch r.URL.Path {
		case "/peers":
			h.WrapHandler("peers", h.servePeers).ServeHTTP(w, r)
		}
	case "POST":
		switch r.URL.Path {
		case "/join":
			h.WrapHandler("update", h.serveJoin).ServeHTTP(w, r)
		}
	default:
		http.Error(w, "", http.StatusBadRequest)
	}

}

func (h *MockHandler) WrapHandler(name string, hf http.HandlerFunc) http.Handler {
	var handler http.Handler
	handler = http.HandlerFunc(hf)

	return handler
}

func (h *MockHandler) servePeers(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode([]string{"127.0.0.1:8088"}); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

func (h *MockHandler) serveJoin(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	n := &meta2.NodeInfo{}
	if err := json.Unmarshal(body, n); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	node, err := h.store.Join(n)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(node); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}

	return

}
func (h *MockHandler) httpError(err error, w http.ResponseWriter, status int) {
	http.Error(w, "", status)
}

func GenerateCreateDataNodeCmd(httpAddr, tcpAddr string) *proto2.Command {
	val := &proto2.CreateDataNodeCommand{
		HTTPAddr: proto.String(httpAddr),
		TCPAddr:  proto.String(tcpAddr),
	}

	t1 := proto2.Command_CreateDataNodeCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_CreateDataNodeCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateUpdateUserCmd() *proto2.Command {
	val := &proto2.UpdateUserCommand{
		Name: proto.String("user"),
		Hash: proto.String("hash"),
	}

	t1 := proto2.Command_UpdateUserCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_UpdateUserCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateCreateSqlNodeCmd(httpAddr string) *proto2.Command {
	val := &proto2.CreateSqlNodeCommand{
		HTTPAddr: proto.String(httpAddr),
	}

	t1 := proto2.Command_CreateSqlNodeCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_CreateSqlNodeCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateCreateDatabaseCmd(database string) *proto2.Command {
	oneReplication := uint32(1)
	val := &proto2.CreateDatabaseCommand{
		Name:       proto.String(database),
		ReplicaNum: proto.Uint32(oneReplication),
	}

	t1 := proto2.Command_CreateDatabaseCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_CreateDatabaseCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateCreateDatabaseCmdWithDefaultRep(database string, replicaN uint32) *proto2.Command {
	val := &proto2.CreateDatabaseCommand{
		Name:       proto.String(database),
		ReplicaNum: proto.Uint32(replicaN),
	}

	t1 := proto2.Command_CreateDatabaseCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_CreateDatabaseCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateCreateMeasurementCmd(db string, rp string, mst string, shardKey []string, shardType string) *proto2.Command {
	val := &proto2.CreateMeasurementCommand{
		DBName: proto.String(db),
		RpName: proto.String(rp),
		Name:   proto.String(mst),
	}
	val.Ski = &proto2.ShardKeyInfo{
		ShardKey: shardKey,
		Type:     proto.String(shardType),
	}

	t1 := proto2.Command_CreateMeasurementCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_CreateMeasurementCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateCreateShardGroupCmd(db, rp string, timestamp time.Time, engineType config.EngineType) *proto2.Command {
	val := &proto2.CreateShardGroupCommand{
		Database:   proto.String(db),
		Policy:     proto.String(rp),
		Timestamp:  proto.Int64(timestamp.UnixNano()),
		ShardTier:  proto.Uint64(meta2.StringToTier("HOT")),
		EngineType: proto.Uint32(uint32(engineType)),
	}
	t1 := proto2.Command_CreateShardGroupCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_CreateShardGroupCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateGetShardRangeInfoCmd(db, rp string, shardId uint64) *proto2.Command {
	val := &proto2.TimeRangeCommand{
		Database: proto.String(db),
		Policy:   proto.String(rp),
		ShardID:  proto.Uint64(shardId),
	}

	t := proto2.Command_TimeRangeCommand
	cmd := &proto2.Command{Type: &t}
	if err := proto.SetExtension(cmd, proto2.E_TimeRangeCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateGetIndexDurationCommand(indexId int, rp string, shardId uint64) *proto2.Command {
	val := &proto2.IndexDurationCommand{
		Index:  proto.Uint64(uint64(indexId)),
		Pts:    []uint32{0},
		NodeId: proto.Uint64(1),
	}

	t := proto2.Command_IndexDurationCommand
	cmd := &proto2.Command{Type: &t}
	if err := proto.SetExtension(cmd, proto2.E_IndexDurationCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateShardDurationCmd(index uint64, pts []uint32, nodeId uint64) *proto2.Command {
	val := &proto2.ShardDurationCommand{Index: proto.Uint64(index), Pts: pts, NodeId: proto.Uint64(nodeId)}
	t := proto2.Command_ShardDurationCommand
	cmd := &proto2.Command{Type: &t}
	if err := proto.SetExtension(cmd, proto2.E_ShardDurationCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateMarkDatabaseDelete(db string) *proto2.Command {
	val := &proto2.MarkDatabaseDeleteCommand{
		Name: proto.String(db),
	}
	t1 := proto2.Command_MarkDatabaseDeleteCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_MarkDatabaseDeleteCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateMarkRpDelete(db, rp string) *proto2.Command {
	val := &proto2.MarkRetentionPolicyDeleteCommand{
		Database: proto.String(db),
		Name:     proto.String(rp),
	}
	t1 := proto2.Command_MarkRetentionPolicyDeleteCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_MarkRetentionPolicyDeleteCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateMarkMeasurementDeleteCmd(db, rp, mst string) *proto2.Command {
	val := &proto2.MarkMeasurementDeleteCommand{
		Database:    proto.String(db),
		Policy:      proto.String(rp),
		Measurement: proto.String(mst),
	}
	t1 := proto2.Command_MarkMeasurementDeleteCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_MarkMeasurementDeleteCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateRegisterQueryIDOffsetCmd(host string) *proto2.Command {
	val := &proto2.RegisterQueryIDOffsetCommand{Host: proto.String(host)}
	t1 := proto2.Command_RegisterQueryIDOffsetCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_RegisterQueryIDOffsetCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func ProcessExecuteRequest(s MetaStoreInterface, cmd *proto2.Command, metaconfig *config.Meta) error {
	body, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	msg := message.NewMetaMessage(message.ExecuteRequestMessage, &message.ExecuteRequest{Body: body})
	h := New(msg.Type())
	h.InitHandler(s, metaconfig, nil)
	if err = h.SetRequestMsg(msg.Data()); err != nil {
		return err
	}
	_, err = h.Process()
	return err
}

func GenerateCreateContinuousQueryCommand(db, name, query string) *proto2.Command {
	val := &proto2.CreateContinuousQueryCommand{
		Database: proto.String(db),
		Name:     proto.String(name),
		Query:    proto.String(query),
	}
	t1 := proto2.Command_CreateContinuousQueryCommand

	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_CreateContinuousQueryCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

type MockStore interface {
	GetShardSplitPoints(node *meta2.DataNode, database string, pt uint32,
		shardId uint64, idxes []int64) ([]string, error)
	DeleteDatabase(node *meta2.DataNode, database string, ptId uint32) error
	DeleteRetentionPolicy(node *meta2.DataNode, db string, rp string, ptId uint32) error
	DeleteMeasurement(node *meta2.DataNode, db string, rp, name string, shardIds []uint64) error
	MigratePt(nodeID uint64, data transport.Codec, cb transport.Callback) error
}

type MockNetStorage struct {
	GetShardSplitPointsFn func(node *meta2.DataNode, database string, pt uint32,
		shardId uint64, idxes []int64) ([]string, error)
	DeleteDatabaseFn        func(node *meta2.DataNode, database string, ptId uint32) error
	DeleteRetentionPolicyFn func(node *meta2.DataNode, db string, rp string, ptId uint32) error
	DeleteMeasurementFn     func(node *meta2.DataNode, db string, rp, name string, shardIds []uint64) error
	MigratePtFn             func(nodeID uint64, data transport.Codec, cb transport.Callback) error
	PingFn                  func(nodeId uint64, address string, timeout time.Duration) error
}

func (s *MockNetStorage) GetShardSplitPoints(node *meta2.DataNode, database string, pt uint32,
	shardId uint64, idxes []int64) ([]string, error) {
	return s.GetShardSplitPointsFn(node, database, pt, shardId, idxes)
}

func (s *MockNetStorage) DeleteDatabase(node *meta2.DataNode, database string, ptId uint32) error {
	return s.DeleteDatabaseFn(node, database, ptId)
}

func (s *MockNetStorage) DeleteRetentionPolicy(node *meta2.DataNode, db string, rp string, ptId uint32) error {
	return s.DeleteRetentionPolicyFn(node, db, rp, ptId)
}

func (s *MockNetStorage) DeleteMeasurement(node *meta2.DataNode, db, rp, name string, shardIds []uint64) error {
	return s.DeleteMeasurementFn(node, db, rp, name, shardIds)
}

func (s *MockNetStorage) MigratePt(nodeID uint64, data transport.Codec, cb transport.Callback) error {
	return s.MigratePtFn(nodeID, data, cb)
}

func (s *MockNetStorage) Ping(nodeId uint64, address string, timeout time.Duration) error {
	return s.PingFn(nodeId, address, timeout)
}

func (s *MockNetStorage) SendSegregateNodeCmds(nodeIDs []uint64, address []string) (int, error) {
	if len(nodeIDs) == 1 && nodeIDs[0] == 10 {
		return 0, fmt.Errorf("first node segregate error")
	}
	return 0, nil
}

func (s *MockNetStorage) TransferLeadership(database string, nodeId uint64, oldMasterPtId, newMasterPtId uint32) error {
	return nil
}

func NewMockNetStorage() *MockNetStorage {
	netStore := &MockNetStorage{}
	netStore.DeleteDatabaseFn = func(node *meta2.DataNode, database string, ptId uint32) error {
		return nil
	}
	netStore.DeleteRetentionPolicyFn = func(node *meta2.DataNode, db string, rp string, ptId uint32) error {
		return nil
	}
	netStore.DeleteMeasurementFn = func(node *meta2.DataNode, db string, rp, name string, shardIds []uint64) error {
		return nil
	}
	netStore.GetShardSplitPointsFn = func(node *meta2.DataNode, database string, pt uint32, shardId uint64, idxes []int64) ([]string, error) {
		return nil, nil
	}
	netStore.MigratePtFn = func(nodeID uint64, data transport.Codec, cb transport.Callback) error {
		cb.Handle(&netstorage.PtResponse{})
		return nil
	}
	netStore.PingFn = func(nodeId uint64, address string, timeout time.Duration) error {
		return nil
	}
	return netStore
}

func GenerateCreateDownSampleCmd(db, rp string, duration time.Duration, sampleIntervals, timeIntervals []time.Duration, calls []*meta2.DownSampleOperators) *proto2.Command {
	val := &proto2.CreateDownSamplePolicyCommand{
		Database: proto.String(db),
		Name:     proto.String(rp),
	}
	info := make([]*meta2.DownSamplePolicy, len(sampleIntervals))
	for i := range sampleIntervals {
		info[i] = &meta2.DownSamplePolicy{
			SampleInterval: sampleIntervals[i],
			TimeInterval:   timeIntervals[i],
		}
	}
	dp := &meta2.DownSamplePolicyInfo{
		Duration:           duration,
		DownSamplePolicies: info,
		Calls:              calls,
	}
	val.DownSamplePolicyInfo = dp.Marshal()
	t1 := proto2.Command_CreateDownSamplePolicyCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_CreateDownSamplePolicyCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateUpdateShardDownSampleInfoCmd(ident *meta2.ShardIdentifier) *proto2.Command {
	val := &proto2.UpdateShardDownSampleInfoCommand{
		Ident: ident.Marshal(),
	}
	t1 := proto2.Command_UpdateShardDownSampleInfoCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_UpdateShardDownSampleInfoCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateDropDownSampleCmd(db, rp string, dropAll bool) *proto2.Command {
	val := &proto2.DropDownSamplePolicyCommand{
		Database: proto.String(db),
		RpName:   proto.String(rp),
		DropAll:  proto.Bool(dropAll),
	}
	t1 := proto2.Command_DropDownSamplePolicyCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_DropDownSamplePolicyCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateCreateStreamCmd(db, rp, srcMst, desMst, name string, calls []*meta2.StreamCall, dims []string, id uint64, delay time.Duration) *proto2.Command {
	val := &proto2.CreateStreamCommand{}
	info := &meta2.StreamInfo{
		Name: name,
		ID:   id,
		SrcMst: &meta2.StreamMeasurementInfo{
			Name:            srcMst,
			Database:        db,
			RetentionPolicy: rp,
		},
		DesMst: &meta2.StreamMeasurementInfo{
			Name:            desMst,
			Database:        db,
			RetentionPolicy: rp,
		},
		Dims:  dims,
		Calls: calls,
		Delay: delay,
	}
	val.StreamInfo = info.Marshal()
	t1 := proto2.Command_CreateStreamCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_CreateStreamCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}

func GenerateDropStreamCmd(name string) *proto2.Command {
	val := &proto2.DropStreamCommand{
		Name: proto.String(name),
	}
	t1 := proto2.Command_DropStreamCommand
	cmd := &proto2.Command{Type: &t1}
	if err := proto.SetExtension(cmd, proto2.E_DropStreamCommand_Command, val); err != nil {
		panic(err)
	}
	return cmd
}
