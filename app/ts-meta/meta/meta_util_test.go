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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/tcp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	logger2 "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
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
join = ["%s:9092"]
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
`, ip, dir, ip, ip, ip), c)
	c.JoinPeers = []string{ip + ":9092"}
	return c, err
}

func MakeRaftListen(c *config.Meta) (net.Listener, net.Listener, error) {
	ln, err := net.Listen("tcp", c.BindAddress)
	if err != nil {
		return ln, nil, err
	}
	mux := tcp.NewMux(tcp.MuxLogger(c.Logging.Build("meta_mux")))
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
	body, err := ioutil.ReadAll(r.Body)
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

func GenerateCreateDatabaseCmd(database string) *proto2.Command {
	val := &proto2.CreateDatabaseCommand{
		Name: proto.String(database),
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

func GenerateCreateShardGroupCmd(db, rp string, timestamp time.Time) *proto2.Command {
	val := &proto2.CreateShardGroupCommand{
		Database:  proto.String(db),
		Policy:    proto.String(rp),
		Timestamp: proto.Int64(timestamp.UnixNano()),
		ShardTier: proto.Uint64(meta2.StringToTier("HOT")),
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

func GenerateShardDurationCmd(index uint64, pts []uint32) *proto2.Command {
	val := &proto2.ShardDurationCommand{Index: proto.Uint64(index), Pts: pts}
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
