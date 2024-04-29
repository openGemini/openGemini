/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/client.go

2022.01.23 changed.
Add new test case etc.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/
package metaclient

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/util/lifted/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func init() {
	RetryGetUserInfoTimeout = 1 * time.Second
	RetryExecTimeout = 3 * time.Second
	RetryReportTimeout = 1 * time.Second
	HttpReqTimeout = 1 * time.Second
}

type RPCServer struct {
	err      error
	metaData *meta2.Data
	ops      []string
	NodeId   uint64
	NoClear  bool
}

func (c *RPCServer) Abort() {

}

func (c *RPCServer) Handle(w spdy.Responser, data interface{}) error {
	metaMsg := data.(*message.MetaMessage)

	switch msg := metaMsg.Data().(type) {
	case *message.SnapshotRequest:
		return c.HandleSnapshot(w, msg)
	case *message.CreateNodeRequest:
		return c.HandleCreateNode(w, msg)
	case *message.SnapshotV2Request:
		return c.HandleSnapshotV2(w, msg)
	case *message.CreateSqlNodeRequest:
		return c.HandleCreateSqlNode(w, msg)
	}
	return nil
}

func (c *RPCServer) HandleSnapshot(w spdy.Responser, msg *message.SnapshotRequest) error {
	c.metaData = &meta2.Data{
		Index: msg.Index + 1,
	}

	buf, _ := c.metaData.MarshalBinary()
	rsp := &message.SnapshotResponse{
		Data: buf,
	}
	if server.err != nil {
		rsp.Err = server.err.Error()
	}
	return w.Response(message.NewMetaMessage(message.SnapshotResponseMessage, rsp), true)
}

func (c *RPCServer) HandleSnapshotV2(w spdy.Responser, msg *message.SnapshotV2Request) error {
	c.metaData = &meta2.Data{
		Index: msg.Index + 1,
	}
	if len(c.ops) > 0 {
		dataOps := meta2.NewDataOps(c.ops, 1, int(meta2.NoClear), msg.Index+uint64(len(c.ops)))
		data := dataOps.Marshal()
		rsp := &message.SnapshotV2Response{
			Data: data,
		}
		return w.Response(message.NewMetaMessage(message.SnapshotV2ResponseMessage, rsp), true)
	}
	if c.NoClear {
		dataOps := meta2.NewDataOps(nil, 1, int(meta2.NoClear), msg.Index+uint64(len(c.ops)))
		data := dataOps.Marshal()
		rsp := &message.SnapshotV2Response{
			Data: data,
		}
		return w.Response(message.NewMetaMessage(message.SnapshotV2ResponseMessage, rsp), true)
	}
	dataPb := c.metaData.Marshal()
	dataOps := meta2.NewDataOpsOfAllClear(int(meta2.AllClear), dataPb, *dataPb.Index)
	buf := dataOps.Marshal()
	rsp := &message.SnapshotV2Response{
		Data: buf,
	}
	if server.err != nil {
		rsp.Err = server.err.Error()
	}
	return w.Response(message.NewMetaMessage(message.SnapshotV2ResponseMessage, rsp), true)
}

func (c *RPCServer) HandleCreateSqlNode(w spdy.Responser, msg *message.CreateSqlNodeRequest) error {
	nodeStartInfo := meta2.NodeStartInfo{}
	nodeStartInfo.NodeId = c.NodeId
	buf, _ := nodeStartInfo.MarshalBinary()
	rsp := &message.CreateSqlNodeResponse{
		Data: buf,
	}
	return w.Response(message.NewMetaMessage(message.CreateSqlNodeResponseMessage, rsp), true)
}

func (c *RPCServer) HandleCreateNode(w spdy.Responser, msg *message.CreateNodeRequest) error {
	fmt.Printf("server HandleCreateNode: %+v \n", msg)
	nodeStartInfo := meta2.NodeStartInfo{}
	nodeStartInfo.NodeId = 1
	buf, _ := nodeStartInfo.MarshalBinary()
	rsp := &message.CreateNodeResponse{
		Data: buf,
		Err:  "",
	}
	return w.Response(message.NewMetaMessage(message.CreateNodeResponseMessage, rsp), true)
}

var server = &RPCServer{}

func startServer(address string) (*spdy.RRCServer, error) {
	rrcServer := spdy.NewRRCServer(spdy.DefaultConfiguration(), "tcp", address)
	rrcServer.RegisterEHF(transport.NewEventHandlerFactory(spdy.MetaRequest, server, &message.MetaMessage{}))
	if err := rrcServer.Start(); err != nil {
		return nil, err
	}
	return rrcServer, nil
}

func TestSnapshot(t *testing.T) {
	address := "127.0.0.1:8491"
	nodeId := 1
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	mc := Client{
		logger:         logger.NewLogger(errno.ModuleUnknown),
		SendRPCMessage: &RPCMessageSender{},
	}
	data, err := mc.getSnapshot(SQL, nodeId, 0)
	require.NoError(t, err)
	if data.Index != server.metaData.Index {
		t.Fatalf("error index; exp: %d; got: %d", server.metaData.Index, data.Index)
	}

	server.err = errors.New("mock error")
	defer func() {
		server.err = nil
	}()
	_, err = mc.getSnapshot(SQL, nodeId, 0)
	require.NotEmpty(t, err)
}

func TestClient_compareHashAndPwdVerOne(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	c.SetHashAlgo("ver01")
	hashed, err := c.genHashPwdVal("AsdF_789")
	if err != nil {
		t.Error("gen pbkdf2 password failed", zap.Error(err))
	}
	err = c.CompareHashAndPlainPwd(hashed, "AsdF_789")
	if err != nil {
		t.Error("compare hash and plaintext failed", zap.Error(err))
	}
}

func TestClient_compareHashAndPwdVerTwo(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	c.SetHashAlgo("ver02")
	hashed, err := c.genHashPwdVal("AsdF_789")
	if err != nil {
		t.Error("gen pbkdf2 password failed", zap.Error(err))
	}
	err = c.CompareHashAndPlainPwd(hashed, "AsdF_789")
	if err != nil {
		t.Error("compare hash and plaintext failed", zap.Error(err))
	}
}

func TestClient_compareHashAndPwdVerThree(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	c.SetHashAlgo("ver03")
	hashed, err := c.genHashPwdVal("AsdF_789")
	if err != nil {
		t.Error("gen pbkdf2 password failed", zap.Error(err))
	}
	err = c.CompareHashAndPlainPwd(hashed, "AsdF_789")
	if err != nil {
		t.Error("compare hash and plaintext failed", zap.Error(err))
	}
}

func TestClient_encryptWithSalt(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	c.SetHashAlgo("ver01")
	hashed := c.encryptWithSalt([]byte("salt"), "AsdF_789")
	if len(hashed) == 0 {
		t.Error("encryptWithSalt for version 01 failed")
	}

	c.optAlgoVer = algoVer02
	hashed = c.encryptWithSalt([]byte("salt"), "AsdF_789")
	if len(hashed) == 0 {
		t.Error("encryptWithSalt for version 02 failed")
	}

	c.optAlgoVer = algoVer03
	hashed = c.encryptWithSalt([]byte("salt"), "AsdF_789")
	if len(hashed) == 0 {
		t.Error("encryptWithSalt for version 03 failed")
	}
}

func TestClient_saltedEncryptByVer(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	c.SetHashAlgo("ver01")
	_, _, err := c.saltedEncryptByVer("AsdF_789")
	if err != nil {
		t.Error("encrypt by version1 failed", zap.Error(err))
	}

	c.optAlgoVer = algoVer02
	_, _, err = c.saltedEncryptByVer("AsdF_789")
	if err != nil {
		t.Error("encrypt by version2 failed", zap.Error(err))
	}

	c.optAlgoVer = algoVer03
	_, _, err = c.saltedEncryptByVer("AsdF_789")
	if err != nil {
		t.Error("encrypt by version3 failed", zap.Error(err))
	}
}

func TestClient_isValidPwd(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	c.SetHashAlgo("ver02")
	err := c.isValidPwd("1337", "jdoe1")
	require.EqualError(t, err, "the password needs to be between 8 and 256 characters long")

	err = c.isValidPwd("jdoe1jdoe1", "jdoe1jdoe1")
	require.EqualError(t, err, "User passwords must not same with username or username's reverse.")

	err = c.isValidPwd("jdoe1jdoe1", "jdoe1")
	require.EqualError(t, err, "The user password must contain more than 8 characters "+
		"and uppercase letters, lowercase letters, digits, and at least one of "+
		"the special characters.")
}

func TestClient_FieldKeys(t *testing.T) {
	keys := []FieldKey{
		{
			Field:     "f1",
			FieldType: 1,
		},
		{
			Field:     "f3",
			FieldType: 3,
		},

		{
			Field:     "f2",
			FieldType: 2,
		},
	}
	sort.Sort(FieldKeys(keys))
	require.Equal(t, keys[0].Field, "f1")
	require.Equal(t, keys[1].Field, "f2")
	require.Equal(t, keys[2].Field, "f3")
}

func TestClient_ReportCtx(t *testing.T) {
	loadCtx := LoadCtx{}
	// new ReportCtx
	ctx := loadCtx.GetReportCtx()
	ctx.DBPTStat = ctx.GetDBPTStat()
	ctx.DBPTStat.DB = proto.String("db0")
	ctx.DBPTStat.PtID = proto.Uint32(0)
	rpShardStatus := ctx.GetRpStat()
	rpShardStatus = append(rpShardStatus, &proto2.RpShardStatus{
		RpName: proto.String("autogen"),
		ShardStats: &proto2.ShardStatus{
			ShardID:     proto.Uint64(1),
			ShardSize:   proto.Uint64(1),
			SeriesCount: proto.Int32(1),
			MaxTime:     proto.Int64(1),
		},
	})
	ctx.DBPTStat.RpStats = rpShardStatus
	// put ReportCtx to sync pool
	loadCtx.PutReportCtx(ctx)
	ctxUsed := loadCtx.GetReportCtx()

	require.Equal(t, &ctx, &ctxUsed)
	// get ReportCtx from sync pool and check it
	dbptStatus := ctxUsed.GetDBPTStat()
	require.Equal(t, &ctx.DBPTStat, &dbptStatus)
	usedRPShardStatus := ctxUsed.GetRpStat()
	require.Equal(t, ctxUsed.DBPTStat.DB, proto.String(""))
	require.Equal(t, ctxUsed.DBPTStat.PtID, proto.Uint32(0))
	require.Equal(t, len(usedRPShardStatus), 0)
	require.Equal(t, cap(usedRPShardStatus), 1)
	usedRPShardStatus = usedRPShardStatus[:1]
	require.Equal(t, &rpShardStatus, &usedRPShardStatus)
}

func TestClient_BasicFunctions(t *testing.T) {
	client := &Client{
		nodeID: 1,
		cacheData: &meta2.Data{
			PtNumPerNode: 1,
			ClusterID:    1,
			DataNodes: []meta2.DataNode{
				meta2.DataNode{
					NodeInfo: meta2.NodeInfo{
						ID:      1,
						Host:    "127.0.0.1:8090",
						TCPHost: "127.0.0.1:8091",
					},
					ConnID:      0,
					AliveConnID: 0,
				},
			},
			MetaNodes: []meta2.NodeInfo{
				{
					ID:      1,
					Host:    "127.0.0.1:8088",
					TCPHost: "127.0.0.1:8089",
				},
			},
		},
	}

	require.Equal(t, uint64(1), client.NodeID())
	require.NoError(t, client.SetTier("hot"))
	require.NoError(t, client.SetTier("warm"))
	require.NoError(t, client.SetTier("cold"))
	require.EqualError(t, client.SetTier("xxxx"), "invalid tier xxxx")

	require.Equal(t, client.ClusterID(), uint64(1))

	// DataNode
	dataNode, _ := client.DataNode(1)
	require.Equal(t, dataNode.ID, uint64(1))
	require.Equal(t, dataNode.Host, "127.0.0.1:8090")
	_, err := client.DataNode(2)
	require.EqualError(t, err, meta2.ErrNodeNotFound.Error())

	// DataNodes
	dataNodes, _ := client.DataNodes()
	require.Equal(t, dataNodes[0].ID, uint64(1))
	require.Equal(t, dataNodes[0].Host, "127.0.0.1:8090")

	// DataNodeByHTTPHost
	dataNode, _ = client.DataNodeByHTTPHost("127.0.0.1:8090")
	require.Equal(t, dataNode.ID, uint64(1))
	require.Equal(t, dataNode.Host, "127.0.0.1:8090")
	_, err = client.DataNodeByHTTPHost("127.0.0.x")
	require.EqualError(t, err, meta2.ErrNodeNotFound.Error())

	// DataNodeByTCPHost
	dataNode, _ = client.DataNodeByTCPHost("127.0.0.1:8091")
	require.Equal(t, dataNode.ID, uint64(1))
	require.Equal(t, dataNode.Host, "127.0.0.1:8090")
	require.Equal(t, dataNode.TCPHost, "127.0.0.1:8091")
	_, err = client.DataNodeByTCPHost("127.0.0.x")
	require.EqualError(t, err, meta2.ErrNodeNotFound.Error())

	// MetaNodes
	metaNodes, _ := client.MetaNodes()
	require.Equal(t, metaNodes[0].ID, uint64(1))
	require.Equal(t, metaNodes[0].Host, "127.0.0.1:8088")
	require.Equal(t, metaNodes[0].TCPHost, "127.0.0.1:8089")

	// MetaNodeByAddr
	metaNode := client.MetaNodeByAddr("127.0.0.1:8088")
	require.Equal(t, metaNode.ID, uint64(1))
	require.Equal(t, metaNode.Host, "127.0.0.1:8088")
	require.Equal(t, metaNode.TCPHost, "127.0.0.1:8089")
}

func TestClient_UserCmd(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Users: []meta2.UserInfo{
				{
					Name: "admin",
					// Suibian@123
					Hash:  "#Ver:002#6012B9879FAB5709DA59F31C5FA676D7DCFB4D05DAF76EBEEFDF64B10428EB8FD532110FA805B7E13CCBB4FCE8FC14D9CD9417236EF41381171C91D3CCC4C622",
					Admin: true,
				},
				{
					Name: "user1",
					// Suibian@123
					Hash:  "#Ver:002#6012B9879FAB5709DA59F31C5FA676D7DCFB4D05DAF76EBEEFDF64B10428EB8FD532110FA805B7E13CCBB4FCE8FC14D9CD9417236EF41381171C91D3CCC4C622",
					Admin: false,
				},
			},
		},
		logger: logger.NewLogger(errno.ModuleUnknown),
	}

	_, err := c.CreateUser("admin", "Suibian@123", true, false)
	require.NoError(t, err)
	_, err = c.CreateUser("admin", "Suibian@1234", true, false)
	require.EqualError(t, err, meta2.ErrUserExists.Error())
	_, err = c.CreateUser("admin_new", "Suibian@1234", true, false)
	require.EqualError(t, err, meta2.ErrUserForbidden.Error())

	require.EqualError(t, c.DropUser("none"), meta2.ErrUserNotFound.Error())
	require.EqualError(t, c.DropUser("admin"), meta2.ErrUserDropSelf.Error())

	require.EqualError(t, c.UpdateUser("user1", "Suibian@123"), meta2.ErrPwdUsed.Error())
}

func TestClient_CreateShardGroup(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
					},
				}},
			},
		},
		metaServers:    []string{"127.0.0.1"},
		logger:         logger.NewLogger(errno.ModuleMetaClient),
		SendRPCMessage: &RPCMessageSender{},
	}

	_, err := c.CreateShardGroup("db0", "rp0", time.Now(), 0, config.COLUMNSTORE)
	require.EqualError(t, err, "execute command timeout")
}

func TestClient_GetShardInfoByTime(t *testing.T) {
	ts := time.Now()
	sgInfo1 := meta2.ShardGroupInfo{ID: 1, StartTime: ts, EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []meta2.ShardInfo{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}, EngineType: config.COLUMNSTORE}
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
						ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
					},
				}},
			},
			PtView: map[string]meta2.DBPtInfos{
				"db0": []meta2.PtInfo{{
					PtId: 0, Owner: meta2.PtOwner{NodeID: 0}, Status: meta2.Online},
					{PtId: 1, Owner: meta2.PtOwner{NodeID: 1}, Status: meta2.Online}},
			},
		},
		metaServers: []string{"127.0.0.1"},
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}

	_, err := c.GetShardInfoByTime("db0", "rp0", ts, 0, 0, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.GetShardInfoByTime("db1", "rp0", ts, 0, 0, config.COLUMNSTORE)
	assert.Equal(t, errno.Equal(err, errno.DatabaseNotFound), true)

	_, err = c.GetShardInfoByTime("db0", "rp0", time.Unix(0, 0), 0, 0, config.COLUMNSTORE)
	assert.Equal(t, errno.Equal(err, errno.ShardNotFound), true)

	_, err = c.GetShardInfoByTime("db0", "rp0", ts, 2, 2, config.COLUMNSTORE)
	assert.Equal(t, strings.Contains(err.Error(), fmt.Sprintf("nodeId cannot find pt")), true)

	c.cacheData.PtView["db0"] = nil
	_, err = c.GetShardInfoByTime("db0", "rp0", ts, 0, 0, config.COLUMNSTORE)
	assert.Equal(t, strings.Contains(err.Error(), fmt.Sprintf("db db0 in PtView not exist")), true)
}

func TestClient_CreateMeasurement(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
					},
				}},
			},
		},
		metaServers:    []string{"127.0.0.1"},
		logger:         logger.NewLogger(errno.ModuleMetaClient),
		SendRPCMessage: &RPCMessageSender{},
	}
	colStoreInfo := meta2.NewColStoreInfo(nil, nil, nil, 0, "")
	schemaInfo := meta2.NewSchemaInfo(map[string]int32{"a": influx.Field_Type_Tag}, map[string]int32{"b": influx.Field_Type_Float})

	options := &meta2.Options{Ttl: 1}
	_, err := c.CreateMeasurement("db0", "rp0", "measurement", nil, 0, nil, config.COLUMNSTORE, colStoreInfo, nil, options)
	require.EqualError(t, err, "execute command timeout")

	_, err = c.CreateMeasurement("db0", "rp0", "measurement", nil, 0, nil, config.COLUMNSTORE, colStoreInfo, schemaInfo, options)
	require.EqualError(t, err, "execute command timeout")
}

func TestClient_CreateDatabase(t *testing.T) {
	c := &Client{
		cacheData:      &meta2.Data{},
		metaServers:    []string{"127.0.0.1"},
		logger:         logger.NewLogger(errno.ModuleMetaClient),
		SendRPCMessage: &RPCMessageSender{},
	}
	options := &obs.ObsOptions{Enabled: true}
	_, err := c.CreateDatabase("db0", false, 1, options)
	require.EqualError(t, err, "execute command timeout")
}

func TestClient_CreateDatabaseWithRetentionPolicy(t *testing.T) {
	c := &Client{
		cacheData:      &meta2.Data{},
		metaServers:    []string{"127.0.0.1"},
		logger:         logger.NewLogger(errno.ModuleMetaClient),
		SendRPCMessage: &RPCMessageSender{},
	}
	spec := &meta2.RetentionPolicySpec{Name: "testRp"}
	ski := &meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}}
	_, err := c.CreateDatabaseWithRetentionPolicy("db0", spec, ski, false, 1)
	require.EqualError(t, err, "execute command timeout")
}

func TestClient_CreateDatabaseWithRetentionPolicy2(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}}}},
		},
	}
	spec := &meta2.RetentionPolicySpec{Name: "testRp"}
	ski := &meta2.ShardKeyInfo{ShardKey: []string{"tag2", "tag3"}}
	_, err := c.CreateDatabaseWithRetentionPolicy("test", spec, ski, false, 1)
	require.EqualError(t, err, "shard key conflict")
}

func TestClient_Stream(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
					},
				}},
				"db1": {
					Name:     "db1",
					ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp1": {
							Name:         "rp1",
							Duration:     72 * time.Hour,
							Measurements: map[string]*meta2.MeasurementInfo{"mst1": {Name: "mst1"}},
						},
					},
				},
			},
		},
		metaServers:    []string{"127.0.0.1:8092"},
		logger:         logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
		SendRPCMessage: &RPCMessageSender{},
	}
	info := &meta2.StreamInfo{
		Name: "test",
		ID:   0,
		SrcMst: &meta2.StreamMeasurementInfo{
			Name:            "mst0",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		DesMst: &meta2.StreamMeasurementInfo{
			Name:            "mst1",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		Dims: []string{"tag1"},
		Calls: []*meta2.StreamCall{
			{
				Call:  "first",
				Field: "age",
				Alias: "first_age",
			},
			{
				Call:  "first",
				Field: "score",
				Alias: "first_score",
			},
			{
				Call:  "first",
				Field: "name",
				Alias: "first_name",
			},
			{
				Call:  "first",
				Field: "alive",
				Alias: "first_alive",
			},
		},
		Delay: time.Second,
	}
	err := c.CreateStreamPolicy(info)
	require.EqualError(t, err, "execute command timeout")
	err = c.DropStream("test")

	c.cacheData.Streams = map[string]*meta2.StreamInfo{"test": info}
	_, _ = c.ShowStreams("db0", false)
	_, _ = c.ShowStreams("", true)

	stmt := &influxql.SelectStatement{
		Fields: influxql.Fields{
			&influxql.Field{
				Expr: &influxql.Call{
					Name: "first",
					Args: []influxql.Expr{&influxql.VarRef{Val: "age", Type: influxql.Integer}},
				},
			},
			&influxql.Field{
				Expr: &influxql.Call{
					Name: "first",
					Args: []influxql.Expr{&influxql.VarRef{Val: "name", Type: influxql.String}},
				},
			},
			&influxql.Field{
				Expr: &influxql.Call{
					Name: "first",
					Args: []influxql.Expr{&influxql.VarRef{Val: "score", Type: influxql.Float}},
				},
			},
			&influxql.Field{
				Expr: &influxql.Call{
					Name: "first",
					Args: []influxql.Expr{&influxql.VarRef{Val: "alive", Type: influxql.Boolean}},
				},
			},
		},
		Dimensions: influxql.Dimensions{
			{
				Expr: &influxql.VarRef{Val: "tag1", Type: influxql.Tag},
			},
		},
	}
	stmt1 := &influxql.SelectStatement{
		Fields: influxql.Fields{
			{
				Expr: &influxql.VarRef{Val: "tag1", Type: influxql.Tag},
			},
		},
	}
	stmt2 := &influxql.SelectStatement{
		Fields: influxql.Fields{
			{
				Expr: &influxql.Call{
					Name: "first",
					Args: []influxql.Expr{&influxql.VarRef{Val: "tag1", Type: influxql.TAG}},
				},
			},
		},
	}
	c.UpdateStreamMstSchema("db0", "rp0", "mst1", stmt)
	if c.UpdateStreamMstSchema("db0", "rp0", "mst1", stmt1).Error() != "unexpected call function" {
		t.Fatal()
	}
	if c.UpdateStreamMstSchema("db0", "rp0", "mst1", stmt2).Error() != "unexpected call type" {
		t.Fatal()
	}

	infos := c.GetStreamInfos()
	if len(infos) != 1 {
		t.Fatal("not find stream infos")
	}
}

func TestClient_GetMeasurements(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"m1": &meta2.MeasurementInfo{Name: "m1"}},
						MstVersions: map[string]meta2.MeasurementVer{"m1": {
							NameWithVersion: "m1",
							Version:         0,
						}},
					},
				}}},
		},
		metaServers:    []string{"127.0.0.1:8092"},
		logger:         logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
		SendRPCMessage: &RPCMessageSender{},
	}
	ms, err := c.GetMeasurements(&influxql.Measurement{Database: "test", RetentionPolicy: "rp0"})
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 {
		t.Fatal(fmt.Sprintf("unexpect measurement %v", ms))
	}
	if ms[0].Name != "m1" {
		t.Fatal(fmt.Sprintf("unexpect measurement %v", ms))
	}
	_, err = c.GetMeasurementInfoStore("test", "rp0", "test")
	if err == nil {
		t.Fatal("query from store should not return value in ut")
	}
	_, err = c.RetryMeasurement("test", "rp0", "test")
	if err == nil {
		t.Fatal("query from store should not return value in ut")
	}
	_, err = c.RetryMeasurement("test", "rp0", "m1")
	if err != nil {
		t.Fatal(err)
	}
}

func TestClient_Stream_GetStreamInfos(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers: []string{"127.0.0.1:8092"},
		logger:      logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}
	info := &meta2.StreamInfo{
		Name: "test",
		ID:   0,
		SrcMst: &meta2.StreamMeasurementInfo{
			Name:            "mst0",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		DesMst: &meta2.StreamMeasurementInfo{
			Name:            "mst1",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		Dims: []string{"tag1", "tag2"},
		Calls: []*meta2.StreamCall{
			{
				Call:  "first",
				Field: "age",
				Alias: "first_age",
			},
		},
		Delay: time.Second,
	}
	c.cacheData.Streams = map[string]*meta2.StreamInfo{"test": info}
	sis := c.GetStreamInfos()
	if _, ok := sis["test"]; !ok {
		t.Fatal("GetStreamInfos failed")
	}
}

func TestClient_Stream_GetDstStreamInfos(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers: []string{"127.0.0.1:8092"},
		logger:      logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}
	info := &meta2.StreamInfo{
		Name: "test",
		ID:   0,
		SrcMst: &meta2.StreamMeasurementInfo{
			Name:            "mst0",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		DesMst: &meta2.StreamMeasurementInfo{
			Name:            "mst1",
			Database:        "db0",
			RetentionPolicy: "rp0",
		},
		Dims: []string{"tag1"},
		Calls: []*meta2.StreamCall{
			{
				Call:  "first",
				Field: "age",
				Alias: "first_age",
			},
		},
		Delay: time.Second,
	}
	c.cacheData.Streams = map[string]*meta2.StreamInfo{"test": info}
	dstSis := &[]*meta2.StreamInfo{}
	db := "db0"
	rp := "rp0"
	exist := c.GetDstStreamInfos(db, rp, dstSis)
	if !exist {
		t.Fatal("GetStreamInfos failed")
	}

	c.cacheData.Streams = map[string]*meta2.StreamInfo{}
	exist = c.GetDstStreamInfos(db, rp, dstSis)
	if exist {
		t.Fatal("GetStreamInfos failed")
	}
}

func TestClient_CreateDownSamplePolicy(t *testing.T) {
	info := &meta2.DownSamplePolicyInfo{
		Calls: []*meta2.DownSampleOperators{
			{
				AggOps:   []string{"min", "max"},
				DataType: int64(influxql.Integer),
			},
		},
		DownSamplePolicies: []*meta2.DownSamplePolicy{
			{
				SampleInterval: time.Hour,
				TimeInterval:   25 * time.Second,
				WaterMark:      time.Hour,
			},
		},
		Duration: 240 * time.Hour,
	}
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers:    []string{"127.0.0.1:8092"},
		logger:         logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
		SendRPCMessage: &RPCMessageSender{},
	}
	err := c.NewDownSamplePolicy("test", "rp0", info)
	require.EqualError(t, err, "execute command timeout")
	err = c.DropDownSamplePolicy("test", "rp0", true)
	require.EqualError(t, err, "execute command timeout")

	c.cacheData.Databases["test"].RetentionPolicies["rp0"].DownSamplePolicyInfo = info
	row, _ := c.ShowDownSamplePolicies("test")
	names := []string{"rpName", "field_operator", "duration", "sampleInterval", "timeInterval", "waterMark"}
	values := []interface{}{"rp0", "integer{min,max}", "240h0m0s", "1h0m0s", "25s", "1h0m0s"}
	for i := range row[0].Columns {
		if row[0].Columns[i] != names[i] {
			t.Fatalf("wrong column names")
		}
		if row[0].Values[0][i] != values[i] {
			t.Fatalf("wrong column values")
		}
	}
}

func TestGetDownSampleInfo(t *testing.T) {
	info := &meta2.DownSamplePolicyInfo{
		Calls: []*meta2.DownSampleOperators{
			{
				AggOps:   []string{"min", "max"},
				DataType: int64(influxql.Integer),
			},
		},
		DownSamplePolicies: []*meta2.DownSamplePolicy{
			{
				SampleInterval: time.Hour,
				TimeInterval:   25 * time.Second,
				WaterMark:      time.Hour,
			},
		},
		Duration: 240 * time.Hour,
	}
	address := "127.0.0.1:8491"
	nodeId := 1
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	mc := Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers:    []string{"127.0.0.1:8491"},
		logger:         logger.NewLogger(errno.ModuleUnknown),
		SendRPCMessage: &RPCMessageSender{},
	}

	mc.cacheData.Databases["test"].RetentionPolicies["rp0"].DownSamplePolicyInfo = info
	var data []byte
	_ = data
	data, err = mc.getDownSampleInfo(nodeId)
	_, err = mc.GetMstInfoWithInRp("test", "rp0", []int64{int64(influxql.Integer)})
	_, err = mc.GetDownSamplePolicies()
	ident := &meta2.ShardIdentifier{
		ShardID: 1,
		OwnerDb: "db0",
		Policy:  "rp0",
	}
	err = mc.UpdateShardDownSampleInfo(ident)
	rmfi := meta2.RpMeasurementsFieldsInfo{
		MeasurementInfos: []*meta2.MeasurementFieldsInfo{
			{
				MstName: "mst",
				TypeFields: []*meta2.MeasurementTypeFields{
					{Type: 1, Fields: []string{"field1"}},
				},
			},
		},
	}
	b1, e1 := rmfi.MarshalBinary()
	if e1 != nil {
		t.Fatal(e1)
	}
	_, err1 := mc.unmarshalMstInfoWithInRp(b1)
	if err1 != nil {
		t.Fatal(err1)
	}
	dspi := &meta2.DownSamplePoliciesInfoWithDbRp{
		Infos: []*meta2.DownSamplePolicyInfoWithDbRp{
			{Info: info, DbName: "test", RpName: "rp0"},
		},
	}
	b2, e2 := dspi.MarshalBinary()
	if e2 != nil {
		t.Fatal(e2)
	}
	_, err2 := mc.unmarshalDownSamplePolicies(b2)
	if err2 != nil {
		t.Fatal(err2)
	}
}

func TestVerifyDataNodeStatus(t *testing.T) {
	config.SetHaPolicy(config.SSPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)

	mc := &Client{
		nodeID:         1,
		metaServers:    []string{"127.0.0.1:8092", "127.0.0.2:8092", "127.0.0.3:8092"},
		closing:        make(chan struct{}),
		logger:         logger.NewLogger(errno.ModuleMetaClient),
		SendRPCMessage: &mockRPCMessageSender{},
	}

	err := mc.retryVerifyDataNodeStatus()
	assert.NoError(t, err)
	close(mc.closing)
}

func TestCreateMeasurement(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers:    []string{"127.0.0.1:8092"},
		logger:         logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
		SendRPCMessage: &RPCMessageSender{},
	}
	var err error
	invalidMst := []string{"", "/111", ".", "..", "bbb\\aaa", string([]byte{'m', 's', 't', 0, '_', '0', '0'})}

	for _, mst := range invalidMst {
		_, err = c.CreateMeasurement("db0", "rp0", mst, nil, 0, nil, config.TSSTORE, nil, nil, nil)
		require.EqualError(t, err, errno.NewError(errno.InvalidMeasurement, mst).Error())
	}

	validMst := []string{"--", "__", ".-_", "mst....", "...", ".mst", "mst中文", "mst#11"}
	for _, mst := range validMst {
		require.True(t, meta2.ValidMeasurementName(mst))
	}
}

func TestDBPTCtx_String(t *testing.T) {
	ctx := &DBPTCtx{}
	ctx.DBPTStat = &proto2.DBPtStatus{
		DB:   proto.String("db0"),
		PtID: proto.Uint32(100),
		RpStats: []*proto2.RpShardStatus{{
			RpName: proto.String("default"),
			ShardStats: &proto2.ShardStatus{
				ShardID:     proto.Uint64(101),
				ShardSize:   proto.Uint64(102),
				SeriesCount: proto.Int32(103),
				MaxTime:     proto.Int64(104),
			},
		}, nil},
	}

	exp := `DB:"db0" PtID:100 RpStats:<RpName:"default" ShardStats:<ShardID:101 ShardSize:102 SeriesCount:103 MaxTime:104 > > RpStats:<nil> `
	require.Equal(t, exp, ctx.String())
	ctx.DBPTStat = nil
	require.Equal(t, "", ctx.String())
}

func TestTagArrayEnabled(t *testing.T) {
	address := "127.0.0.1:8496"
	nodeId := 1
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	mc := Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers: []string{"127.0.0.1:8491"},
		logger:      logger.NewLogger(errno.ModuleUnknown),
	}

	mc.cacheData.Databases["test"].EnableTagArray = true
	var data []byte
	_ = data
	enabled := mc.TagArrayEnabled("test")
	require.True(t, enabled)

	enabled = mc.TagArrayEnabled("test1")
	require.False(t, enabled)
}

func TestInitMetaClient(t *testing.T) {
	address := "127.0.0.1:8496"
	nodeId := 0
	transport.NewMetaNodeManager().Clear()
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	mc := Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		logger:         logger.NewLogger(errno.ModuleUnknown),
		SendRPCMessage: &RPCMessageSender{},
	}

	_, _, _, err = mc.InitMetaClient(nil, true, nil, nil, "", STORE)
	if err == nil {
		t.Fatalf("fail")
	}
	joinPeers := []string{"127.0.0.1:8491", "127.0.0.2:8491"}
	info := &StorageNodeInfo{"127.0.0.5:8081", "127.0.0.5:8082"}
	_, _, _, err = mc.InitMetaClient(joinPeers, true, info, nil, "", STORE)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestInitMetaClientForSql(t *testing.T) {
	address := "127.0.0.1:8496"
	nodeId := 0
	transport.NewMetaNodeManager().Clear()
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	mc := Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"test": &meta2.DatabaseInfo{
				Name:     "test",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		logger:         logger.NewLogger(errno.ModuleUnknown),
		SendRPCMessage: &RPCMessageSender{},
		UseSnapshotV2:  true,
	}

	joinPeers := []string{"127.0.0.1:8491", "127.0.0.2:8491"}
	info := &SqlNodeInfo{"127.0.0.5:8086", "127.0.0.5:8086"}
	server.NodeId = 234
	if nid, clock, connId, err := mc.InitMetaClient(joinPeers, true, nil, info, "", SQL); err != nil {
		t.Fatalf("%v %d %d %d", err, nid, clock, connId)
	}
}

func TestClient_RetryRegisterQueryIDOffset(t *testing.T) {
	type fields struct {
		metaServers []string
		cacheData   *meta2.Data
		logger      *logger.Logger
	}

	type args struct {
		host string
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    uint64
	}{
		{
			name: "TimeOut",
			fields: fields{
				metaServers: []string{"127.0.0.1:8092"},
				cacheData: &meta2.Data{
					QueryIDInit: map[meta2.SQLHost]uint64{},
				},
				logger: logger.NewLogger(errno.ModuleMetaClient),
			},
			args:    args{host: "127.0.0.1:8086"},
			wantErr: true,
			want:    0,
		},
		{
			name: "DuplicateRegistration",
			fields: fields{
				metaServers: []string{"127.0.0.1:8092"},
				cacheData: &meta2.Data{
					QueryIDInit: map[meta2.SQLHost]uint64{"127.0.0.1:8086": 100000},
				},
				logger: logger.NewLogger(errno.ModuleMetaClient),
			},
			args:    args{"127.0.0.1:8086"},
			wantErr: false,
			want:    100000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				metaServers:    tt.fields.metaServers,
				cacheData:      tt.fields.cacheData,
				logger:         tt.fields.logger,
				SendRPCMessage: &RPCMessageSender{},
			}

			offset, err := c.RetryRegisterQueryIDOffset(tt.args.host)
			if tt.wantErr {
				require.EqualError(t, err, "register query id offset timeout")
			}
			require.Equal(t, offset, tt.want)
		})
	}
}

func TestClient_ThermalShards(t *testing.T) {
	type fields struct {
		cacheData *meta2.Data
	}

	tests := []struct {
		name   string
		fields fields
		start  time.Duration
		end    time.Duration
		want   map[uint64]struct{}
	}{
		{
			name: "db not exist",
			fields: fields{
				cacheData: &meta2.Data{},
			},
			start: 0,
			end:   0,
			want:  nil,
		},
		{
			name: "start end is zero",
			fields: fields{
				cacheData: &meta2.Data{
					Databases: map[string]*meta2.DatabaseInfo{
						"db0": {
							RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
								"rp0": {
									ShardGroupDuration: time.Hour,
								},
							},
						},
					},
				},
			},
			start: 0,
			end:   0,
			want:  map[uint64]struct{}{},
		},
		{
			name: "start end not zero",
			fields: fields{
				cacheData: &meta2.Data{
					Databases: map[string]*meta2.DatabaseInfo{
						"db0": {
							RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
								"rp0": {
									ShardGroups: []meta2.ShardGroupInfo{
										{DeletedAt: time.Now()},                                            // deleted
										{EndTime: time.Now().Add(-24 * time.Hour)},                         // ignore
										{EndTime: time.Now(), Shards: []meta2.ShardInfo{{ID: 1}, {ID: 2}}}, // thermal shards
									},
								},
							},
						},
					},
				},
			},
			start: time.Hour,
			end:   time.Hour,
			want: map[uint64]struct{}{
				1: {},
				2: {},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				cacheData: tt.fields.cacheData,
				logger:    logger.NewLogger(errno.ModuleMetaClient),
			}

			result := c.ThermalShards("db0", tt.start, tt.end)
			assert.Equal(t, result, tt.want)
		})
	}
}

func TestClient_CreateSubscription(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	server1 := httptest.NewServer(mux)
	defer server1.Close()
	server2 := httptest.NewServer(mux)
	defer server2.Close()

	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name: "db0",
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers:    []string{"127.0.0.1:8092"},
		logger:         logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
		SendRPCMessage: &RPCMessageSender{},
	}
	destinations := []string{server1.URL, server2.URL}
	err := c.CreateSubscription("db0", "rp0", "subs1", "ALL", destinations)
	require.EqualError(t, err, "execute command timeout")
}

func TestClient_GetNodePtsMap(t *testing.T) {
	ts := time.Now()
	sgInfo1 := meta2.ShardGroupInfo{ID: 1, StartTime: ts, EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []meta2.ShardInfo{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}, EngineType: config.COLUMNSTORE}
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
						ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
					},
				}},
			},
			PtView: map[string]meta2.DBPtInfos{
				"db0": []meta2.PtInfo{
					{PtId: 0, Owner: meta2.PtOwner{NodeID: 0}, Status: meta2.Online},
					{PtId: 1, Owner: meta2.PtOwner{NodeID: 1}, Status: meta2.Offline}},
			},
		},
		metaServers: []string{"127.0.0.1"},
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}
	// test db not found
	nodePtMap, err := c.GetNodePtsMap("db1")
	assert.Equal(t, true, errno.Equal(err, errno.DatabaseNotFound))
	// test db has offline and online pt
	nodePtMap, err = c.GetNodePtsMap("db0")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(nodePtMap))
	assert.Equal(t, uint32(0), nodePtMap[0][0])
}

func TestClient_GetNodePtsMapForReplication(t *testing.T) {
	ts := time.Now()
	sgInfo1 := meta2.ShardGroupInfo{ID: 1, StartTime: ts, EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []meta2.ShardInfo{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}, EngineType: config.COLUMNSTORE}
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ReplicaN: 2,
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
						ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
					},
				}},
			},
			PtView: map[string]meta2.DBPtInfos{
				"db0": []meta2.PtInfo{
					{PtId: 0, Owner: meta2.PtOwner{NodeID: 0}, Status: meta2.Online, RGID: 0},
					{PtId: 1, Owner: meta2.PtOwner{NodeID: 1}, Status: meta2.Offline, RGID: 0},
					{PtId: 2, Owner: meta2.PtOwner{NodeID: 2}, Status: meta2.Online, RGID: 1},
					{PtId: 3, Owner: meta2.PtOwner{NodeID: 3}, Status: meta2.Offline, RGID: 1},
				},
			},
			ReplicaGroups: map[string][]meta2.ReplicaGroup{
				"db0": {
					{ID: 0, MasterPtID: 0, Peers: []meta2.Peer{{ID: 1, PtRole: meta2.Slave}}},
					{ID: 1, MasterPtID: 2, Peers: []meta2.Peer{{ID: 3, PtRole: meta2.Slave}}},
				},
			},
		},
		metaServers: []string{"127.0.0.1"},
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}
	// test db not found
	config.SetHaPolicy(config.RepPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	nodePtMap, err := c.GetNodePtsMap("db1")
	assert.Equal(t, true, errno.Equal(err, errno.DatabaseNotFound))
	// test db has offline and online pt
	nodePtMap, err = c.GetNodePtsMap("db0")
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(nodePtMap))
	assert.Equal(t, uint32(0), nodePtMap[0][0])
	assert.Equal(t, uint32(2), nodePtMap[2][0])

}

func TestClient_MultiRpWithSameMeasurement(t *testing.T) {
	ts := time.Now()
	sgInfo1 := meta2.ShardGroupInfo{ID: 1, StartTime: ts, EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []meta2.ShardInfo{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}, EngineType: config.TSSTORE}
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
						ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
					},
					"rp1": {
						Name:         "rp1",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
						ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
					},
					"rp2": {
						Name:         "rp2",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
						ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
					},
				}},
			},
			PtView: map[string]meta2.DBPtInfos{
				"db0": []meta2.PtInfo{{
					PtId: 0, Owner: meta2.PtOwner{NodeID: 0}, Status: meta2.Online},
					{PtId: 1, Owner: meta2.PtOwner{NodeID: 1}, Status: meta2.Offline}},
			},
		},
		metaServers: []string{"127.0.0.1"},
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}
	ms, err := c.Measurements("db0", nil)
	// test db not found
	if len(ms) != 1 || err != nil {
		t.Fatal("the number of measurement is not correct, actual", len(ms))
	}

}

func TestClient_GetReplicaN(t *testing.T) {
	c := &Client{
		cacheData: &meta2.Data{
			ReplicaGroups: nil,
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				ReplicaN: 1,
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
					},
				}},
			},
		},
		metaServers: []string{"127.0.0.1"},
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}

	replicaN, err := c.GetReplicaN("db0")
	assert.Equal(t, replicaN, 1)
	assert.Equal(t, err, nil)

	_, err = c.GetReplicaN("db1")
	assert.Equal(t, err, errno.NewError(errno.DatabaseNotFound, "db1"))

	repGroups := c.DBRepGroups("db0")
	assert.Equal(t, len(repGroups), 0)
}

func TestClient_GetAliveShards(t *testing.T) {
	ts := time.Now()
	sgInfo1 := meta2.ShardGroupInfo{
		ID:        1,
		StartTime: ts,
		EndTime:   time.Now().Add(time.Duration(3600)),
		DeletedAt: time.Time{},
		Shards: []meta2.ShardInfo{
			{ID: 1, Owners: []uint32{0}},
			{ID: 2, Owners: []uint32{1}},
			{ID: 3, Owners: []uint32{2}},
			{ID: 4, Owners: []uint32{3}},
		},
		EngineType: config.TSSTORE,
	}
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
						ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
					},
				}},
			},
			PtView: map[string]meta2.DBPtInfos{
				"db0": []meta2.PtInfo{
					{PtId: 0, Owner: meta2.PtOwner{NodeID: 0}, Status: meta2.Online},
					{PtId: 1, Owner: meta2.PtOwner{NodeID: 1}, Status: meta2.Online},
					{PtId: 2, Owner: meta2.PtOwner{NodeID: 2}, Status: meta2.Online},
					{PtId: 3, Owner: meta2.PtOwner{NodeID: 3}, Status: meta2.Online},
				},
			},
		},
		metaServers: []string{"127.0.0.1"},
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}

	shardIndexes := c.GetAliveShards("db0", &sgInfo1)
	assert.Equal(t, shardIndexes, []int{0, 1, 2, 3})

	config.SetHaPolicy(config.SSPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	shardIndexes = c.GetAliveShards("db0", &sgInfo1)
	assert.Equal(t, shardIndexes, []int{0, 1, 2, 3})
}

func TestClient_GetAliveShardsForReplication(t *testing.T) {
	ts := time.Now()
	sgInfo1 := meta2.ShardGroupInfo{
		ID:        1,
		StartTime: ts,
		EndTime:   time.Now().Add(time.Duration(3600)),
		DeletedAt: time.Time{},
		Shards: []meta2.ShardInfo{
			{ID: 1, Owners: []uint32{0}},
			{ID: 2, Owners: []uint32{1}},
			{ID: 3, Owners: []uint32{2}},
			{ID: 4, Owners: []uint32{3}},
		},
		EngineType: config.TSSTORE,
	}
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ReplicaN: 2,
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}, "rp0": {Name: "rp0"}},
						ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
					},
				}},
			},
			PtView: map[string]meta2.DBPtInfos{
				"db0": []meta2.PtInfo{
					{PtId: 0, Owner: meta2.PtOwner{NodeID: 0}, Status: meta2.Online, RGID: 0},
					{PtId: 1, Owner: meta2.PtOwner{NodeID: 1}, Status: meta2.Online, RGID: 0},
					{PtId: 2, Owner: meta2.PtOwner{NodeID: 2}, Status: meta2.Online, RGID: 1},
					{PtId: 3, Owner: meta2.PtOwner{NodeID: 3}, Status: meta2.Online, RGID: 1},
				},
			},
			ReplicaGroups: map[string][]meta2.ReplicaGroup{
				"db0": {
					{ID: 0, MasterPtID: 0, Peers: []meta2.Peer{{ID: 1, PtRole: meta2.Slave}}},
					{ID: 1, MasterPtID: 2, Peers: []meta2.Peer{{ID: 3, PtRole: meta2.Slave}}},
				},
			},
		},
		metaServers: []string{"127.0.0.1"},
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}

	config.SetHaPolicy(config.RepPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)
	shardIndexes := c.GetAliveShards("db0", &sgInfo1)
	assert.Equal(t, shardIndexes, []int{0, 2})
}

func TestClient_GetShardGroupByTimeRange(t *testing.T) {
	ts := time.Now()
	sgInfo1 := meta2.ShardGroupInfo{
		ID:        1,
		StartTime: ts,
		EndTime:   time.Now().Add(time.Duration(3600)),
		DeletedAt: time.Time{},
		Shards: []meta2.ShardInfo{
			{ID: 1, Owners: []uint32{0}},
			{ID: 2, Owners: []uint32{1}},
			{ID: 3, Owners: []uint32{2}},
			{ID: 4, Owners: []uint32{3}},
		},
		EngineType: config.TSSTORE,
	}
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name:     "db0",
					ReplicaN: 2,
					ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp0": {
							Name:         "rp0",
							Duration:     72 * time.Hour,
							Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}, "rp0": {Name: "rp0"}},
							ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
						},
					},
				},
				"db1": {
					Name:        "db1",
					ReplicaN:    2,
					MarkDeleted: true,
					ShardKey:    meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp0": {
							Name:         "rp0",
							Duration:     72 * time.Hour,
							Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}, "rp0": {Name: "rp0"}},
							ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
						},
					},
				},
				"db2": {
					Name:        "db2",
					ReplicaN:    2,
					MarkDeleted: true,
					ShardKey:    meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp0": {
							Name:         "rp0",
							Duration:     72 * time.Hour,
							Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}, "rp0": {Name: "rp0"}},
							ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
							MarkDeleted:  true,
						},
					},
				},
			},
			PtView: map[string]meta2.DBPtInfos{
				"db0": []meta2.PtInfo{
					{PtId: 0, Owner: meta2.PtOwner{NodeID: 0}, Status: meta2.Online, RGID: 0},
					{PtId: 1, Owner: meta2.PtOwner{NodeID: 1}, Status: meta2.Online, RGID: 0},
					{PtId: 2, Owner: meta2.PtOwner{NodeID: 2}, Status: meta2.Online, RGID: 1},
					{PtId: 3, Owner: meta2.PtOwner{NodeID: 3}, Status: meta2.Online, RGID: 1},
				},
			},
			ReplicaGroups: map[string][]meta2.ReplicaGroup{
				"db0": {
					{ID: 0, MasterPtID: 0, Peers: []meta2.Peer{{ID: 1, PtRole: meta2.Slave}}},
					{ID: 1, MasterPtID: 2, Peers: []meta2.Peer{{ID: 3, PtRole: meta2.Slave}}},
				},
			},
		},
		metaServers: []string{"127.0.0.1"},
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}
	_, err := c.GetShardGroupByTimeRange("db0", "rp0", ts, ts.Add(time.Duration(3600)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GetShardGroupByTimeRange("db1", "xx", ts, ts.Add(time.Duration(3600)))
	if err == nil {
		t.Fatal("unexpect")
	}
	_, err = c.GetShardGroupByTimeRange("db3", "xx", ts, ts.Add(time.Duration(3600)))
	if err == nil {
		t.Fatal("unexpect")
	}
	_, err = c.GetShardGroupByTimeRange("db0", "xx", ts, ts.Add(time.Duration(3600)))
	if err == nil {
		t.Fatal("unexpect")
	}
	_, err = c.GetShardGroupByTimeRange("db2", "rp0", ts, ts.Add(time.Duration(3600)))
	if err == nil {
		t.Fatal("unexpect")
	}
}

func TestClient_UpdateMeasurement(t *testing.T) {
	orgOptions := &meta2.Options{Ttl: 2}
	c := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{
				"db0": {
					Name: "db0",
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"rp0": {
							Duration: 1,
							Measurements: map[string]*meta2.MeasurementInfo{
								"cpu_0001": {Options: orgOptions},
							},
							MstVersions: map[string]meta2.MeasurementVer{
								"cpu": {NameWithVersion: "cpu_0001", Version: 1},
							},
						},
					},
				},
			},
		},
		metaServers:    []string{"127.0.0.1"},
		logger:         logger.NewLogger(errno.ModuleMetaClient),
		SendRPCMessage: &RPCMessageSender{},
	}
	options := &meta2.Options{Ttl: 1}
	err := c.UpdateMeasurement("db0", "rp0", "cpu", options)
	require.EqualError(t, err, "execute command timeout")
}

func TestGetAliveReadNode(t *testing.T) {
	client := NewClient("", false, 0)
	data := &meta2.Data{}
	client.SetCacheData(data)

	data.DataNodes = append(data.DataNodes, meta2.DataNode{NodeInfo: meta2.NodeInfo{ID: 5, Status: serf.StatusAlive, Role: meta2.NodeWriter}, ConnID: 0, AliveConnID: 0})
	_, err := client.AliveReadNodes()
	if err == nil {
		t.Fatalf("get alive readNodes failed")
	}

	data.DataNodes = append(data.DataNodes, meta2.DataNode{NodeInfo: meta2.NodeInfo{ID: 6, Status: serf.StatusAlive, Role: meta2.NodeDefault}, ConnID: 0, AliveConnID: 0})
	dataNodes, err := client.AliveReadNodes()
	if err != nil {
		t.Fatalf("get alive readNodes failed")
	}
	if len(dataNodes) != 1 || dataNodes[0].Role != meta2.NodeDefault {
		t.Fatalf("get alive readNodes failed")
	}

	data.DataNodes = append(data.DataNodes, meta2.DataNode{NodeInfo: meta2.NodeInfo{ID: 7, Status: serf.StatusAlive, Role: meta2.NodeReader}, ConnID: 0, AliveConnID: 0})
	data.DataNodes = append(data.DataNodes, meta2.DataNode{NodeInfo: meta2.NodeInfo{ID: 8, Status: serf.StatusFailed, Role: meta2.NodeReader}, ConnID: 0, AliveConnID: 0})
	dataNodes, err = client.AliveReadNodes()
	if err != nil {
		t.Fatalf("get alive readNodes failed")
	}
	if len(dataNodes) != 1 || dataNodes[0].Role != meta2.NodeReader {
		t.Fatalf("get alive readNodes failed")
	}
}

func TestClient_ShowCluster(t *testing.T) {
	m1 := meta2.NodeInfo{
		ID:     1,
		Host:   "127.0.0.2:8091",
		Status: serf.MemberStatus(meta2.StatusAlive),
	}
	m2 := meta2.NodeInfo{
		ID:     2,
		Host:   "127.0.0.2:8091",
		Status: serf.MemberStatus(meta2.StatusAlive),
	}
	m3 := meta2.NodeInfo{
		ID:     3,
		Host:   "127.0.0.2:8091",
		Status: serf.MemberStatus(meta2.StatusAlive),
	}
	d1 := meta2.DataNode{
		NodeInfo: meta2.NodeInfo{
			ID:     4,
			Host:   "127.0.0.2:8400",
			Status: serf.MemberStatus(meta2.StatusAlive),
		},
	}
	d2 := meta2.DataNode{
		NodeInfo: meta2.NodeInfo{
			ID:     5,
			Host:   "127.0.0.2:8400",
			Status: serf.MemberStatus(meta2.StatusAlive),
		},
	}
	d3 := meta2.DataNode{
		NodeInfo: meta2.NodeInfo{
			ID:     6,
			Host:   "127.0.0.2:8400",
			Status: serf.MemberStatus(meta2.StatusAlive),
		},
	}
	c := &Client{
		cacheData: &meta2.Data{
			MetaNodes: []meta2.NodeInfo{m1, m2, m3},
			DataNodes: []meta2.DataNode{d1, d2, d3},
		},
	}

	names := []string{"time", "status", "hostname", "nodeID", "nodeType"}
	value1 := []interface{}{m1.Status.String(), m1.Host, m1.ID, "meta"}
	value2 := []interface{}{m2.Status.String(), m2.Host, m2.ID, "meta"}
	value3 := []interface{}{m3.Status.String(), m3.Host, m3.ID, "meta"}
	value4 := []interface{}{d1.Status.String(), d1.Host, d1.ID, "data"}
	value5 := []interface{}{d2.Status.String(), d2.Host, d2.ID, "data"}
	value6 := []interface{}{d3.Status.String(), d3.Host, d3.ID, "data"}

	row1 := c.ShowCluster()
	values1 := [][]interface{}{value1, value2, value3, value4, value5, value6}
	for i := range row1[0].Columns {
		if row1[0].Columns[i] != names[i] {
			t.Fatalf("wrong column names")
		}
		for j := range row1[0].Values {
			if i != 0 && row1[0].Values[j][i] != values1[j][i-1] {
				t.Fatalf("wrong column values %s %s", row1[0].Values[j][i], values1[j][i-1])
			}
		}
	}

	row2, err := c.ShowClusterWithCondition("meta", 0)
	if err != nil {
		t.Fatalf("get cluster info failed")
	}
	values2 := [][]interface{}{value1, value2, value3}
	for i := range row2[0].Columns {
		if row2[0].Columns[i] != names[i] {
			t.Fatalf("wrong column names")
		}
		for j := range row2[0].Values {
			if i != 0 && row2[0].Values[j][i] != values2[j][i-1] {
				t.Fatalf("wrong column values %s %s", row2[0].Values[j][i], values2[j][i-1])
			}
		}
	}

	row3, err := c.ShowClusterWithCondition("data", 5)
	if err != nil {
		t.Fatalf("get cluster info failed")
	}
	values3 := [][]interface{}{value5}
	for i := range row3[0].Columns {
		if row3[0].Columns[i] != names[i] {
			t.Fatalf("wrong column names")
		}
		for j := range row3[0].Values {
			if i != 0 && row3[0].Values[j][i] != values3[j][i-1] {
				t.Fatalf("wrong column values %s %s", row3[0].Values[j][i], values3[j][i-1])
			}
		}
	}
}

func TestCheckAndUpdateReplication(t *testing.T) {
	type args struct {
		dbReplicaN uint32
		rpReplicaN *int
	}

	config.SetHaPolicy(config.RepPolicy)
	defer config.SetHaPolicy(config.WAFPolicy)

	oneReplication := 1
	twoReplication := 2
	threeReplication := 3
	fourReplication := 4

	tests := []struct {
		name    string
		args    args
		want    uint32
		want1   *int
		wantErr bool
	}{
		{
			name: "Test with both dbReplicaN and rpReplicaN as 0",
			args: args{
				dbReplicaN: 0,
				rpReplicaN: nil,
			},
			want:    1,
			want1:   &oneReplication,
			wantErr: false,
		},
		{
			name: "Test with dbReplicaN as not 0 and rpReplicaN as nil",
			args: args{
				dbReplicaN: 3,
				rpReplicaN: nil,
			},
			want:    3,
			want1:   &threeReplication,
			wantErr: false,
		},
		{
			name: "Test with dbReplicaN and rpReplicaN not equal",
			args: args{
				dbReplicaN: 2,
				rpReplicaN: nil,
			},
			want:    2,
			want1:   &twoReplication,
			wantErr: true,
		},
		{
			name: "Test with dbReplicaN is even",
			args: args{
				dbReplicaN: 4,
				rpReplicaN: nil,
			},
			want:    4,
			want1:   &fourReplication,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := checkAndUpdateReplication(tt.args.dbReplicaN, tt.args.rpReplicaN)
			require.Equalf(t, tt.wantErr, err != nil, "checkAndUpdateReplication() error = %v, wantErr %v", err, tt.wantErr)
			assert.Equal(t, tt.want, got, "checkAndUpdateReplication() got = %v, want %v", got, tt.want)
			assert.Equal(t, tt.want1, got1, "checkAndUpdateReplication() got1 = %v, want %v", got1, tt.want1)
		})
	}
}

func MocTestCacheData(dbName, logStreamName string) (*meta2.Data, error) {
	data := &meta2.Data{PtNumPerNode: 1}
	data.CreateDataNode("127.0.0.1:8086", "127.0.0.1:8188", "")
	data.CreateDataNode("127.0.0.2:8086", "127.0.0.2:8188", "")

	if err := data.CreateDatabase(dbName, nil, nil, false, 1, &proto2.ObsOptions{}); err != nil {
		return nil, err
	}
	if err := data.CreateDBPtView(dbName); err != nil {
		return nil, err
	}

	sgTtl := 1 * time.Hour
	rpi := &meta2.RetentionPolicyInfo{Name: logStreamName, ReplicaN: 1, Duration: 24 * sgTtl, ShardGroupDuration: sgTtl,
		HotDuration: 0, WarmDuration: 0, IndexGroupDuration: 24 * sgTtl}
	if err := data.CreateRetentionPolicy(dbName, rpi, false); err != nil {
		return nil, err
	}

	var ttl int64 = 1
	options := &proto2.Options{Ttl: &ttl}
	if err := data.CreateMeasurement(dbName, logStreamName, logStreamName, &proto2.ShardKeyInfo{
		ShardKey: []string{"hostname"},
		Type:     proto.String(influxql.HASH),
	}, 0, nil, config.COLUMNSTORE, nil, nil, options); err != nil {
		return nil, err
	}

	t := time.Now().UTC()
	if err := data.CreateShardGroup(dbName, logStreamName, t.Add(-time.Hour*24*3), 0, config.COLUMNSTORE, 0); err != nil {
		return nil, err
	}

	if err := data.CreateShardGroup(dbName, logStreamName, t.Add(-time.Hour*24*2), 0, config.COLUMNSTORE, 0); err != nil {
		return nil, err
	}
	rp, _ := data.RetentionPolicy(dbName, logStreamName)
	rp.ShardGroupDuration = time.Hour * 24
	rp.ShardGroups[1].DeletedAt = t.Add(-time.Hour * 24 * 1).Truncate(rp.ShardGroupDuration).UTC()

	if err := data.CreateShardGroup(dbName, logStreamName, t.Add(-time.Hour*24*1), 0, config.COLUMNSTORE, 0); err != nil {
		return nil, err
	}

	return data, nil
}

func TestRevertRetentionPolicyDelete(t *testing.T) {
	options := &meta2.Options{}
	options.InitDefault()
	cacheData, err := MocTestCacheData("db0", "rp0")
	if err != nil {
		t.Fatalf("mock cache data failed, err:%+v", err)
	}
	c := &Client{
		cacheData:      cacheData,
		metaServers:    []string{"127.0.0.1:8092"},
		logger:         logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
		SendRPCMessage: &RPCMessageSender{},
	}
	err = c.RevertRetentionPolicyDelete("db0", "rp0")
	if err != nil && !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("RevertRetentionPolicyDelete failed, err:%+v", err)
	}
	err = c.RevertRetentionPolicyDelete("db1", "rp1")
	if err == nil {
		t.Fatalf("db1 does not exist, but there is no error reported")
	}
	err = c.RevertRetentionPolicyDelete("db0", "rp1")
	if err == nil {
		t.Fatalf("rp1 does not exist, but there is no error reported")
	}
	err = c.DelayDeleteShardGroup("db0", "rp1", 1, time.Now().UTC(), 0)
	if err == nil {
		t.Fatalf("rp1 does not exist, but there is no error reported")
	}
}

func MocTestCacheDataForExpired() (*meta2.Data, error) {
	dbName := "testDb"
	logStreamName := "testLogstrem"
	data := &meta2.Data{PtNumPerNode: 1}
	data.CreateDataNode("127.0.0.1:8086", "127.0.0.1:8188", "")
	data.CreateDataNode("127.0.0.2:8086", "127.0.0.2:8188", "")

	if err := data.CreateDatabase(dbName, nil, nil, false, 1, &proto2.ObsOptions{}); err != nil {
		return nil, err
	}
	if err := data.CreateDBPtView(dbName); err != nil {
		return nil, err
	}
	sgTtl := 1 * time.Hour
	rpi := &meta2.RetentionPolicyInfo{Name: logStreamName, ReplicaN: 1, Duration: 24 * sgTtl, ShardGroupDuration: sgTtl,
		HotDuration: 0, WarmDuration: 0, IndexGroupDuration: 24 * sgTtl}
	if err := data.CreateRetentionPolicy(dbName, rpi, false); err != nil {
		return nil, err
	}

	var ttl int64 = 1
	options := &proto2.Options{Ttl: &ttl}
	if err := data.CreateMeasurement(dbName, logStreamName, logStreamName, &proto2.ShardKeyInfo{
		ShardKey: []string{"hostname"},
		Type:     proto.String(influxql.HASH),
	}, 0, nil, config.COLUMNSTORE, nil, nil, options); err != nil {
		return nil, err
	}

	t := time.Now().UTC()
	if err := data.CreateShardGroup(dbName, logStreamName, t.Add(-time.Hour*24*3), 0, config.COLUMNSTORE, 0); err != nil {
		return nil, err
	}

	if err := data.CreateShardGroup(dbName, logStreamName, t.Add(-time.Hour*24*2), 0, config.COLUMNSTORE, 0); err != nil {
		return nil, err
	}
	rp, _ := data.RetentionPolicy(dbName, logStreamName)
	rp.ShardGroupDuration = time.Hour * 24
	rp.ShardGroups[1].DeletedAt = t.Add(-time.Hour * 24 * 1).Truncate(rp.ShardGroupDuration).UTC()

	if err := data.CreateShardGroup(dbName, logStreamName, t.Add(-time.Hour*24*1), 0, config.COLUMNSTORE, 0); err != nil {
		return nil, err
	}

	dbName1 := "testDb1"
	if err := data.CreateDatabase(dbName1, nil, nil, false, 1, nil); err != nil {
		return nil, err
	}

	dbName2 := "testDb2"
	if err := data.CreateDatabase(dbName2, nil, nil, false, 1, &proto2.ObsOptions{}); err != nil {
		return nil, err
	}

	dbName3 := "testDb3"
	logStreamName3 := "testLogstrem3"
	if err := data.CreateDatabase(dbName3, nil, nil, false, 1, &proto2.ObsOptions{}); err != nil {
		return nil, err
	}
	if err := data.CreateDBPtView(dbName3); err != nil {
		return nil, err
	}
	rpi3 := &meta2.RetentionPolicyInfo{Name: logStreamName3, ReplicaN: 1, Duration: 0, ShardGroupDuration: 0,
		HotDuration: 0, WarmDuration: 0, IndexGroupDuration: 0}
	if err := data.CreateRetentionPolicy(dbName, rpi3, false); err != nil {
		return nil, err
	}
	return data, nil
}

func TestCacheDataForExpired(t *testing.T) {
	// mock cache data
	cacheData, err := MocTestCacheDataForExpired()
	if err != nil {
		t.Fatalf("mock cachedata failed, err:%+v", err)
	}
	c := &Client{
		cacheData:      cacheData,
		metaServers:    []string{"127.0.0.1:8092"},
		logger:         logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
		SendRPCMessage: &RPCMessageSender{},
	}

	markDelSgInfos, expiredShards := c.GetExpiredShards()
	fmt.Println("dd", len(expiredShards), len(markDelSgInfos))
	if len(expiredShards) == 0 || len(markDelSgInfos) == 0 {
		t.Fatalf("get expired shards failed, err:%d, %d", len(expiredShards), len(markDelSgInfos))
	}

	expiredIndexes := c.GetExpiredIndexes()
	if len(expiredIndexes) == 0 {
		t.Fatalf("get expired indexes failed, err:%d", len(expiredIndexes))
	}
}

func TestClient_ShowShards(t *testing.T) {
	type args struct {
		db  string
		rp  string
		mst string
	}
	ts := time.Now()
	sgInfo1 := meta2.ShardGroupInfo{ID: 1, StartTime: ts, EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []meta2.ShardInfo{{ID: 1}, {ID: 21}, {ID: 33}, {ID: 46}}, EngineType: config.COLUMNSTORE}
	c1 := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
						ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
					},
				}},
			},
		},
	}
	c2 := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{
							"mst0_0001": {
								Name: "mst0_0001",
								ShardIdexes: map[uint64][]int{
									1: {1, 2, 3},
								},
							},
						},
						MstVersions: map[string]meta2.MeasurementVer{
							"mst0": {
								NameWithVersion: "mst0_0001",
								Version:         1,
							},
						},
						ShardGroups: []meta2.ShardGroupInfo{sgInfo1},
					},
				}},
			},
		},
	}
	tests := []struct {
		name     string
		fields   *Client
		args     args
		wantSize int
		want     []uint64
	}{
		{
			name:   "test1",
			fields: c1,
			args: args{
				db:  "",
				rp:  "",
				mst: "",
			},
			wantSize: 4,
			want:     []uint64{1, 21, 33, 46},
		},
		{
			name:   "test2",
			fields: c2,
			args: args{
				db:  "db0",
				rp:  "rp0",
				mst: "mst0",
			},
			wantSize: 3,
			want:     []uint64{21, 33, 46},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				cacheData: tt.fields.cacheData,
			}
			got := c.ShowShards(tt.args.db, tt.args.rp, tt.args.mst)
			if len(got[0].Values) != tt.wantSize {
				t.Errorf("Client.ShowShards() = %v, want %v", got, tt.wantSize)
			}
			for i := 0; i < tt.wantSize; i++ {
				if got[0].Values[i][0] != tt.want[i] {
					t.Errorf("shard id = %v, want %v", got[0].Values[i][0], tt.want[i])
				}
			}
		})
	}
}

func TestSnapshotV2_GetData(t *testing.T) {
	address := "127.0.0.1:8491"
	nodeId := 1
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	var err error
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	mc := Client{
		logger:         logger.NewLogger(errno.ModuleUnknown),
		SendRPCMessage: &RPCMessageSender{},
		UseSnapshotV2:  true,
		metaServers:    []string{"127.0.0.1:8491", "127.0.0.1:8492"},
	}
	connectedServer = nodeId

	err = mc.retryUntilSnapshotV2(SQL, 0)
	require.NoError(t, err)
	if mc.cacheData.Index != server.metaData.Index {
		t.Fatalf("TestSnapshotV2_GetData1 error index; exp: %d; got: %d", server.metaData.Index, mc.cacheData.Index)
	}

	cmd1 := &proto2.CreateDatabaseCommand{
		Name:           proto.String("db0"),
		EnableTagArray: proto.Bool(false),
		ReplicaNum:     proto.Uint32(1),
	}
	typ := proto2.Command_CreateDatabaseCommand
	cmd2 := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd2, proto2.E_CreateDatabaseCommand_Command, cmd1); err != nil {
		panic(err)
	}
	strOps, _ := proto.Marshal(cmd2)
	server.ops = append(server.ops, string(strOps))
	connectedServer = nodeId

	err = mc.retryUntilSnapshotV2(SQL, 1)
	require.NoError(t, err)
	if mc.cacheData.Index != server.metaData.Index {
		t.Fatalf("TestSnapshotV2_GetData2 error index; exp: %d; got: %d", server.metaData.Index, mc.cacheData.Index)
	}

	server.NoClear = true
	server.ops = nil
	err = mc.retryUntilSnapshotV2(SQL, 10)
	require.NoError(t, err)
	if mc.cacheData.Index != 10 {
		t.Fatalf("TestSnapshotV2_GetData3 error index; exp: %d; got: %d", server.metaData.Index, mc.cacheData.Index)
	}
}

func TestCreateSqlNode(t *testing.T) {
	address := "127.0.0.1:8491"
	nodeId := 1
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	var err error
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	mc := Client{
		logger:         logger.NewLogger(errno.ModuleUnknown),
		SendRPCMessage: &RPCMessageSender{},
		UseSnapshotV2:  true,
		metaServers:    []string{"127.0.0.1:8491", "127.0.0.1:8492"},
	}
	connectedServer = nodeId
	server.NodeId = 123
	nodeID, _, _, err := mc.CreateSqlNode("127.0.0.1:8086", "127.0.0.1:8012")
	require.NoError(t, err)
	if nodeID != server.NodeId {
		t.Fatalf("TestCreateSqlNode err")
	}
}

var newPbFunc = map[proto2.Command_Type]func() (interface{}, *proto.ExtensionDesc){
	proto2.Command_CreateDatabaseCommand:            newCreateDatabasePb,
	proto2.Command_DropDatabaseCommand:              newDropDatabasePb,
	proto2.Command_CreateRetentionPolicyCommand:     newCreateRetentionPolicyPb,
	proto2.Command_DropRetentionPolicyCommand:       newDropRetentionPolicyPb,
	proto2.Command_SetDefaultRetentionPolicyCommand: newSetDefaultRetentionPolicyPb,
	proto2.Command_UpdateRetentionPolicyCommand:     newUpdateRetentionPolicyPb,
	proto2.Command_CreateShardGroupCommand:          newCreateShardGroupPb,
	proto2.Command_DeleteShardGroupCommand:          newDeleteShardGroupPb,
	proto2.Command_CreateSubscriptionCommand:        newCreateSubscriptionPb,
	proto2.Command_DropSubscriptionCommand:          newDropSubscriptionPb,
	proto2.Command_CreateUserCommand:                newCreateUserPb,
	proto2.Command_DropUserCommand:                  newDropUserPb,
	proto2.Command_UpdateUserCommand:                newUpdateUserPb,
	proto2.Command_SetPrivilegeCommand:              newSetPrivilegePb,
	proto2.Command_SetAdminPrivilegeCommand:         newSetAdminPrivilegePb,
	proto2.Command_SetDataCommand:                   newSetDataPb,
	proto2.Command_CreateMetaNodeCommand:            newCreateMetaNodePb,
	proto2.Command_DeleteMetaNodeCommand:            newDeleteMetaNodePb,
	proto2.Command_SetMetaNodeCommand:               newSetMetaNodePb,
	proto2.Command_CreateDataNodeCommand:            newCreateDataNodePb,
	proto2.Command_CreateSqlNodeCommand:             newCreateSqlNodePb,
	proto2.Command_DeleteDataNodeCommand:            newDeleteDataNodePb,
	proto2.Command_MarkDatabaseDeleteCommand:        newMarkDatabaseDeletePb,
	proto2.Command_MarkRetentionPolicyDeleteCommand: newMarkRetentionPolicyDeletePb,
	proto2.Command_CreateMeasurementCommand:         newCreateMeasurementPb,
	proto2.Command_ReShardingCommand:                newReShardingPb,
	proto2.Command_UpdateSchemaCommand:              newUpdateSchemaPb,
	proto2.Command_AlterShardKeyCmd:                 newAlterShardKeyPb,
	proto2.Command_PruneGroupsCommand:               newPruneGroupsPb,
	proto2.Command_MarkMeasurementDeleteCommand:     newMarkMeasurementDeletePb,
	proto2.Command_DropMeasurementCommand:           newDropMeasurementPb,
	proto2.Command_DeleteIndexGroupCommand:          newDeleteIndexGroupPb,
	proto2.Command_UpdateShardInfoTierCommand:       newUpdateShardInfoTierPb,
	proto2.Command_UpdateNodeStatusCommand:          newUpdateNodeStatusPb,
	proto2.Command_UpdateSqlNodeStatusCommand:       newUpdateSqlNodeStatusPb,
	proto2.Command_CreateEventCommand:               newCreateEventPb,
	proto2.Command_UpdateEventCommand:               newUpdateEventPb,
	proto2.Command_UpdatePtInfoCommand:              newUpdatePtInfoPb,
	proto2.Command_RemoveEventCommand:               newRemoveEventPb,
	proto2.Command_CreateDownSamplePolicyCommand:    newCreateDownSamplePb,
	proto2.Command_DropDownSamplePolicyCommand:      newDropDownSamplePb,
	proto2.Command_CreateDbPtViewCommand:            newCreateDbPtViewPb,
	proto2.Command_UpdateShardDownSampleInfoCommand: newUpdateShardDownSampleInfoPb,
	proto2.Command_MarkTakeoverCommand:              newMarkTakeoverPb,
	proto2.Command_MarkBalancerCommand:              newMarkBalancerPb,
	proto2.Command_CreateStreamCommand:              newCreateStreamPb,
	proto2.Command_DropStreamCommand:                newDropStreamPb,
	proto2.Command_VerifyDataNodeCommand:            newVerifyDataNodePb,
	proto2.Command_ExpandGroupsCommand:              newExpandGroupsPb,
	proto2.Command_UpdatePtVersionCommand:           newUpdatePtVersionPb,
	proto2.Command_RegisterQueryIDOffsetCommand:     newRegisterQueryIDOffsetPb,
	proto2.Command_CreateContinuousQueryCommand:     newCreateContinuousQueryPb,
	proto2.Command_ContinuousQueryReportCommand:     newContinuousQueryReportPb,
	proto2.Command_DropContinuousQueryCommand:       newDropContinuousQueryPb,
	proto2.Command_NotifyCQLeaseChangedCommand:      newNotifyCQLeaseChangedPb,
	proto2.Command_SetNodeSegregateStatusCommand:    newSetNodeSegregateStatusPb,
	proto2.Command_RemoveNodeCommand:                newRemoveNodePb,
	proto2.Command_UpdateReplicationCommand:         newUpdateReplicationPb,
	proto2.Command_UpdateMeasurementCommand:         newUpdateMeasurementPb,
}

func newCreateDatabasePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateDatabaseCommand{
		Name:           proto.String("db0"),
		EnableTagArray: proto.Bool(false),
		ReplicaNum:     proto.Uint32(1),
	}, proto2.E_CreateDatabaseCommand_Command
}

func newDropDatabasePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DropDatabaseCommand{
		Name: proto.String("db0"),
	}, proto2.E_DropDatabaseCommand_Command
}

func newCreateRetentionPolicyPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateRetentionPolicyCommand{
		Database: proto.String("db0"),
	}, proto2.E_CreateRetentionPolicyCommand_Command
}

func newDropRetentionPolicyPb() (interface{}, *proto.ExtensionDesc) {
	rpName := "rp0"
	return &proto2.DropRetentionPolicyCommand{
		Database: proto.String("db0"),
		Name:     &rpName,
	}, proto2.E_DropRetentionPolicyCommand_Command
}

func newSetDefaultRetentionPolicyPb() (interface{}, *proto.ExtensionDesc) {
	rpName := "rp0"
	return &proto2.SetDefaultRetentionPolicyCommand{
		Database: proto.String("db0"),
		Name:     &rpName,
	}, proto2.E_SetDefaultRetentionPolicyCommand_Command
}

func newUpdateRetentionPolicyPb() (interface{}, *proto.ExtensionDesc) {
	rpName := "rp0"
	newName := "rp1"
	return &proto2.UpdateRetentionPolicyCommand{
		Database: proto.String("db0"),
		Name:     &rpName,
		NewName:  &newName,
	}, proto2.E_UpdateRetentionPolicyCommand_Command
}

func newCreateShardGroupPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateShardGroupCommand{
		Database: proto.String("db0"),
	}, proto2.E_CreateShardGroupCommand_Command
}

func newDeleteShardGroupPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DeleteShardGroupCommand{
		Database: proto.String("db0"),
	}, proto2.E_DeleteShardGroupCommand_Command
}

func newCreateSubscriptionPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateSubscriptionCommand{
		Database: proto.String("db0"),
	}, proto2.E_CreateSubscriptionCommand_Command
}

func newDropSubscriptionPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DropSubscriptionCommand{
		Database: proto.String("db0"),
	}, proto2.E_DropSubscriptionCommand_Command
}

func newCreateUserPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateUserCommand{
		Name: proto.String("use0"),
	}, proto2.E_CreateUserCommand_Command
}

func newDropUserPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DropUserCommand{
		Name: proto.String("use0"),
	}, proto2.E_DropUserCommand_Command
}

func newUpdateUserPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdateUserCommand{
		Name: proto.String("use0"),
	}, proto2.E_UpdateUserCommand_Command
}

func newSetPrivilegePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.SetPrivilegeCommand{
		Username: proto.String("use0"),
	}, proto2.E_SetPrivilegeCommand_Command
}

func newSetAdminPrivilegePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.SetAdminPrivilegeCommand{
		Username: proto.String("use0"),
	}, proto2.E_SetAdminPrivilegeCommand_Command
}

func newSetDataPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.SetDataCommand{
		Data: &proto2.Data{
			Databases: []*proto2.DatabaseInfo{&proto2.DatabaseInfo{ReplicaN: proto.Int64(1), Name: proto.String("ds"), RetentionPolicies: []*proto2.RetentionPolicyInfo{&proto2.RetentionPolicyInfo{Name: proto.String("ds"), DownSamplePolicyInfo: &proto2.DownSamplePolicyInfo{}}}}},
		},
	}, proto2.E_SetDataCommand_Command
}

func newCreateMetaNodePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateMetaNodeCommand{}, proto2.E_CreateMetaNodeCommand_Command
}

func newSetMetaNodePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.SetMetaNodeCommand{
		HTTPAddr: proto.String("127.0.0.1:8091"),
		RPCAddr:  proto.String("127.0.0.1:8092"),
		TCPAddr:  proto.String("127.0.0.1:8088"),
	}, proto2.E_SetMetaNodeCommand_Command
}

func newDeleteMetaNodePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DeleteMetaNodeCommand{}, proto2.E_DeleteMetaNodeCommand_Command
}

func newCreateDataNodePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateDataNodeCommand{}, proto2.E_CreateDataNodeCommand_Command
}

func newCreateSqlNodePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateSqlNodeCommand{}, proto2.E_CreateSqlNodeCommand_Command
}

func newUpdateSqlNodeStatusPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdateSqlNodeStatusCommand{}, proto2.E_UpdateSqlNodeStatusCommand_Command
}

func newDeleteDataNodePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DeleteDataNodeCommand{}, proto2.E_DeleteDataNodeCommand_Command
}

func newMarkDatabaseDeletePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.MarkDatabaseDeleteCommand{}, proto2.E_MarkDatabaseDeleteCommand_Command
}

func newMarkRetentionPolicyDeletePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.MarkRetentionPolicyDeleteCommand{}, proto2.E_MarkRetentionPolicyDeleteCommand_Command
}

func newCreateMeasurementPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateMeasurementCommand{}, proto2.E_CreateMeasurementCommand_Command
}

func newReShardingPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.ReShardingCommand{}, proto2.E_ReShardingCommand_Command
}

func newUpdateSchemaPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdateSchemaCommand{}, proto2.E_UpdateSchemaCommand_Command
}

func newAlterShardKeyPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.AlterShardKeyCmd{}, proto2.E_AlterShardKeyCmd_Command
}

func newPruneGroupsPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.PruneGroupsCommand{}, proto2.E_PruneGroupsCommand_Command
}

func newMarkMeasurementDeletePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.MarkMeasurementDeleteCommand{}, proto2.E_MarkMeasurementDeleteCommand_Command
}

func newDropMeasurementPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DropMeasurementCommand{}, proto2.E_DropMeasurementCommand_Command
}

func newDeleteIndexGroupPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DeleteIndexGroupCommand{}, proto2.E_DeleteIndexGroupCommand_Command
}

func newUpdateShardInfoTierPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdateShardInfoTierCommand{}, proto2.E_UpdateShardInfoTierCommand_Command
}

func newUpdateNodeStatusPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdateNodeStatusCommand{}, proto2.E_UpdateNodeStatusCommand_Command
}

func newCreateEventPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateEventCommand{
		EventInfo: &proto2.MigrateEventInfo{
			Pti: &proto2.DbPt{},
		},
	}, proto2.E_CreateEventCommand_Command
}

func newUpdateEventPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdateEventCommand{}, proto2.E_UpdateEventCommand_Command
}

func newUpdatePtInfoPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdatePtInfoCommand{}, proto2.E_UpdatePtInfoCommand_Command
}

func newRemoveEventPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.RemoveEventCommand{}, proto2.E_RemoveEventCommand_Command
}

func newCreateDownSamplePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateDownSamplePolicyCommand{
		Database: proto.String("ds"),
		Name:     proto.String("ds"),
		DownSamplePolicyInfo: &proto2.DownSamplePolicyInfo{
			TaskID:             proto.Uint64(1),
			DownSamplePolicies: []*proto2.DownSamplePolicy{&proto2.DownSamplePolicy{}},
			Calls:              []*proto2.DownSampleOperators{&proto2.DownSampleOperators{}},
		},
	}, proto2.E_CreateDownSamplePolicyCommand_Command
}

func newDropDownSamplePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DropDownSamplePolicyCommand{
		Database: proto.String("ds"),
		RpName:   proto.String("ds"),
	}, proto2.E_DropDownSamplePolicyCommand_Command
}

func newCreateDbPtViewPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateDbPtViewCommand{}, proto2.E_CreateDbPtViewCommand_Command
}

func newUpdateShardDownSampleInfoPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdateShardDownSampleInfoCommand{}, proto2.E_UpdateShardDownSampleInfoCommand_Command
}

func newMarkTakeoverPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.MarkTakeoverCommand{}, proto2.E_MarkTakeoverCommand_Command
}

func newMarkBalancerPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.MarkBalancerCommand{}, proto2.E_MarkBalancerCommand_Command
}

func newCreateStreamPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateStreamCommand{
		StreamInfo: &proto2.StreamInfo{},
	}, proto2.E_CreateStreamCommand_Command
}

func newDropStreamPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DropStreamCommand{}, proto2.E_DropStreamCommand_Command
}

func newVerifyDataNodePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.VerifyDataNodeCommand{}, proto2.E_VerifyDataNodeCommand_Command
}

func newExpandGroupsPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.ExpandGroupsCommand{}, proto2.E_ExpandGroupsCommand_Command
}

func newUpdatePtVersionPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdatePtVersionCommand{}, proto2.E_UpdatePtVersionCommand_Command
}

func newRegisterQueryIDOffsetPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.RegisterQueryIDOffsetCommand{}, proto2.E_RegisterQueryIDOffsetCommand_Command
}

func newCreateContinuousQueryPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.CreateContinuousQueryCommand{}, proto2.E_CreateContinuousQueryCommand_Command
}

func newContinuousQueryReportPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.ContinuousQueryReportCommand{}, proto2.E_ContinuousQueryReportCommand_Command
}

func newDropContinuousQueryPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.DropContinuousQueryCommand{}, proto2.E_DropContinuousQueryCommand_Command
}

func newNotifyCQLeaseChangedPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.NotifyCQLeaseChangedCommand{}, proto2.E_NotifyCQLeaseChangedCommand_Command
}

func newSetNodeSegregateStatusPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.SetNodeSegregateStatusCommand{}, proto2.E_SetNodeSegregateStatusCommand_Command
}

func newRemoveNodePb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.RemoveNodeCommand{}, proto2.E_RemoveNodeCommand_Command
}

func newUpdateReplicationPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdateReplicationCommand{}, proto2.E_UpdateReplicationCommand_Command
}

func newUpdateMeasurementPb() (interface{}, *proto.ExtensionDesc) {
	return &proto2.UpdateMeasurementCommand{}, proto2.E_UpdateMeasurementCommand_Command
}

func BuildCmd(t proto2.Command_Type) *proto2.Command {
	cmd1, ext := newPbFunc[t]()
	cmd2 := &proto2.Command{Type: &t}
	if err := proto.SetExtension(cmd2, ext, cmd1); err != nil {
		panic(err)
	}
	return cmd2
}

func TestMetaClientApply(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
		},
		logger: logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient"))}
	c.cacheData.Databases["ds"] = &meta2.DatabaseInfo{RetentionPolicies: make(map[string]*meta2.RetentionPolicyInfo), ReplicaN: 1, Name: "ds"}
	c.cacheData.Databases["ds"].RetentionPolicies["ds"] = &meta2.RetentionPolicyInfo{DownSamplePolicyInfo: &meta2.DownSamplePolicyInfo{}}
	for k, v := range applyFunc {
		cmd := BuildCmd(k)
		v(c, cmd)
	}
}

func TestMetaClientApplyPanic(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
		},
		logger: logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient"))}

	cmd1 := &proto2.CreateEventCommand{}
	typ := proto2.Command_CreateEventCommand
	cmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, proto2.E_CreateEventCommand_Command, cmd1); err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(len(applyFunc))
	for _, v := range applyFunc {
		go func(fn func(c *Client, op *proto2.Command) error) {
			defer func() {
				recover()
				wg.Done()
			}()
			fn(c, cmd)
		}(v)
	}
	wg.Wait()
}

func TestMetaClientComFunc(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
		},
	}
	c.EnableUseSnapshotV2(true, true)
	c.SetNode(1, 2)
	require.Equal(t, int(c.nodeID), 1)
	require.Equal(t, int(c.Clock), 2)
}

func TestCreateStreamMeasurement(t *testing.T) {
	ts := time.Now()
	sgInfo1 := meta2.ShardGroupInfo{ID: 1, StartTime: ts, EndTime: time.Now().Add(time.Duration(3600)), DeletedAt: time.Time{},
		Shards: []meta2.ShardInfo{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}, EngineType: config.COLUMNSTORE}
	c1 := &Client{
		logger:         logger.NewLogger(errno.ModuleMetaClient),
		SendRPCMessage: &RPCMessageSender{},
		metaServers:    []string{"127.0.0.1"},
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:         "rp0",
						Duration:     72 * time.Hour,
						Measurements: map[string]*meta2.MeasurementInfo{"mst0": {Name: "mst0"}},
						ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
						MstVersions: map[string]meta2.MeasurementVer{
							"mst0": {
								NameWithVersion: "mst0",
								Version:         1,
							},
						},
					},
				}},
				"srcRepo": {
					Name:     "srcRepo",
					ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
					RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
						"srcStore": {
							Name:         "srcStore",
							Duration:     72 * time.Hour,
							Measurements: map[string]*meta2.MeasurementInfo{"srcStore": {Name: "srcStore"}},
							ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
							MstVersions: map[string]meta2.MeasurementVer{
								"srcStore": {
									NameWithVersion: "srcStore",
									Version:         1,
								},
							},
						},
						"srcStore1": {
							Name:         "srcStore1",
							Duration:     72 * time.Hour,
							Measurements: map[string]*meta2.MeasurementInfo{"srcStore1": {Name: "srcStore1", EngineType: config.COLUMNSTORE}},
							ShardGroups:  []meta2.ShardGroupInfo{sgInfo1},
							MstVersions: map[string]meta2.MeasurementVer{
								"srcStore1": {
									NameWithVersion: "srcStore1",
									Version:         1,
								},
							},
						},
					},
				},
			},
		},
	}

	newStream := "newStore"
	repository := "srcRepo"
	logStream := "srcStore"

	sql := "SELECT count(count_cnt) GROUP BY time(5s)"
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	q, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal(err)
	}
	stmt := q.Statements[0]
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		t.Fatal(fmt.Errorf("invalid SelectStatement"))
	}

	dest := &influxql.Measurement{Database: repository, Name: newStream, RetentionPolicy: "srcStore"}
	src := &influxql.Measurement{Database: repository, Name: logStream, RetentionPolicy: logStream}
	info := meta2.NewStreamInfo(newStream, 0, src,
		&meta2.StreamMeasurementInfo{Name: newStream, Database: repository, RetentionPolicy: "srcStore"},
		selectStmt)

	dest1 := &influxql.Measurement{Database: "db0", Name: "mst0", RetentionPolicy: "rp0"}
	err = c1.CreateStreamMeasurement(info, src, dest1, selectStmt)
	if err != nil {
		t.Fatal(err)
	}

	dest2 := &influxql.Measurement{Database: "db0", Name: "mst0", RetentionPolicy: "rp1"}
	err = c1.CreateStreamMeasurement(info, src, dest2, selectStmt)
	if err == nil || !strings.Contains(err.Error(), "retention policy not found") {
		t.Fatal("unexpect err", err)
	}

	src1 := &influxql.Measurement{Database: "srcRepo", Name: "srcStore1", RetentionPolicy: "srcStore"}
	err = c1.CreateStreamMeasurement(info, src1, dest, selectStmt)
	if err == nil || !strings.Contains(err.Error(), "measurement not found") {
		t.Fatal("unexpect err", err)
	}

	err = c1.CreateStreamMeasurement(info, src, dest, selectStmt)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatal("unexpect err", err)
	}

	logStream1 := "srcStore1"
	destColumn := &influxql.Measurement{Database: repository, Name: newStream, RetentionPolicy: logStream1}
	srcColumn := &influxql.Measurement{Database: repository, Name: logStream1, RetentionPolicy: logStream1}
	infoColumn := meta2.NewStreamInfo(newStream, 0, srcColumn,
		&meta2.StreamMeasurementInfo{Name: newStream, Database: repository, RetentionPolicy: logStream1},
		selectStmt)
	err = c1.CreateStreamMeasurement(infoColumn, srcColumn, destColumn, selectStmt)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatal("unexpect err", err)
	}
}

func TestMetaClientLocalExec(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
			Index:     2,
		},
		logger: logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
	}

	cmd := &proto2.CreateMeasurementCommand{}
	err := c.LocalExec(1, proto2.Command_CreateMeasurementCommand, proto2.E_CreateMeasurementCommand_Command, cmd)
	require.Equal(t, err, nil)
	cmd1 := &proto2.CreateEventCommand{}
	err1 := c.LocalExec(3, proto2.Command_CreateEventCommand, proto2.E_CreateEventCommand_Command, cmd1)
	require.Equal(t, err1, nil)
}

func TestCreateDatabase(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
			DataNodes: []meta2.DataNode{meta2.DataNode{NodeInfo: meta2.NodeInfo{Status: serf.StatusAlive}}},
		},
		RetentionAutoCreate: true,
	}
	cmd1 := &proto2.CreateDatabaseCommand{
		Name:           proto.String("db0"),
		EnableTagArray: proto.Bool(false),
		ReplicaNum:     proto.Uint32(0),
	}
	typ := proto2.Command_CreateDatabaseCommand
	cmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, proto2.E_CreateDatabaseCommand_Command, cmd1); err != nil {
		panic(err)
	}
	err := applyFunc[proto2.Command_CreateDatabaseCommand](c, cmd)
	if !errno.Equal(err, errno.ApplyFuncErr) {
		t.Fatal("TestCreateDatabase1 err")
	}

	cmd2 := &proto2.CreateDatabaseCommand{
		Name:            proto.String("db0"),
		EnableTagArray:  proto.Bool(false),
		ReplicaNum:      proto.Uint32(1),
		RetentionPolicy: &proto2.RetentionPolicyInfo{},
	}
	cmd = &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, proto2.E_CreateDatabaseCommand_Command, cmd2); err != nil {
		panic(err)
	}
	err = applyFunc[proto2.Command_CreateDatabaseCommand](c, cmd)
	if !errno.Equal(err, errno.ApplyFuncErr) {
		t.Fatal("TestCreateDatabase2 err")
	}
}

func TestDropDatabase(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
			DataNodes: []meta2.DataNode{meta2.DataNode{NodeInfo: meta2.NodeInfo{Status: serf.StatusAlive}}},
		},
	}
	c.cacheData.Databases["db0"] = &meta2.DatabaseInfo{ContinuousQueries: make(map[string]*meta2.ContinuousQueryInfo)}
	c.cacheData.Databases["db0"].ContinuousQueries["1"] = &meta2.ContinuousQueryInfo{}
	cmd1 := &proto2.DropDatabaseCommand{
		Name: proto.String("db0"),
	}
	typ := proto2.Command_DropDatabaseCommand
	cmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, proto2.E_DropDatabaseCommand_Command, cmd1); err != nil {
		panic(err)
	}
	err := applyFunc[proto2.Command_DropDatabaseCommand](c, cmd)
	require.Equal(t, err, nil)
}

func Test_CreateMeasurement(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
			DataNodes: []meta2.DataNode{meta2.DataNode{NodeInfo: meta2.NodeInfo{Status: serf.StatusAlive}}},
		},
	}
	c.cacheData.Databases["db0"] = &meta2.DatabaseInfo{ContinuousQueries: make(map[string]*meta2.ContinuousQueryInfo)}
	c.cacheData.Databases["db0"].ContinuousQueries["1"] = &meta2.ContinuousQueryInfo{}
	cmd1 := &proto2.CreateMeasurementCommand{
		Name: proto.String("mst"),
	}
	typ := proto2.Command_CreateMeasurementCommand
	cmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, proto2.E_CreateMeasurementCommand_Command, cmd1); err != nil {
		panic(err)
	}
	err := applyFunc[proto2.Command_CreateMeasurementCommand](c, cmd)
	if !errno.Equal(err, errno.ApplyFuncErr) {
		t.Fatal("TestCreateDatabase2 err")
	}
}

func TestUpdateSchema(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
			DataNodes: []meta2.DataNode{meta2.DataNode{NodeInfo: meta2.NodeInfo{Status: serf.StatusAlive}}},
		},
	}
	c.cacheData.Databases["db0"] = &meta2.DatabaseInfo{RetentionPolicies: make(map[string]*meta2.RetentionPolicyInfo)}
	c.cacheData.Databases["db0"].RetentionPolicies["rp0"] = &meta2.RetentionPolicyInfo{Measurements: make(map[string]*meta2.MeasurementInfo), MstVersions: make(map[string]meta2.MeasurementVer)}
	c.cacheData.Databases["db0"].RetentionPolicies["rp0"].Measurements["mst0"] = &meta2.MeasurementInfo{Schema: make(map[string]int32)}
	c.cacheData.Databases["db0"].RetentionPolicies["rp0"].MstVersions["mst0"] = meta2.MeasurementVer{NameWithVersion: "mst0"}
	c.cacheData.Databases["db0"].RetentionPolicies["rp0"].Measurements["mst0"].Schema["col1"] = 1
	cmd1 := &proto2.UpdateSchemaCommand{
		Database:      proto.String("db0"),
		RpName:        proto.String("rp0"),
		Measurement:   proto.String("mst0"),
		FieldToCreate: []*proto2.FieldSchema{&proto2.FieldSchema{FieldName: proto.String("col2"), FieldType: proto.Int32(2)}},
	}
	typ := proto2.Command_UpdateSchemaCommand
	cmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, proto2.E_UpdateSchemaCommand_Command, cmd1); err != nil {
		panic(err)
	}
	err := applyFunc[proto2.Command_UpdateSchemaCommand](c, cmd)
	require.Equal(t, err, nil)
	cmd2 := &proto2.UpdateSchemaCommand{
		Database:      proto.String("db0"),
		RpName:        proto.String("rp0"),
		Measurement:   proto.String("mst0"),
		FieldToCreate: []*proto2.FieldSchema{&proto2.FieldSchema{FieldName: proto.String("col3"), FieldType: proto.Int32(2)}},
	}
	cmd = &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, proto2.E_UpdateSchemaCommand_Command, cmd2); err != nil {
		panic(err)
	}
	err = applyFunc[proto2.Command_UpdateSchemaCommand](c, cmd)
	require.Equal(t, err, nil)
}

func TestCreateDBPtView(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
			DataNodes: []meta2.DataNode{meta2.DataNode{NodeInfo: meta2.NodeInfo{Status: serf.StatusAlive}}},
		},
	}
	cmd1 := &proto2.CreateDbPtViewCommand{
		DbName:     proto.String("db0"),
		ReplicaNum: proto.Uint32(1),
	}
	typ := proto2.Command_CreateDbPtViewCommand
	cmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, proto2.E_CreateDbPtViewCommand_Command, cmd1); err != nil {
		panic(err)
	}
	err := applyFunc[proto2.Command_CreateDbPtViewCommand](c, cmd)
	require.Equal(t, err, nil)
}

func TestCreateDataNode(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
			DataNodes: []meta2.DataNode{meta2.DataNode{NodeInfo: meta2.NodeInfo{Status: serf.StatusAlive, Host: "127.0.0.1:8086"}}},
		},
	}
	cmd1 := &proto2.CreateDataNodeCommand{
		HTTPAddr: proto.String("127.0.0.1:8086"),
		Role:     proto.String("store"),
	}
	typ := proto2.Command_CreateDataNodeCommand
	cmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, proto2.E_CreateDataNodeCommand_Command, cmd1); err != nil {
		panic(err)
	}
	err := applyFunc[proto2.Command_CreateDataNodeCommand](c, cmd)
	require.Equal(t, err, nil)
}

func Test_CreateSqlNode(t *testing.T) {
	meta2.DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	var c *Client = &Client{
		cacheData: &meta2.Data{
			Databases: make(map[string]*meta2.DatabaseInfo),
			SqlNodes:  []meta2.DataNode{meta2.DataNode{NodeInfo: meta2.NodeInfo{Status: serf.StatusAlive, TCPHost: "127.0.0.1:8086"}}},
		},
	}
	cmd1 := &proto2.CreateSqlNodeCommand{
		HTTPAddr: proto.String("127.0.0.1:8086"),
	}
	typ := proto2.Command_CreateSqlNodeCommand
	cmd := &proto2.Command{Type: &typ}
	if err := proto.SetExtension(cmd, proto2.E_CreateSqlNodeCommand_Command, cmd1); err != nil {
		panic(err)
	}
	err := applyFunc[proto2.Command_CreateSqlNodeCommand](c, cmd)
	require.Equal(t, err, nil)
}

func TestOpenClientAtStore(t *testing.T) {
	var c *Client = &Client{
		cacheData:     &meta2.Data{},
		UseSnapshotV2: true,
		closing:       make(chan struct{}),
	}
	c.Close()
	c.OpenAtStore()
	time.Sleep(time.Second * 3)
}

func TestOpenClientAtSql(t *testing.T) {
	var c *Client = &Client{
		cacheData:     &meta2.Data{},
		UseSnapshotV2: true,
		closing:       make(chan struct{}),
	}
	c.Close()
	c.Open()
	time.Sleep(time.Second * 3)
}

func TestClient_InsertFiles(t *testing.T) {
	client := &Client{
		cacheData: &meta2.Data{
			Databases: map[string]*meta2.DatabaseInfo{"db0": {
				Name:     "db0",
				ShardKey: meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}},
				RetentionPolicies: map[string]*meta2.RetentionPolicyInfo{
					"rp0": {
						Name:     "rp0",
						Duration: 72 * time.Hour,
					},
				}}},
		},
		metaServers:    []string{"127.0.0.1"},
		logger:         logger.NewLogger(errno.ModuleMetaClient).With(zap.String("service", "metaclient")),
		SendRPCMessage: &RPCMessageSender{},
	}
	type args struct {
		fileInfos []meta2.FileInfo
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				fileInfos: []meta2.FileInfo{
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
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.InsertFiles(tt.args.fileInfos)
			require.EqualError(t, err, "execute command timeout")
		})
	}
}
