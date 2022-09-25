/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/client.go

2022.01.23 changed.
Add new test case etc.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/
package metaclient

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/app/ts-meta/meta/message"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type RPCServer struct {
	metaData *meta2.Data
}

func (c *RPCServer) Abort() {

}

func (c *RPCServer) Handle(w spdy.Responser, data interface{}) error {
	metaMsg := data.(*message.MetaMessage)

	switch msg := metaMsg.Data().(type) {
	case *message.SnapshotRequest:
		return c.HandleSnapshot(w, msg)
	}
	return nil
}

func (c *RPCServer) HandleSnapshot(w spdy.Responser, msg *message.SnapshotRequest) error {
	fmt.Printf("server Handle: %+v \n", msg)

	c.metaData = &meta2.Data{
		Index: msg.Index + 1,
	}

	buf, err := c.metaData.MarshalBinary()
	rsp := &message.SnapshotResponse{
		Data: buf,
		Err:  fmt.Sprintf("%v", err),
	}

	return w.Response(message.NewMetaMessage(message.SnapshotResponseMessage, rsp), true)
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
	address := "127.0.0.10:8491"
	nodeId := 1
	transport.NewMetaNodeManager().Add(uint64(nodeId), address)

	// Server
	rrcServer, err := startServer(address)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer rrcServer.Stop()
	time.Sleep(time.Second)

	mc := Client{logger: logger.NewLogger(errno.ModuleUnknown)}
	data, err := mc.getSnapshot(nodeId, 0)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if data.Index != server.metaData.Index {
		t.Fatalf("error index; exp: %d; got: %d", server.metaData.Index, data.Index)
	}
}

func TestClient_compareHashAndPwdVerTwo(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	hashed, err := c.genPbkdf2PwdVal("AsdF_789")
	if err != nil {
		t.Error("gen pbkdf2 password failed", zap.Error(err))
	}
	err = c.compareHashAndPwdVerTwo(hashed, "AsdF_789")
	if err != nil {
		t.Error("compare hash and plaintext failed", zap.Error(err))
	}
}

func TestClient_saltedEncryptByVer(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
	_, _, err := c.saltedEncryptByVer("AsdF_789", hashAlgoVerOne)
	if err != nil {
		t.Error("encrypt by version one failed", zap.Error(err))
	}
	_, _, err = c.saltedEncryptByVer("AsdF_789", hashAlgoVerTwo)
	if err != nil {
		t.Error("encrypt by version two failed", zap.Error(err))
	}
}

func TestClient_isValidPwd(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta")
	c := NewClient(metaPath, false, 20)
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
		ptIds:  []uint32{1, 2, 3},
		cacheData: &meta2.Data{
			PtNumPerNode: 1,
			ClusterID:    1,
			DataNodes: []meta2.DataNode{
				meta2.DataNode{
					meta2.NodeInfo{
						ID:      1,
						Host:    "127.0.0.1:8090",
						TCPHost: "127.0.0.1:8091",
					},
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
	require.Equal(t, []uint32{1, 2, 3}, client.PtIds())

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

	// AddPt
	client.AddPt(3)
	client.AddPt(4)
	require.Equal(t, []uint32{1, 2, 3, 4}, client.PtIds())

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

func TestClient_CreateDatabaseWithRetentionPolicy(t *testing.T) {
	c := &Client{
		cacheData:   &meta2.Data{},
		metaServers: []string{"127.0.0.1"},
		logger:      logger.NewLogger(errno.ModuleMetaClient),
	}
	spec := &meta2.RetentionPolicySpec{Name: "testRp"}
	ski := &meta2.ShardKeyInfo{ShardKey: []string{"tag1", "tag2"}}
	_, err := c.CreateDatabaseWithRetentionPolicy("db0", spec, ski)
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
	_, err := c.CreateDatabaseWithRetentionPolicy("test", spec, ski)
	require.EqualError(t, err, "shard key conflict")
}
